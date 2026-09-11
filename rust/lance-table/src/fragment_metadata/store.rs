// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Immutable protobuf routing nodes and columnar Lance leaves.
//!
//! Each fragment has a header containing its non-file metadata, followed by
//! ordered data-file rows. File fields are separate columns for compression.
//! Object paths are relative to the dataset root; every rewrite gets a new UUID.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use arrow_array::builder::{BinaryBuilder, Int32Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt8Type;
use arrow_array::types::{Int32Type, UInt32Type, UInt64Type};
use arrow_array::{Array, RecordBatch, StringArray, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::{FutureExt, TryStreamExt, future::BoxFuture};
use prost::Message;

use crate::format::pb;
use crate::format::pb::fragment_action::Action;
use crate::format::{DataFile, DataFileFieldInterner, Fragment, RowDatasetVersionMeta, RowIdMeta};
use crate::fragment_metadata::node::{self, InternalNode};
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::{Error, Result};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_file::version::ConcreteFileVersion;
use lance_file::writer::FileWriterOptions;
use lance_io::ReadBatchParams;
use lance_io::object_reader::SmallReader;
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::ScanScheduler;
use lance_io::traits::Reader;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore as OSObjectStore, PutOptions, PutPayload};
use uuid::Uuid;
mod validation_reads;

// Decode each leaf in batches of READ_BATCH_ROWS, with up to
// READ_BATCH_READAHEAD decoder batches in flight for that leaf. A scan may
// keep several leaves in flight separately.
const READ_BATCH_ROWS: u32 = 16 * 1024;
const READ_BATCH_READAHEAD: u32 = 16;

/// A written node: its parent child reference plus the actual bytes written to
/// storage (for write-amplification accounting).
#[derive(Debug)]
pub struct Written {
    pub child_ref: pb::FragmentMetadataChild,
    pub io_bytes: u64,
}

struct EncodedLeaf {
    range: Range<usize>,
    bytes: bytes::Bytes,
}

/// Reads and writes fragment metadata tree node files against an object store.
#[derive(Clone)]
pub struct NodeStore {
    pub(super) object_store: Arc<ObjectStore>,
    base: Path,
    scheduler: Arc<ScanScheduler>,
    cache: Arc<LanceCache>,
    validation_reads: Option<Arc<validation_reads::ValidationReads>>,
    pub(super) next_action_sequence: u64,
    pub(super) hard_capacity_bytes: u64,
    foreign_bases: HashMap<u32, ForeignBase>,
    interner: Arc<Mutex<DataFileFieldInterner>>,
}

#[derive(Clone)]
struct ForeignBase {
    store: Arc<ObjectStore>,
    base: Path,
}

impl NodeStore {
    pub fn new(
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
    ) -> Self {
        Self {
            object_store,
            base,
            scheduler,
            cache,
            validation_reads: None,
            next_action_sequence: u64::MAX,
            hard_capacity_bytes: u64::MAX,
            foreign_bases: HashMap::new(),
            interner: Arc::new(Mutex::new(DataFileFieldInterner::default())),
        }
    }

    pub(super) fn set_foreign_bases(
        &mut self,
        foreign_bases: HashMap<u32, (Arc<ObjectStore>, Path)>,
    ) {
        self.foreign_bases = foreign_bases
            .into_iter()
            .map(|(id, (store, base))| (id, ForeignBase { store, base }))
            .collect();
    }

    pub(super) fn rebind(&mut self, object_store: Arc<ObjectStore>) {
        self.scheduler = ScanScheduler::new(
            object_store.clone(),
            lance_io::scheduler::SchedulerConfig::max_bandwidth(&object_store),
        );
        self.object_store = object_store;
        self.validation_reads = None;
    }

    pub(super) fn retain_validation_reads(&mut self) -> Result<()> {
        self.validation_reads = Some(Arc::new(validation_reads::ValidationReads::new()?));
        Ok(())
    }

    pub(super) fn clear_validation_reads(&mut self) {
        self.validation_reads = None;
    }

    /// Write a leaf (sorted fragments) as a columnar Lance file.
    ///
    /// Each data file occupies one row. A fragment with no data files occupies
    /// one marker row, and full non-file metadata is stored once per fragment.
    /// Returns a leaf child reference plus actual bytes written.
    pub async fn write_leaf(
        &self,
        fragments: &[Fragment],
        materialized_through_action_sequence: u64,
    ) -> Result<Written> {
        let bytes = self.encode_leaf(fragments).await?;
        self.write_encoded_leaf(fragments, bytes, materialized_through_action_sequence)
            .await
    }

    /// Encode before publishing, splitting only when actual Lance bytes exceed
    /// the target. A single fragment may exceed the target, but never the hard
    /// object limit. Rejected encodings create no remote intermediate objects.
    pub(super) async fn write_leaves(
        &self,
        fragments: &[Fragment],
        watermark: u64,
        config: &node::FragmentMetadataTreeConfig,
    ) -> Result<Vec<Written>> {
        let encoded = self.encoded_leaves(fragments, 0..fragments.len(), config);
        futures::pin_mut!(encoded);
        let mut output = Vec::new();
        while let Some(leaf) = encoded.try_next().await? {
            output.push(
                self.write_encoded_leaf(&fragments[leaf.range], leaf.bytes, watermark)
                    .await?,
            );
        }
        Ok(output)
    }

    /// Pack adjacent bootstrap batches when their combined encoding fits. Keep
    /// at most three targets of logical metadata in a candidate, except for an
    /// indivisible fragment. Normal mutation splits retain their own headroom.
    pub(super) async fn write_initial_leaves(
        &self,
        fragments: &[Fragment],
        config: &node::FragmentMetadataTreeConfig,
    ) -> Result<Vec<Written>> {
        let mut pending: Option<EncodedLeaf> = None;
        let mut output = Vec::new();
        let mut start = 0;
        while start < fragments.len() {
            let mut end = start;
            let mut logical_bytes = 0;
            while end < fragments.len() && logical_bytes < config.max_leaf_bytes {
                logical_bytes += node::fragment_logical_bytes(&fragments[end]);
                end += 1;
            }
            let encoded = self.encoded_leaves(fragments, start..end, config);
            futures::pin_mut!(encoded);
            while let Some(leaf) = encoded.try_next().await? {
                if let Some(previous) = pending.take() {
                    let encoded_bytes = previous.bytes.len().checked_add(leaf.bytes.len());
                    let range = previous.range.start..leaf.range.end;
                    if encoded_bytes.is_some_and(|bytes| bytes as u64 <= config.max_leaf_bytes)
                        && node::leaf_logical_bytes(&fragments[range.clone()])
                            <= config.max_leaf_bytes * 3
                    {
                        let bytes = self.encode_leaf(&fragments[range.clone()]).await?;
                        // Compression is not additive. The estimate only chooses
                        // a candidate; its actual encoding decides admission.
                        if bytes.len() as u64 <= config.max_leaf_bytes {
                            pending = Some(EncodedLeaf { range, bytes });
                            continue;
                        }
                    }
                    output.push(
                        self.write_encoded_leaf(&fragments[previous.range], previous.bytes, 0)
                            .await?,
                    );
                }
                pending = Some(leaf);
            }
            start = end;
        }
        if let Some(leaf) = pending {
            output.push(
                self.write_encoded_leaf(&fragments[leaf.range], leaf.bytes, 0)
                    .await?,
            );
        }
        Ok(output)
    }

    /// Read a columnar leaf back into a fragment list: each FRAGMENT header
    /// row starts a fragment, its DATA_FILE rows follow.
    pub fn read_leaf<'a>(
        &'a self,
        child: &'a pb::FragmentMetadataChild,
    ) -> BoxFuture<'a, Result<Vec<Fragment>>> {
        async move {
            match &self.validation_reads {
                Some(reads) => reads.read(self, child).await,
                None => self.read_leaf_uncached(child).await,
            }
        }
        .boxed()
    }

    /// Write an internal node (children + buffer) as a protobuf object.
    pub async fn write_internal(
        &self,
        children: Vec<pb::FragmentMetadataChild>,
        buffer: Vec<pb::FragmentMetadataMutation>,
    ) -> Result<Written> {
        let path = self.node_path();
        let node = pb::FragmentMetadataNode {
            children: children.clone(),
            buffer,
        };
        let bytes = node.encode_to_vec();
        let io_bytes = bytes.len() as u64;
        self.object_store
            .inner
            .put_opts(
                &self.resolve_path(path.as_ref())?,
                PutPayload::from(bytes),
                PutOptions::default(),
            )
            .await?;
        Ok(Written {
            child_ref: node::internal_ref(path.to_string(), &children, &node.buffer, io_bytes)?,
            io_bytes,
        })
    }

    /// Read an internal node.
    pub async fn read_internal(&self, child: &pb::FragmentMetadataChild) -> Result<InternalNode> {
        let (store, path) = self.resolve_child(child)?;
        let bytes = store
            .inner
            .get_opts(&path, GetOptions::default())
            .await?
            .bytes()
            .await?;
        let object_size = bytes.len() as u64;
        if object_size != child.object_size {
            return Err(super::validation::corrupt(format!(
                "Internal node {} has {object_size} bytes but its parent declares {}",
                child.path, child.object_size,
            )));
        }
        let mut node = pb::FragmentMetadataNode::decode(bytes)?;
        super::validation::children(&node.children, self.next_action_sequence, child.min_key)?;
        super::validation::buffer(&node.buffer, 1, self.next_action_sequence)?;
        let summary = node::internal_ref(
            child.path.clone(),
            &node.children,
            &node.buffer,
            object_size,
        )?;
        if !derived_child_fields_match(&summary, child) {
            return Err(super::validation::corrupt(format!(
                "Internal node {} contents differ from its parent reference",
                child.path
            )));
        }
        // Inherited nodes belong to another dataset. Unset child and file
        // refs take that dataset's base_id. External lineage has no base_id, so
        // a source-relative path would bind to the wrong dataset.
        if let Some(base_id) = child.base_id {
            for descendant in &mut node.children {
                if descendant.base_id.is_none() {
                    descendant.base_id = Some(base_id);
                }
            }
            let (store, base) = self.child_dataset(child)?;
            preserve_inherited_refs(
                &mut node.buffer,
                SourceStore {
                    object_store: store.as_ref(),
                    base: &base,
                    base_id,
                },
            )
            .await?;
        }
        Ok(InternalNode {
            children: node.children,
            buffer: node.buffer,
        })
    }

    /// Write an immutable root base without publishing a dataset version.
    /// The caller must publish its reference through the Version Manifest.
    pub async fn write_root_base(&self, root: &pb::FragmentMetadataRoot) -> Result<(String, u64)> {
        let path = Path::from("_bt/base").join(format!("{}.root", Uuid::new_v4()));
        let bytes = root.encode_to_vec();
        let size = bytes.len() as u64;
        self.object_store
            .put(&self.resolve_path(path.as_ref())?, &bytes)
            .await?;
        Ok((path.to_string(), size))
    }

    /// Read a base named directly by a Version Manifest, without version lookup.
    pub async fn read_root_base(&self, path: &str) -> Result<pb::FragmentMetadataRoot> {
        let bytes = get_whole(&self.object_store, &self.resolve_path(path)?).await?;
        Ok(pb::FragmentMetadataRoot::decode(bytes.as_ref())?)
    }

    pub(super) fn apply_verified(
        &self,
        fragments: &mut BTreeMap<u64, Fragment>,
        actions: Vec<pb::FragmentMetadataMutation>,
    ) -> Result<()> {
        if actions.is_empty() {
            return Ok(());
        }
        let touched: BTreeSet<u64> = actions.iter().map(node::action_key).collect();
        node::apply_verified(fragments, actions)?;
        let mut intern = self.lock_interner();
        for id in touched {
            if let Some(fragment) = fragments.get_mut(&id) {
                intern_data_file_lists(&mut intern, fragment);
            }
        }
        Ok(())
    }

    pub(super) fn share_data_file_lists<'a>(
        &self,
        fragments: impl IntoIterator<Item = &'a mut Fragment>,
    ) {
        let mut intern = self.lock_interner();
        for fragment in fragments {
            intern_data_file_lists(&mut intern, fragment);
        }
    }

    pub(super) async fn encode_leaf(&self, fragments: &[Fragment]) -> Result<bytes::Bytes> {
        let num_rows: usize = fragments.iter().map(|f| f.files.len() + 1).sum();
        let mut row_kind = Vec::with_capacity(num_rows);
        let mut frag_ids = Vec::with_capacity(num_rows);
        let mut fragment_meta = BinaryBuilder::new();
        let mut paths = Vec::with_capacity(num_rows);
        let mut major = Vec::with_capacity(num_rows);
        let mut minor = Vec::with_capacity(num_rows);
        let mut sizes = Vec::with_capacity(num_rows);
        let mut base_ids: Vec<Option<u32>> = Vec::with_capacity(num_rows);
        let list_item = ArrowField::new("item", DataType::Int32, false);
        let mut field_builder = ListBuilder::new(Int32Builder::new()).with_field(list_item.clone());
        let mut col_builder = ListBuilder::new(Int32Builder::new()).with_field(list_item);

        for f in fragments {
            let mut header = pb::DataFragment::from(f);
            header.files.clear();
            row_kind.push(ROW_KIND_FRAGMENT);
            frag_ids.push(f.id);
            fragment_meta.append_value(header.encode_to_vec());
            paths.push(String::new());
            field_builder.append(true);
            col_builder.append(true);
            major.push(0);
            minor.push(0);
            sizes.push(0);
            base_ids.push(None);
            for data_file in &f.files {
                row_kind.push(ROW_KIND_DATA_FILE);
                frag_ids.push(f.id);
                fragment_meta.append_null();
                paths.push(data_file.path.clone());
                for &field_id in data_file.fields.iter() {
                    field_builder.values().append_value(field_id);
                }
                field_builder.append(true);
                for &column_index in data_file.column_indices.iter() {
                    col_builder.values().append_value(column_index);
                }
                col_builder.append(true);
                major.push(data_file.file_major_version);
                minor.push(data_file.file_minor_version);
                sizes.push(
                    data_file
                        .file_size_bytes
                        .get()
                        .map(|size| size.get())
                        .unwrap_or_default(),
                );
                base_ids.push(data_file.base_id);
            }
        }

        let arrow_schema = leaf_arrow_schema();
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(UInt8Array::from(row_kind)),
                Arc::new(UInt64Array::from(frag_ids)),
                Arc::new(fragment_meta.finish()),
                Arc::new(StringArray::from(paths)),
                Arc::new(field_builder.finish()),
                Arc::new(col_builder.finish()),
                Arc::new(UInt32Array::from(major)),
                Arc::new(UInt32Array::from(minor)),
                Arc::new(UInt64Array::from(sizes)),
                Arc::new(UInt32Array::from(base_ids)),
            ],
        )?;

        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref())?;
        let memory = ObjectStore::memory();
        let path = Path::from("encoded-leaf");
        let writer = memory.create(&path).await?;
        let mut file_writer = lance_file::versions::create_writer(
            ConcreteFileVersion::V2_1,
            writer,
            lance_schema,
            FileWriterOptions::default(),
        )?;
        file_writer.write_batch(&batch).await?;
        file_writer.finish().await?;
        get_whole(&memory, &path).await
    }

    async fn write_encoded_leaf(
        &self,
        fragments: &[Fragment],
        bytes: bytes::Bytes,
        materialized_through_action_sequence: u64,
    ) -> Result<Written> {
        let path = self.leaf_path();
        let size = bytes.len() as u64;
        self.object_store
            .inner
            .put_opts(
                &self.resolve_path(path.as_ref())?,
                bytes.into(),
                PutOptions::default(),
            )
            .await?;
        Ok(Written {
            child_ref: node::leaf_ref(
                path.to_string(),
                fragments,
                size,
                materialized_through_action_sequence,
            )?,
            io_bytes: size,
        })
    }

    fn encoded_leaves<'a>(
        &'a self,
        fragments: &'a [Fragment],
        range: Range<usize>,
        config: &'a node::FragmentMetadataTreeConfig,
    ) -> impl futures::Stream<Item = Result<EncodedLeaf>> + 'a {
        futures::stream::try_unfold(vec![range], move |mut pending| async move {
            while let Some(range) = pending.pop() {
                if range.is_empty() {
                    continue;
                }
                let piece = &fragments[range.clone()];
                let bytes = self.encode_leaf(piece).await?;
                if bytes.len() as u64 > config.max_leaf_bytes && piece.len() > 1 {
                    let middle = range.start + piece.len() / 2;
                    pending.push(middle..range.end);
                    pending.push(range.start..middle);
                    continue;
                }
                if bytes.len() as u64 > config.hard_capacity_bytes {
                    return Err(Error::invalid_input(format!(
                        "fragment {} requires an encoded leaf of {} bytes, exceeding hard_capacity_bytes={}",
                        piece[0].id,
                        bytes.len(),
                        config.hard_capacity_bytes
                    )));
                }
                return Ok(Some((EncodedLeaf { range, bytes }, pending)));
            }
            Ok(None)
        })
    }

    async fn read_leaf_uncached(&self, child: &pb::FragmentMetadataChild) -> Result<Vec<Fragment>> {
        let (store, path) = self.resolve_child(child)?;
        let object_size = usize::try_from(child.object_size).map_err(|_| {
            Error::invalid_input(format!(
                "leaf object_size does not fit usize: path={}, object_size={}",
                child.path, child.object_size
            ))
        })?;
        let object_reader = Arc::new(SmallReader::new(store.inner.clone(), path, 3, object_size));
        let file_scheduler = self.scheduler.open_reader(object_reader.clone());
        let reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &self.cache,
            FileReaderOptions::default(),
        )
        .await?;

        // Opening the footer has already fetched the complete SmallReader
        // object through the scheduler. Inspect those cached bytes, not a HEAD
        // or a second GET, before trusting the reference's size.
        let actual_size = object_reader.get_all().await?.len() as u64;
        if actual_size != child.object_size {
            return Err(super::validation::corrupt(format!(
                "Leaf {} has {actual_size} bytes but its parent declares {}",
                child.path, child.object_size,
            )));
        }

        // A reference is untrusted until the leaf contents have been checked.
        let mut fragments: Vec<Fragment> =
            Vec::with_capacity(child.num_keys.min(READ_BATCH_ROWS as u64) as usize);
        let mut stream = reader
            .read_stream(
                ReadBatchParams::RangeFull,
                READ_BATCH_ROWS,
                READ_BATCH_READAHEAD,
                FilterExpression::no_filter(),
            )
            .await?;
        while let Some(batch) = stream.try_next().await? {
            if batch.schema().fields() != leaf_arrow_schema().fields() {
                return Err(super::validation::corrupt(format!(
                    "Leaf {} has an unexpected schema: {:?}",
                    child.path,
                    batch.schema()
                )));
            }
            for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                if !field.is_nullable() && column.null_count() != 0 {
                    return Err(super::validation::corrupt(format!(
                        "Leaf {} has null values in {}",
                        child.path,
                        field.name()
                    )));
                }
            }
            let col = |name: &str| {
                batch.column_by_name(name).ok_or_else(|| {
                    Error::invalid_input(format!("leaf {} is missing column {name}", child.path))
                })
            };
            let row_kinds = col("row_kind")?.as_primitive::<UInt8Type>();
            let frag_ids = col("frag_id")?.as_primitive::<UInt64Type>();
            let fragment_meta = col("fragment_meta")?.as_binary::<i32>();
            let paths = col("path")?.as_string::<i32>();
            let field_ids = col("field_ids")?.as_list::<i32>();
            let col_indices = col("column_indices")?.as_list::<i32>();
            let major = col("major_version")?.as_primitive::<UInt32Type>();
            let minor = col("minor_version")?.as_primitive::<UInt32Type>();
            let sizes = col("file_size_bytes")?.as_primitive::<UInt64Type>();
            let base_ids = col("base_id")?.as_primitive::<UInt32Type>();

            for row in 0..batch.num_rows() {
                let fid = frag_ids.value(row);
                if row_kinds.value(row) == ROW_KIND_FRAGMENT {
                    if fragment_meta.is_null(row) {
                        return Err(Error::invalid_input(format!(
                            "leaf FRAGMENT row frag_id={fid} is missing fragment_meta"
                        )));
                    }
                    let header = pb::DataFragment::decode(fragment_meta.value(row))?;
                    let fragment = super::validation::fragment(header)?;
                    if fragment.id != fid {
                        return Err(Error::invalid_input(format!(
                            "leaf FRAGMENT row frag_id={fid} does not match fragment_meta id={}",
                            fragment.id
                        )));
                    }
                    if !fragment.files.is_empty() {
                        return Err(Error::invalid_input(format!(
                            "leaf fragment_meta for frag_id={fid} unexpectedly contains {} data files",
                            fragment.files.len()
                        )));
                    }
                    // FRAGMENT rows carry fixed sentinels in the file columns.
                    if !paths.value(row).is_empty()
                        || !field_ids.value(row).is_empty()
                        || !col_indices.value(row).is_empty()
                        || major.value(row) != 0
                        || minor.value(row) != 0
                        || sizes.value(row) != 0
                        || !base_ids.is_null(row)
                    {
                        return Err(super::validation::corrupt(format!(
                            "Leaf {} FRAGMENT row for fragment {fid} has non sentinel file columns",
                            child.path
                        )));
                    }
                    if fragments.last().is_some_and(|previous| previous.id >= fid) {
                        return Err(super::validation::corrupt(format!(
                            "Leaf {} fragment ID {fid} is duplicated or out of order",
                            child.path
                        )));
                    }
                    fragments.push(fragment);
                    continue;
                }
                if row_kinds.value(row) != ROW_KIND_DATA_FILE {
                    return Err(super::validation::corrupt(format!(
                        "Leaf {} has unknown row_kind={} at row {row}",
                        child.path,
                        row_kinds.value(row)
                    )));
                }
                if !fragment_meta.is_null(row)
                    || field_ids.value(row).null_count() != 0
                    || col_indices.value(row).null_count() != 0
                {
                    return Err(super::validation::corrupt(format!(
                        "Leaf {} has an invalid file row for fragment {fid}",
                        child.path
                    )));
                }
                let fields_array = field_ids.value(row);
                let cols_array = col_indices.value(row);
                let fields_values = fields_array.as_primitive::<Int32Type>().values();
                let cols_values = cols_array.as_primitive::<Int32Type>().values();
                let (fields, cols) = {
                    let mut intern = self.lock_interner();
                    (
                        intern.intern_field_ids(fields_values),
                        intern.intern_column_indices(cols_values),
                    )
                };
                let base = (!base_ids.is_null(row)).then(|| base_ids.value(row));
                // Metadata replay must preserve the stored numbers, including
                // noncanonical pairs; decoding a data file is a separate step.
                let df = DataFile {
                    path: paths.value(row).to_string(),
                    fields,
                    column_indices: cols,
                    file_major_version: major.value(row),
                    file_minor_version: minor.value(row),
                    file_size_bytes: lance_io::utils::CachedFileSize::new(sizes.value(row)),
                    base_id: base,
                };
                let Some(fragment) = fragments.last_mut() else {
                    return Err(Error::invalid_input(format!(
                        "leaf DATA_FILE row frag_id={fid} precedes any FRAGMENT row"
                    )));
                };
                if fragment.id != fid {
                    return Err(Error::invalid_input(format!(
                        "leaf DATA_FILE row frag_id={fid} does not follow its FRAGMENT row \
                         (current fragment {})",
                        fragment.id
                    )));
                }
                fragment.files.push(df);
            }
        }
        let mut summary = node::leaf_ref(
            child.path.clone(),
            &fragments,
            child.object_size,
            child.materialized_through_action_sequence,
        )?;
        // The fence comes from the parent and may sit below the first stored key.
        if child.min_key > summary.min_key {
            return Err(super::validation::corrupt(format!(
                "Leaf {} fence {} is above its first fragment {}",
                child.path, child.min_key, summary.min_key
            )));
        }
        summary.min_key = child.min_key;
        summary.base_id = child.base_id;
        if summary.num_keys != child.num_keys
            || summary.total_rows != child.total_rows
            || summary.visible_rows != child.visible_rows
            || summary.height != 0
            || summary.num_children != 0
        {
            return Err(super::validation::corrupt(format!(
                "Leaf {} contents differ from its parent reference",
                child.path
            )));
        }
        if let Some(base_id) = child.base_id {
            let (store, base) = self.child_dataset(child)?;
            preserve_inherited_fragments(
                &mut fragments,
                SourceStore {
                    object_store: store.as_ref(),
                    base: &base,
                    base_id,
                },
            )
            .await?;
        }
        Ok(fragments)
    }

    fn leaf_path(&self) -> Path {
        Path::from("_bt/leaf").join(format!("{}.lance", Uuid::new_v4()))
    }

    fn node_path(&self) -> Path {
        Path::from("_bt/node").join(format!("{}.node", Uuid::new_v4()))
    }

    fn resolve_path(&self, path: &str) -> Result<Path> {
        self.resolve_under(&self.base, path)
    }

    fn resolve_child(&self, child: &pb::FragmentMetadataChild) -> Result<(Arc<ObjectStore>, Path)> {
        let (store, base) = self.child_dataset(child)?;
        Ok((store, self.resolve_under(&base, &child.path)?))
    }

    fn resolve_under(&self, base: &Path, path: &str) -> Result<Path> {
        let relative = Path::parse(path).map_err(|error| {
            super::validation::corrupt(format!("Invalid metadata path {path:?}: {error}"))
        })?;
        if !path.starts_with("_bt/") || path != relative.as_ref() {
            return Err(super::validation::corrupt(format!(
                "Metadata path {path:?} must be relative to _bt/"
            )));
        }
        let mut resolved = base.clone();
        resolved.extend(&relative);
        Ok(resolved)
    }

    fn child_dataset(&self, child: &pb::FragmentMetadataChild) -> Result<(Arc<ObjectStore>, Path)> {
        match child.base_id {
            Some(id) => {
                let foreign = self.foreign_bases.get(&id).ok_or_else(|| {
                    super::validation::corrupt(format!(
                        "Tree child {} names unknown base_id {id}",
                        child.path
                    ))
                })?;
                Ok((foreign.store.clone(), foreign.base.clone()))
            }
            None => Ok((self.object_store.clone(), self.base.clone())),
        }
    }

    fn lock_interner(&self) -> std::sync::MutexGuard<'_, DataFileFieldInterner> {
        self.interner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Fetch a whole object through the object-store trait, never through the
/// local-filesystem fast path, so store wrappers (tracking, injected
/// latency, caches) observe all metadata reads.
pub async fn get_whole(object_store: &ObjectStore, path: &Path) -> Result<bytes::Bytes> {
    let result = object_store
        .inner
        .get_opts(path, object_store::GetOptions::default())
        .await?;
    Ok(result.bytes().await?)
}

fn intern_data_file_lists(intern: &mut DataFileFieldInterner, fragment: &mut Fragment) {
    for file in fragment.referenced_lance_files_mut() {
        file.fields = intern.intern_field_ids(file.fields.as_ref());
        file.column_indices = intern.intern_column_indices(file.column_indices.as_ref());
    }
}

fn int_list_type() -> DataType {
    DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, false)))
}

/// A fragment header row; its DATA_FILE rows follow in file order.
const ROW_KIND_FRAGMENT: u8 = 0;
/// One data file of the most recent FRAGMENT row.
const ROW_KIND_DATA_FILE: u8 = 1;

/// Columnar leaf schema: a FRAGMENT header row per fragment, then one
/// DATA_FILE row per data file with each `DataFile` field in its own column.
fn leaf_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        ArrowField::new("row_kind", DataType::UInt8, false),
        ArrowField::new("frag_id", DataType::UInt64, false),
        // FRAGMENT rows retain all metadata except the separately stored files.
        ArrowField::new("fragment_meta", DataType::Binary, true),
        // DATA_FILE rows only.
        ArrowField::new("path", DataType::Utf8, false),
        ArrowField::new("field_ids", int_list_type(), false),
        ArrowField::new("column_indices", int_list_type(), false),
        ArrowField::new("major_version", DataType::UInt32, false),
        ArrowField::new("minor_version", DataType::UInt32, false),
        ArrowField::new("file_size_bytes", DataType::UInt64, false), // 0 = unknown
        // Absent means the file lives in the dataset that owns this leaf. A
        // leaf inherited through a clone stamps its child reference's base_id
        // on read, so the file still resolves against the source dataset.
        ArrowField::new("base_id", DataType::UInt32, true),
    ]))
}

/// Object store and dataset root that own inherited file and lineage references.
#[derive(Clone, Copy)]
pub struct SourceStore<'a> {
    pub object_store: &'a ObjectStore,
    pub base: &'a Path,
    pub base_id: u32,
}

fn stamp_unset_base_id(fragment: &mut Fragment, base_id: u32) {
    for file in fragment.referenced_lance_files_mut() {
        if file.base_id.is_none() {
            file.base_id = Some(base_id);
        }
    }
    if let Some(deletion) = &mut fragment.deletion_file
        && deletion.base_id.is_none()
    {
        deletion.base_id = Some(base_id);
    }
}

async fn preserve_inherited_fragments(
    fragments: &mut [Fragment],
    source: SourceStore<'_>,
) -> Result<()> {
    for fragment in fragments.iter_mut() {
        stamp_unset_base_id(fragment, source.base_id);
    }
    inline_external_lineage(source.object_store, source.base, fragments).await
}

/// Stamp unset file `base_id`s and inline source-relative lineage.
pub async fn preserve_inherited_refs(
    mutations: &mut [pb::FragmentMetadataMutation],
    source: SourceStore<'_>,
) -> Result<()> {
    for tagged in mutations {
        let Some(action) = tagged
            .action
            .as_mut()
            .and_then(|action| action.action.as_mut())
        else {
            continue;
        };
        match action {
            Action::AddFragment(encoded) => {
                let mut fragment = Fragment::try_from(encoded.clone())?;
                preserve_inherited_fragments(std::slice::from_mut(&mut fragment), source).await?;
                *encoded = pb::DataFragment::from(&fragment);
            }
            Action::AddDataFile(value) => {
                if let Some(file) = value.file.as_mut()
                    && file.base_id.is_none()
                {
                    file.base_id = Some(source.base_id);
                }
            }
            Action::AddDeletionFile(value) => {
                if let Some(file) = value.deletion_file.as_mut()
                    && file.base_id.is_none()
                {
                    file.base_id = Some(source.base_id);
                }
            }
            Action::ReplaceDataFile(value) if value.base_id.is_none() => {
                value.base_id = Some(source.base_id);
            }
            _ => {}
        }
    }
    Ok(())
}

/// External lineage has no base_id. A source-relative path on a clone would
/// bind to the destination dataset.
pub async fn inline_external_lineage(
    store: &ObjectStore,
    base: &Path,
    fragments: &mut [Fragment],
) -> Result<()> {
    async fn read_slice(
        store: &ObjectStore,
        base: &Path,
        slice: &crate::format::ExternalFile,
    ) -> Result<Vec<u8>> {
        let start = usize::try_from(slice.offset).map_err(|_| {
            Error::invalid_input(format!(
                "lineage offset {} on {} exceeds usize",
                slice.offset, slice.path
            ))
        })?;
        let size = usize::try_from(slice.size).map_err(|_| {
            Error::invalid_input(format!(
                "lineage size {} on {} exceeds usize",
                slice.size, slice.path
            ))
        })?;
        let end = start.checked_add(size).ok_or_else(|| {
            Error::invalid_input(format!(
                "lineage byte range overflow on {} at offset {}",
                slice.path, slice.offset
            ))
        })?;
        let mut path = base.clone();
        path.extend(&Path::parse(&slice.path)?);
        Ok(store
            .open(&path)
            .await?
            .get_range(start..end)
            .await?
            .to_vec())
    }
    for fragment in fragments {
        if let Some(RowIdMeta::External(slice)) = &fragment.row_id_meta {
            let bytes = read_slice(store, base, slice).await?;
            fragment.row_id_meta = Some(RowIdMeta::Inline(bytes.into()));
        }
        for metadata in [
            &mut fragment.created_at_version_meta,
            &mut fragment.last_updated_at_version_meta,
        ] {
            if let Some(RowDatasetVersionMeta::External(slice)) = metadata.as_ref() {
                let bytes = read_slice(store, base, slice).await?;
                *metadata = Some(RowDatasetVersionMeta::Inline(bytes.into()));
            }
        }
    }
    Ok(())
}

fn derived_child_fields_match(
    summary: &pb::FragmentMetadataChild,
    child: &pb::FragmentMetadataChild,
) -> bool {
    summary.num_keys == child.num_keys
        && summary.total_rows == child.total_rows
        && summary.visible_rows == child.visible_rows
        && summary.height == child.height
        && summary.num_children == child.num_children
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::{
        DeletionFile, DeletionFileType, ExternalFile, RowDatasetVersionMeta, RowIdMeta,
    };
    use crate::fragment_metadata::action;
    use crate::fragment_metadata::support::{
        make_backfill_data_file, make_fragment, make_fragment_with_files,
    };
    use lance_core::utils::tempfile::TempObjDir;
    use lance_io::scheduler::SchedulerConfig;
    use object_store::ObjectStoreExt;
    use rstest::rstest;

    #[rstest]
    #[case::historical_numbers(0, 3)]
    #[case::unknown_numbers(17, 42)]
    #[tokio::test]
    async fn leaf_preserves_data_file_version_numbers(#[case] major: u32, #[case] minor: u32) {
        let base = TempObjDir::default();
        let store = test_store(base.clone());
        let mut fragment = make_fragment(0);
        fragment.files[0].file_major_version = major;
        fragment.files[0].file_minor_version = minor;
        let written = store.write_leaf(&[fragment.clone()], 0).await.unwrap();
        assert_eq!(
            store.read_leaf(&written.child_ref).await.unwrap(),
            vec![fragment]
        );
    }

    #[tokio::test]
    async fn internal_read_rejects_an_understated_object_size() {
        let base = TempObjDir::default();
        let store = test_store(base.clone());
        let children = (0..2)
            .map(|id| {
                node::leaf_ref(format!("_bt/leaf/{id}.lance"), &[make_fragment(id)], 1, 0).unwrap()
            })
            .collect();
        let mut written = store.write_internal(children, Vec::new()).await.unwrap();
        written.child_ref.object_size = 1;
        let error = store.read_internal(&written.child_ref).await.unwrap_err();
        assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
        assert!(error.to_string().contains("parent declares"), "{error}");
    }

    #[tokio::test]
    async fn root_read_preserves_unknown_protobuf_fields_without_writer_limits() {
        let base = TempObjDir::default();
        let store = test_store(base.clone());
        let mut bytes = pb::FragmentMetadataRoot::default().encode_to_vec();
        bytes.extend_from_slice(&[0xa2, 0x06, 0x80, 0x01]);
        bytes.extend_from_slice(&[0; 128]);
        let path = "_bt/base/oversized.root";
        store
            .object_store
            .put(&store.resolve_path(path).unwrap(), &bytes)
            .await
            .unwrap();
        store.read_root_base(path).await.unwrap();
    }

    #[tokio::test]
    async fn leaf_read_rejects_an_understated_object_size() {
        let base = TempObjDir::default();
        let store = test_store(base.clone());
        let written = store.write_leaf(&[make_fragment(0)], 0).await.unwrap();
        let path = store.resolve_path(&written.child_ref.path).unwrap();
        let mut bytes = get_whole(&store.object_store, &path)
            .await
            .unwrap()
            .to_vec();
        bytes.push(0);
        store.object_store.put(&path, &bytes).await.unwrap();
        let error = store.read_leaf(&written.child_ref).await.unwrap_err();
        assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
        assert!(error.to_string().contains("parent declares"), "{error}");
    }

    fn test_store(base: Path) -> NodeStore {
        let object_store = Arc::new(ObjectStore::local());
        let scheduler =
            ScanScheduler::new(object_store.clone(), SchedulerConfig::default_for_testing());
        NodeStore::new(
            object_store,
            base,
            scheduler,
            Arc::new(LanceCache::with_capacity(64 * 1024 * 1024)),
        )
    }

    #[tokio::test]
    async fn encoded_leaf_target_and_oversized_singleton_limit() {
        let base = TempObjDir::default();
        let store = test_store(base.clone());
        let config = node::FragmentMetadataTreeConfig::new(4096, 8)
            .with_max_leaf_bytes(8192)
            .with_hard_capacity_bytes(4 * 1024 * 1024);
        let fragments: Vec<_> = (0..256).map(make_fragment).collect();
        let written = store.write_leaves(&fragments, 17, &config).await.unwrap();
        assert!(written.len() > 1);
        let mut read = Vec::new();
        for leaf in &written {
            assert!(leaf.io_bytes <= config.max_leaf_bytes || leaf.child_ref.num_keys == 1);
            assert_eq!(leaf.child_ref.object_size, leaf.io_bytes);
            assert_eq!(leaf.child_ref.materialized_through_action_sequence, 17);
            read.extend(store.read_leaf(&leaf.child_ref).await.unwrap());
        }
        assert_eq!(read, fragments);

        let wide = make_fragment_with_files(257, 4096);
        let written = store
            .write_leaves(std::slice::from_ref(&wide), 18, &config)
            .await
            .unwrap();
        assert_eq!(written.len(), 1);
        assert!(written[0].io_bytes > config.max_leaf_bytes);
        assert_eq!(
            store.read_leaf(&written[0].child_ref).await.unwrap(),
            vec![wide.clone()]
        );
        let before = store
            .object_store
            .read_dir_all(&store.base.clone().join("_bt").join("leaf"), None)
            .map_ok(|meta| meta.location)
            .try_collect::<std::collections::BTreeSet<_>>()
            .await
            .unwrap();
        let restricted = config.with_hard_capacity_bytes(16 * 1024);
        let error = store
            .write_leaves(&[wide], 19, &restricted)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
        assert!(error.to_string().contains("fragment 257"));
        assert!(error.to_string().contains("hard_capacity_bytes=16384"));
        assert_eq!(
            store
                .object_store
                .read_dir_all(&store.base.clone().join("_bt").join("leaf"), None)
                .map_ok(|meta| meta.location)
                .try_collect::<std::collections::BTreeSet<_>>()
                .await
                .unwrap(),
            before
        );
    }

    #[tokio::test]
    async fn leaf_round_trips_fragment_metadata_and_empty_fragment() {
        let mut fragment = make_fragment(7);
        fragment.deletion_file = Some(DeletionFile {
            read_version: 3,
            id: 11,
            file_type: DeletionFileType::Bitmap,
            num_deleted_rows: Some(1),
            base_id: Some(2),
        });
        fragment.row_id_meta = Some(RowIdMeta::Inline(vec![1, 2, 3, 4].into()));
        fragment.created_at_version_meta =
            Some(RowDatasetVersionMeta::Inline(Arc::from([5, 6, 7])));
        fragment.last_updated_at_version_meta =
            Some(RowDatasetVersionMeta::Inline(Arc::from([8, 9, 10])));

        let mut empty_fragment = Fragment::new(8);
        empty_fragment.physical_rows = Some(12);
        empty_fragment.row_id_meta = Some(RowIdMeta::Inline(vec![12, 13].into()));

        let expected = vec![fragment, empty_fragment];
        let tempdir = TempObjDir::default();
        let store = test_store(tempdir.clone().join("fragment_metadata"));
        let written = store.write_leaf(&expected, 0).await.unwrap();
        let actual = store.read_leaf(&written.child_ref).await.unwrap();

        assert_eq!(actual, expected);
    }
    #[tokio::test]
    async fn validation_spill_preserves_known_zero_counts_and_relative_paths() {
        let base = TempObjDir::default();
        let mut store = test_store(base.clone());
        let mut fragment = make_fragment(0);
        fragment.physical_rows = Some(0);
        fragment.deletion_file = Some(DeletionFile {
            read_version: 1,
            id: 1,
            file_type: DeletionFileType::Bitmap,
            num_deleted_rows: Some(0),
            base_id: None,
        });
        let written = store
            .write_leaf(std::slice::from_ref(&fragment), 0)
            .await
            .unwrap();
        assert!(written.child_ref.path.starts_with("_bt/leaf/"));
        assert!(!written.child_ref.path.contains("%2F"));
        let path = store.resolve_path(&written.child_ref.path).unwrap();
        assert!(store.object_store.inner.head(&path).await.is_ok());
        store.retain_validation_reads().unwrap();
        // First read populates scratch; the second decodes the scratch record.
        for _ in 0..2 {
            assert_eq!(
                store.read_leaf(&written.child_ref).await.unwrap(),
                vec![fragment.clone()]
            );
        }
        for path in [
            "../_bt/leaf/escape",
            "/_bt/leaf/absolute",
            "data/not-metadata",
        ] {
            assert!(store.resolve_path(path).is_err(), "{path}");
        }
    }

    #[tokio::test]
    async fn foreign_interior_buffer_inherits_dataset_on_file_refs() {
        let base = TempObjDir::default();
        let mut store = test_store(base.clone());
        let children = (0..2)
            .map(|id| {
                node::leaf_ref(format!("_bt/leaf/{id}.lance"), &[make_fragment(id)], 1, 0).unwrap()
            })
            .collect();
        let file = make_backfill_data_file(0, 0);
        let mutation = pb::FragmentMetadataMutation {
            action_sequence: 1,
            action: Some(action::add_data_file(0, &file)),
            fragment_count_delta: 0,
            total_rows_delta: 0,
            visible_rows_delta: 0,
        };
        let mut written = store
            .write_internal(children, vec![mutation])
            .await
            .unwrap();
        let object_store = store.object_store.clone();
        let dataset_base = store.base.clone();
        store.set_foreign_bases(HashMap::from([(7, (object_store, dataset_base))]));
        written.child_ref.base_id = Some(7);
        let node = store.read_internal(&written.child_ref).await.unwrap();
        let Some(pb::fragment_action::Action::AddDataFile(add)) = node.buffer[0]
            .action
            .as_ref()
            .and_then(|a| a.action.as_ref())
        else {
            panic!("expected AddDataFile");
        };
        assert_eq!(add.file.as_ref().and_then(|file| file.base_id), Some(7));
    }

    #[tokio::test]
    async fn foreign_interior_buffer_inlines_add_fragment_lineage() {
        let source_dir = TempObjDir::default();
        let source = test_store(source_dir.clone());
        source
            .object_store
            .put(
                &source.resolve_path("_bt/lineage").unwrap(),
                &[10, 11, 12, 13, 14],
            )
            .await
            .unwrap();
        let slice = ExternalFile {
            path: "_bt/lineage".into(),
            offset: 1,
            size: 3,
        };
        let mut fragment = make_fragment(2);
        fragment.row_id_meta = Some(RowIdMeta::External(slice.clone()));
        fragment.created_at_version_meta = Some(RowDatasetVersionMeta::External(slice.clone()));
        fragment.last_updated_at_version_meta = Some(RowDatasetVersionMeta::External(slice));
        let mutation = pb::FragmentMetadataMutation {
            action_sequence: 1,
            action: Some(action::add_fragment(&fragment)),
            fragment_count_delta: 1,
            total_rows_delta: fragment.physical_rows.unwrap_or(0) as i64,
            visible_rows_delta: fragment.num_rows().unwrap_or(0) as i64,
        };
        let children = (0..2)
            .map(|id| {
                node::leaf_ref(format!("_bt/leaf/{id}.lance"), &[make_fragment(id)], 1, 0).unwrap()
            })
            .collect();
        let mut written = source
            .write_internal(children, vec![mutation])
            .await
            .unwrap();
        let dest_dir = TempObjDir::default();
        let mut dest = test_store(dest_dir.clone());
        dest.set_foreign_bases(HashMap::from([(
            7,
            (source.object_store.clone(), source.base.clone()),
        )]));
        written.child_ref.base_id = Some(7);
        let node = dest.read_internal(&written.child_ref).await.unwrap();
        let Some(pb::fragment_action::Action::AddFragment(encoded)) = node.buffer[0]
            .action
            .as_ref()
            .and_then(|a| a.action.as_ref())
        else {
            panic!("expected AddFragment");
        };
        let read = Fragment::try_from(encoded.clone()).unwrap();
        assert_eq!(
            read.row_id_meta,
            Some(RowIdMeta::Inline(vec![11, 12, 13].into()))
        );
        assert_eq!(
            read.created_at_version_meta,
            Some(RowDatasetVersionMeta::Inline(Arc::from([11, 12, 13])))
        );
        assert_eq!(read.files[0].base_id, Some(7));
    }

    #[tokio::test]
    async fn leaf_reads_share_field_list_allocations_across_leaves() {
        let base = TempObjDir::default();
        let store = test_store(base.clone());
        let first = store.write_leaf(&[make_fragment(0)], 0).await.unwrap();
        let second = store.write_leaf(&[make_fragment(1)], 0).await.unwrap();
        let a = store.read_leaf(&first.child_ref).await.unwrap();
        let b = store.read_leaf(&second.child_ref).await.unwrap();
        assert!(std::sync::Arc::ptr_eq(
            &a[0].files[0].fields,
            &b[0].files[0].fields
        ));
        assert!(std::sync::Arc::ptr_eq(
            &a[0].files[0].column_indices,
            &b[0].files[0].column_indices
        ));
    }

    #[tokio::test]
    async fn foreign_leaf_inlines_external_lineage() {
        let source_dir = TempObjDir::default();
        let source = test_store(source_dir.clone());
        source
            .object_store
            .put(
                &source.resolve_path("_bt/lineage").unwrap(),
                &[10, 11, 12, 13, 14],
            )
            .await
            .unwrap();
        let slice = ExternalFile {
            path: "_bt/lineage".into(),
            offset: 1,
            size: 3,
        };
        let mut fragment = make_fragment(0);
        fragment.row_id_meta = Some(RowIdMeta::External(slice.clone()));
        fragment.created_at_version_meta = Some(RowDatasetVersionMeta::External(slice.clone()));
        fragment.last_updated_at_version_meta = Some(RowDatasetVersionMeta::External(slice));
        let written = source
            .write_leaf(std::slice::from_ref(&fragment), 0)
            .await
            .unwrap();

        let dest_dir = TempObjDir::default();
        let mut dest = test_store(dest_dir.clone());
        dest.set_foreign_bases(HashMap::from([(
            4,
            (source.object_store.clone(), source.base.clone()),
        )]));
        let mut child = written.child_ref.clone();
        child.base_id = Some(4);
        let read = dest.read_leaf(&child).await.unwrap();
        assert_eq!(
            read[0].row_id_meta,
            Some(RowIdMeta::Inline(vec![11, 12, 13].into()))
        );
        assert_eq!(
            read[0].created_at_version_meta,
            Some(RowDatasetVersionMeta::Inline(Arc::from([11, 12, 13])))
        );
        assert_eq!(
            read[0].last_updated_at_version_meta,
            read[0].created_at_version_meta
        );
        assert_eq!(read[0].files[0].base_id, Some(4));
    }
}
