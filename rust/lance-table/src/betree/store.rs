// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! PROTOTYPE (discussion #7499): copy-on-write node IO.
//!
//! Nodes are immutable object-store files; every rewrite produces a new file
//! (uuid-named), so flush/split/merge just write new files and the parent
//! repoints. Internal nodes and the root are protobuf objects.
//!
//! Leaves are tabular Lance v2 files with **one row per data file** and each
//! `DataFile` field in its own column (path, versions, size, base_id, field
//! ids). Decomposing the data files into columns lets Lance compress each column
//! independently (identical file versions RLE to ~nothing, sizes cluster, paths
//! dictionary/FSST-encode), which is the columnar win @Xuanwo measured. A
//! fragment's metadata is encoded once on its first row. A fragment with no data
//! files gets one explicit marker row, so every fragment can round-trip.

use std::num::NonZero;
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, Int32Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, UInt32Type, UInt64Type};
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use prost::Message;

use crate::betree::node::{self, InternalNode};
use crate::format::pb;
use crate::format::{DataFile, Fragment};
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::{Error, Result};
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_file::writer::{FileWriter, FileWriterOptions};
use lance_io::ReadBatchParams;
use lance_io::object_reader::SmallReader;
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::ScanScheduler;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore as OSObjectStore, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const READ_BATCH_ROWS: u32 = 16 * 1024;
const READ_BATCH_READAHEAD: u32 = 16;

/// A written node: its parent `ChildRef` (with logical byte size for tree logic)
/// plus the actual bytes written to storage (for write-amplification accounting).
pub struct Written {
    pub child_ref: pb::ChildRef,
    pub io_bytes: u64,
}

#[derive(Serialize, Deserialize)]
struct VersionHint {
    version: u64,
}

/// Reads and writes Bε-tree node files against an object store.
pub struct NodeStore {
    object_store: Arc<ObjectStore>,
    base: Path,
    scheduler: Arc<ScanScheduler>,
    cache: Arc<LanceCache>,
}

fn int_list_type() -> DataType {
    DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, true)))
}

/// Columnar leaf schema: one row per data file, each `DataFile` field its own column.
fn leaf_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        ArrowField::new("frag_id", DataType::UInt64, false),
        ArrowField::new("physical_rows", DataType::UInt64, false), // 0 = unknown
        // Full fragment metadata with `files` cleared, present on the first row
        // for each fragment. This retains deletion, row-id, version, and overlay
        // metadata without duplicating the separately columnized data files.
        ArrowField::new("fragment_meta", DataType::Binary, true),
        ArrowField::new("has_data_file", DataType::Boolean, false),
        ArrowField::new("path", DataType::Utf8, false),
        ArrowField::new("field_ids", int_list_type(), false),
        ArrowField::new("column_indices", int_list_type(), false),
        ArrowField::new("major_version", DataType::UInt32, false),
        ArrowField::new("minor_version", DataType::UInt32, false),
        ArrowField::new("file_size_bytes", DataType::UInt64, false), // 0 = unknown
        ArrowField::new("base_id", DataType::UInt32, true),          // null = None
    ]))
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
        }
    }

    fn leaf_path(&self) -> Path {
        self.base
            .clone()
            .join("_bt")
            .join("leaf")
            .join(format!("{}.lance", Uuid::new_v4()))
    }
    fn node_path(&self) -> Path {
        self.base
            .clone()
            .join("_bt")
            .join("node")
            .join(format!("{}.node", Uuid::new_v4()))
    }
    fn root_path(&self, version: u64) -> Path {
        self.base
            .clone()
            .join("_bt")
            .join("root")
            .join(format!("{version}.root"))
    }
    fn hint_path(&self) -> Path {
        self.base
            .clone()
            .join("_bt")
            .join("root")
            .join("latest_hint.json")
    }

    /// Write a leaf (sorted fragments) as a columnar Lance file.
    ///
    /// Each data file occupies one row. A fragment with no data files occupies
    /// one marker row, and full non-file metadata is stored once per fragment.
    /// Returns a leaf `ChildRef` (logical byte size) + actual bytes written.
    pub async fn write_leaf(&self, fragments: &[Fragment]) -> Result<Written> {
        let num_rows: usize = fragments.iter().map(|f| f.files.len().max(1)).sum();
        let mut frag_ids = Vec::with_capacity(num_rows);
        let mut physical = Vec::with_capacity(num_rows);
        let mut fragment_meta = BinaryBuilder::new();
        let mut has_data_file = Vec::with_capacity(num_rows);
        let mut paths = Vec::with_capacity(num_rows);
        let mut major = Vec::with_capacity(num_rows);
        let mut minor = Vec::with_capacity(num_rows);
        let mut sizes = Vec::with_capacity(num_rows);
        let mut base_ids: Vec<Option<u32>> = Vec::with_capacity(num_rows);
        let mut field_builder = ListBuilder::new(Int32Builder::new());
        let mut col_builder = ListBuilder::new(Int32Builder::new());

        for f in fragments {
            let pr = f.physical_rows.unwrap_or(0) as u64;
            let mut metadata = pb::DataFragment::from(f);
            metadata.files.clear();
            let metadata = metadata.encode_to_vec();

            let mut append_row = |df: Option<&DataFile>, is_first: bool| {
                frag_ids.push(f.id);
                physical.push(pr);
                if is_first {
                    fragment_meta.append_value(&metadata);
                } else {
                    fragment_meta.append_null();
                }
                has_data_file.push(df.is_some());
                paths.push(df.map(|file| file.path.clone()).unwrap_or_default());
                major.push(df.map(|file| file.file_major_version).unwrap_or_default());
                minor.push(df.map(|file| file.file_minor_version).unwrap_or_default());
                sizes.push(
                    df.and_then(|file| file.file_size_bytes.get())
                        .map(|size| size.get())
                        .unwrap_or_default(),
                );
                base_ids.push(df.and_then(|file| file.base_id));
                for &field_id in df.into_iter().flat_map(|file| file.fields.iter()) {
                    field_builder.values().append_value(field_id);
                }
                field_builder.append(true);
                for &column_index in df.into_iter().flat_map(|file| file.column_indices.iter()) {
                    col_builder.values().append_value(column_index);
                }
                col_builder.append(true);
            };

            if f.files.is_empty() {
                append_row(None, true);
            } else {
                for (index, data_file) in f.files.iter().enumerate() {
                    append_row(Some(data_file), index == 0);
                }
            }
        }

        let arrow_schema = leaf_arrow_schema();
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(frag_ids)),
                Arc::new(UInt64Array::from(physical)),
                Arc::new(fragment_meta.finish()),
                Arc::new(BooleanArray::from(has_data_file)),
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
        let path = self.leaf_path();
        let writer = self.object_store.create(&path).await?;
        let mut file_writer =
            FileWriter::try_new(writer, lance_schema, FileWriterOptions::default())?;
        file_writer.write_batch(&batch).await?;
        let summary = file_writer.finish().await?;

        let logical = node::leaf_logical_bytes(fragments);
        Ok(Written {
            child_ref: node::leaf_ref(path.to_string(), fragments, logical, summary.size_bytes)?,
            io_bytes: summary.size_bytes,
        })
    }

    /// Read a columnar leaf back into a fragment list (rows grouped by frag_id).
    pub async fn read_leaf(&self, child: &pb::ChildRef) -> Result<Vec<Fragment>> {
        let path = Path::from(child.node_path.as_str());
        let object_size = usize::try_from(child.object_size).map_err(|_| {
            Error::invalid_input(format!(
                "leaf object_size does not fit usize: node_path={}, object_size={}",
                child.node_path, child.object_size
            ))
        })?;
        let reader = Arc::new(SmallReader::new(
            self.object_store.inner.clone(),
            path,
            3,
            object_size,
        ));
        let file_scheduler = self.scheduler.open_reader(reader);
        let reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &self.cache,
            FileReaderOptions::default(),
        )
        .await?;

        let mut fragments: Vec<Fragment> = Vec::with_capacity(child.num_keys as usize);
        let mut stream = reader
            .read_stream(
                ReadBatchParams::RangeFull,
                READ_BATCH_ROWS,
                READ_BATCH_READAHEAD,
                FilterExpression::no_filter(),
            )
            .await?;
        while let Some(batch) = stream.try_next().await? {
            let col = |name: &str| {
                batch
                    .column_by_name(name)
                    .ok_or_else(|| Error::invalid_input(format!("leaf missing column {name}")))
            };
            let frag_ids = col("frag_id")?.as_primitive::<UInt64Type>();
            let fragment_meta = col("fragment_meta")?.as_binary::<i32>();
            let has_data_file = col("has_data_file")?.as_boolean();
            let paths = col("path")?.as_string::<i32>();
            let field_ids = col("field_ids")?.as_list::<i32>();
            let col_indices = col("column_indices")?.as_list::<i32>();
            let major = col("major_version")?.as_primitive::<UInt32Type>();
            let minor = col("minor_version")?.as_primitive::<UInt32Type>();
            let sizes = col("file_size_bytes")?.as_primitive::<UInt64Type>();
            let base_ids = col("base_id")?.as_primitive::<UInt32Type>();

            for row in 0..batch.num_rows() {
                let fid = frag_ids.value(row);
                if fragments.last().map(|f| f.id) != Some(fid) {
                    if fragment_meta.is_null(row) {
                        return Err(Error::invalid_input(format!(
                            "leaf fragment frag_id={fid} is missing fragment_meta on its first row"
                        )));
                    }
                    let fragment_pb = pb::DataFragment::decode(fragment_meta.value(row))?;
                    let fragment = Fragment::try_from(fragment_pb)?;
                    if fragment.id != fid {
                        return Err(Error::invalid_input(format!(
                            "leaf row frag_id={fid} does not match fragment_meta id={}",
                            fragment.id
                        )));
                    }
                    if !fragment.files.is_empty() {
                        return Err(Error::invalid_input(format!(
                            "leaf fragment_meta for frag_id={fid} unexpectedly contains {} data files",
                            fragment.files.len()
                        )));
                    }
                    fragments.push(fragment);
                }
                if !has_data_file.value(row) {
                    continue;
                }
                let fields = field_ids
                    .value(row)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec();
                let cols = col_indices
                    .value(row)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec();
                let base = (!base_ids.is_null(row)).then(|| base_ids.value(row));
                let df = DataFile::new(
                    paths.value(row),
                    fields,
                    cols,
                    major.value(row),
                    minor.value(row),
                    NonZero::new(sizes.value(row)),
                    base,
                );
                let Some(fragment) = fragments.last_mut() else {
                    return Err(Error::invalid_input(format!(
                        "leaf data-file row has no fragment for frag_id={fid}"
                    )));
                };
                fragment.files.push(df);
            }
        }
        Ok(fragments)
    }

    /// Write an internal node (children + buffer) as a protobuf object.
    pub async fn write_internal(
        &self,
        children: Vec<pb::ChildRef>,
        buffer: Vec<pb::TaggedAction>,
    ) -> Result<Written> {
        let logical = node::internal_logical_bytes(&children, &buffer);
        let path = self.node_path();
        let node = pb::InternalNode {
            children: children.clone(),
            buffer,
        };
        let bytes = node.encode_to_vec();
        let io_bytes = bytes.len() as u64;
        self.object_store
            .inner
            .put_opts(&path, PutPayload::from(bytes), PutOptions::default())
            .await?;
        Ok(Written {
            child_ref: node::internal_ref(
                path.to_string(),
                &children,
                &node.buffer,
                logical,
                io_bytes,
            )?,
            io_bytes,
        })
    }

    /// Read an internal node.
    pub async fn read_internal(&self, child: &pb::ChildRef) -> Result<InternalNode> {
        let path = Path::from(child.node_path.as_str());
        let bytes = self
            .object_store
            .inner
            .get_opts(&path, GetOptions::default())
            .await?
            .bytes()
            .await?;
        let node = pb::InternalNode::decode(bytes)?;
        Ok(InternalNode {
            children: node.children,
            buffer: node.buffer,
        })
    }

    /// Write the root object and update the latest-version hint. Returns bytes written.
    pub async fn write_root(&self, root: &pb::BeTreeRoot) -> Result<u64> {
        let bytes = root.encode_to_vec();
        let size = bytes.len() as u64;
        self.object_store
            .inner
            .put_opts(
                &self.root_path(root.version),
                PutPayload::from(bytes),
                PutOptions::default(),
            )
            .await?;
        let hint = serde_json::to_vec(&VersionHint {
            version: root.version,
        })
        .map_err(|e| Error::invalid_input(format!("failed to encode hint: {e}")))?;
        self.object_store
            .inner
            .put_opts(
                &self.hint_path(),
                PutPayload::from(hint),
                PutOptions::default(),
            )
            .await?;
        Ok(size)
    }

    pub async fn read_latest_version(&self) -> Result<u64> {
        let bytes = self
            .object_store
            .inner
            .get_opts(&self.hint_path(), GetOptions::default())
            .await?
            .bytes()
            .await?;
        let hint: VersionHint = serde_json::from_slice(&bytes)
            .map_err(|e| Error::invalid_input(format!("failed to decode hint: {e}")))?;
        Ok(hint.version)
    }

    pub async fn read_root(&self, version: u64) -> Result<pb::BeTreeRoot> {
        let bytes = self
            .object_store
            .inner
            .get_opts(&self.root_path(version), GetOptions::default())
            .await?
            .bytes()
            .await?;
        Ok(pb::BeTreeRoot::decode(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::betree::support::make_fragment;
    use crate::format::{DeletionFile, DeletionFileType, RowDatasetVersionMeta, RowIdMeta};
    use lance_core::utils::tempfile::TempObjDir;
    use lance_io::scheduler::SchedulerConfig;

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
    async fn leaf_round_trips_fragment_metadata_and_empty_fragment() {
        let mut fragment = make_fragment(7);
        fragment.deletion_file = Some(DeletionFile {
            read_version: 3,
            id: 11,
            file_type: DeletionFileType::Bitmap,
            num_deleted_rows: Some(1),
            base_id: Some(2),
        });
        fragment.row_id_meta = Some(RowIdMeta::Inline(vec![1, 2, 3, 4]));
        fragment.created_at_version_meta =
            Some(RowDatasetVersionMeta::Inline(Arc::from([5, 6, 7])));
        fragment.last_updated_at_version_meta =
            Some(RowDatasetVersionMeta::Inline(Arc::from([8, 9, 10])));

        let mut empty_fragment = Fragment::new(8);
        empty_fragment.physical_rows = Some(12);
        empty_fragment.row_id_meta = Some(RowIdMeta::Inline(vec![12, 13]));

        let expected = vec![fragment, empty_fragment];
        let tempdir = TempObjDir::default();
        let store = test_store(tempdir.clone().join("betree"));
        let written = store.write_leaf(&expected).await.unwrap();
        let actual = store.read_leaf(&written.child_ref).await.unwrap();

        assert_eq!(actual, expected);
    }
}
