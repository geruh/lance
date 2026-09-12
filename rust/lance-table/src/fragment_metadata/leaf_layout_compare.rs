// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Local in-memory Lance file encode and decode of synthetic fragment records.
//!
//! Compares flat and nested leaf layouts. Nested files are not a voted leaf
//! format. Reports encoded bytes, encode-time Arrow batch memory, decoded
//! Arrow batch memory, and interned fragment heap separately.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, UInt32Type, UInt64Type};
use arrow_array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Int32Array, RecordBatch, StringArray,
    StructArray, UInt8Array, UInt32Array, UInt64Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
use bytes::Bytes;
use futures::TryStreamExt;
use lance_core::Result;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::deepsize::DeepSizeOf;
use lance_encoding::constants::DICT_DIVISOR_META_KEY;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader, FileReaderOptions};
use lance_file::version::ConcreteFileVersion;
use lance_file::versions::create_writer;
use lance_file::writer::FileWriterOptions;
use lance_io::ReadBatchParams;
use lance_io::object_reader::SmallReader;
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use object_store::path::Path;
use object_store::{PutOptions, PutPayload};
use prost::Message;
use serde_json::json;

use crate::format::pb;
use crate::format::{DataFile, DataFileFieldInterner, Fragment, RowDatasetVersionMeta};
use crate::fragment_metadata::action;
use crate::fragment_metadata::store::get_whole;
use crate::fragment_metadata::support::{data_file_path, make_backfill_data_file};

const FILE_VERSION: ConcreteFileVersion = ConcreteFileVersion::V2_1;
const ROW_KIND_FRAGMENT: u8 = 0;
const ROW_KIND_DATA_FILE: u8 = 1;
const DISABLE_PHYSICAL_DICTIONARY: &str = "1000000000";

#[derive(Clone, Copy)]
pub(super) enum Layout {
    Flat,
    Nested,
}

#[derive(Clone, Copy)]
pub(super) enum Mapping {
    RepeatedList,
    DictionaryList,
}

#[derive(Clone, Copy)]
pub(super) struct EncodeSpec {
    pub(super) layout: Layout,
    pub(super) mapping: Mapping,
    pub(super) physical_dictionary: bool,
}

impl EncodeSpec {
    fn label(self) -> String {
        format!(
            "{} {} physical_dict={}",
            match self.layout {
                Layout::Flat => "flat",
                Layout::Nested => "nested",
            },
            match self.mapping {
                Mapping::RepeatedList => "list",
                Mapping::DictionaryList => "arrow_dict",
            },
            self.physical_dictionary
        )
    }
}

pub(super) fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn int_list_type() -> DataType {
    DataType::List(Arc::new(ArrowField::new("item", DataType::Int32, false)))
}

fn mapped_list_type(mapping: Mapping) -> DataType {
    match mapping {
        Mapping::RepeatedList => int_list_type(),
        Mapping::DictionaryList => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(int_list_type()))
        }
    }
}

fn stamp_physical_dictionary(field: ArrowField, physical_dictionary: bool) -> ArrowField {
    if physical_dictionary {
        return field;
    }
    let mut metadata = field.metadata().clone();
    metadata.insert(
        DICT_DIVISOR_META_KEY.to_string(),
        DISABLE_PHYSICAL_DICTIONARY.to_string(),
    );
    let data_type = match field.data_type() {
        DataType::List(item) => {
            DataType::List(Arc::new(stamp_physical_dictionary((**item).clone(), false)))
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|child| stamp_physical_dictionary((**child).clone(), false))
                .collect(),
        ),
        DataType::Dictionary(key, value) => {
            let value_field = stamp_physical_dictionary(
                ArrowField::new("value", value.as_ref().clone(), true),
                false,
            );
            DataType::Dictionary(key.clone(), Box::new(value_field.data_type().clone()))
        }
        other => other.clone(),
    };
    ArrowField::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata)
}

fn list_item_field(physical_dictionary: bool) -> ArrowField {
    stamp_physical_dictionary(
        ArrowField::new("item", DataType::Int32, false),
        physical_dictionary,
    )
}

fn list_column(mapping: Mapping, lists: &[Vec<i32>], physical_dictionary: bool) -> ArrayRef {
    let item = list_item_field(physical_dictionary);
    match mapping {
        Mapping::RepeatedList => {
            let mut builder = ListBuilder::new(Int32Builder::new()).with_field(item);
            for list in lists {
                builder.values().append_slice(list);
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        Mapping::DictionaryList => {
            let mut unique: HashMap<Vec<i32>, i32> = HashMap::new();
            let mut keys = Vec::with_capacity(lists.len());
            let mut dictionary = Vec::new();
            for list in lists {
                if let Some(&key) = unique.get(list) {
                    keys.push(key);
                    continue;
                }
                let key = i32::try_from(dictionary.len()).expect("dictionary key fits i32");
                unique.insert(list.clone(), key);
                dictionary.push(list.clone());
                keys.push(key);
            }
            let mut values = ListBuilder::new(Int32Builder::new()).with_field(item);
            for list in &dictionary {
                values.values().append_slice(list);
                values.append(true);
            }
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(keys),
                    Arc::new(values.finish()),
                )
                .expect("dictionary keys index values"),
            )
        }
    }
}

pub(super) fn list_i32_at(array: &dyn Array, row: usize) -> Vec<i32> {
    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_dictionary::<Int32Type>();
            list_i32_at(
                dictionary.values().as_ref(),
                dictionary.keys().value(row) as usize,
            )
        }
        _ => array
            .as_list::<i32>()
            .value(row)
            .as_primitive::<Int32Type>()
            .values()
            .to_vec(),
    }
}

fn header_proto(fragment: &Fragment) -> Vec<u8> {
    let mut header = pb::DataFragment::from(fragment);
    header.files.clear();
    header.encode_to_vec()
}

pub(super) fn data_file_from_parts(
    proto: pb::DataFile,
    intern: Option<&mut DataFileFieldInterner>,
) -> DataFile {
    match intern {
        Some(interner) => interner
            .intern_data_file(proto)
            .expect("data file proto is valid"),
        None => DataFile::try_from(proto).expect("data file proto is valid"),
    }
}

pub(super) fn fragment_from_header(
    bytes: &[u8],
    files: Vec<DataFile>,
    intern: Option<&mut DataFileFieldInterner>,
) -> Fragment {
    let proto = pb::DataFragment::decode(bytes).expect("fragment_meta is a DataFragment");
    let mut fragment = match intern {
        Some(interner) => interner
            .intern_fragment(proto)
            .expect("fragment_meta is a DataFragment"),
        None => crate::format::Fragment::try_from(proto).expect("fragment_meta is a DataFragment"),
    };
    fragment.files = files;
    fragment
}

fn file_column_lists(fragments: &[Fragment]) -> (Vec<Vec<i32>>, Vec<Vec<i32>>) {
    let mut field_ids = Vec::new();
    let mut column_indices = Vec::new();
    for fragment in fragments {
        field_ids.push(Vec::new());
        column_indices.push(Vec::new());
        for file in &fragment.files {
            field_ids.push(file.fields.to_vec());
            column_indices.push(file.column_indices.to_vec());
        }
    }
    (field_ids, column_indices)
}

pub(super) fn encode_flat(fragments: &[Fragment], spec: EncodeSpec) -> Result<RecordBatch> {
    let num_rows: usize = fragments
        .iter()
        .map(|fragment| fragment.files.len() + 1)
        .sum();
    let mut row_kind = Vec::with_capacity(num_rows);
    let mut frag_ids = Vec::with_capacity(num_rows);
    let mut fragment_meta = Vec::with_capacity(num_rows);
    let mut paths = Vec::with_capacity(num_rows);
    let mut major = Vec::with_capacity(num_rows);
    let mut minor = Vec::with_capacity(num_rows);
    let mut sizes = Vec::with_capacity(num_rows);
    let mut base_ids: Vec<Option<u32>> = Vec::with_capacity(num_rows);
    for fragment in fragments {
        row_kind.push(ROW_KIND_FRAGMENT);
        frag_ids.push(fragment.id);
        fragment_meta.push(Some(header_proto(fragment)));
        paths.push(String::new());
        major.push(0);
        minor.push(0);
        sizes.push(0);
        base_ids.push(None);
        for file in &fragment.files {
            row_kind.push(ROW_KIND_DATA_FILE);
            frag_ids.push(fragment.id);
            fragment_meta.push(None);
            paths.push(file.path.clone());
            major.push(file.file_major_version);
            minor.push(file.file_minor_version);
            sizes.push(
                file.file_size_bytes
                    .get()
                    .map(|size| size.get())
                    .unwrap_or_default(),
            );
            base_ids.push(file.base_id);
        }
    }
    let (field_ids, column_indices) = file_column_lists(fragments);
    let schema = ArrowSchema::new(vec![
        stamp_physical_dictionary(
            ArrowField::new("row_kind", DataType::UInt8, false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("frag_id", DataType::UInt64, false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("fragment_meta", DataType::Binary, true),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("path", DataType::Utf8, false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("field_ids", mapped_list_type(spec.mapping), false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("column_indices", mapped_list_type(spec.mapping), false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("major_version", DataType::UInt32, false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("minor_version", DataType::UInt32, false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("file_size_bytes", DataType::UInt64, false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("base_id", DataType::UInt32, true),
            spec.physical_dictionary,
        ),
    ]);
    Ok(RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt8Array::from(row_kind)),
            Arc::new(UInt64Array::from(frag_ids)),
            Arc::new(BinaryArray::from_opt_vec(
                fragment_meta
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(paths)),
            list_column(spec.mapping, &field_ids, spec.physical_dictionary),
            list_column(spec.mapping, &column_indices, spec.physical_dictionary),
            Arc::new(UInt32Array::from(major)),
            Arc::new(UInt32Array::from(minor)),
            Arc::new(UInt64Array::from(sizes)),
            Arc::new(UInt32Array::from(base_ids)),
        ],
    )?)
}

fn file_struct_fields(mapping: Mapping, physical_dictionary: bool) -> Fields {
    Fields::from(vec![
        stamp_physical_dictionary(
            ArrowField::new("path", DataType::Utf8, false),
            physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("field_ids", mapped_list_type(mapping), false),
            physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("column_indices", mapped_list_type(mapping), false),
            physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("major_version", DataType::UInt32, false),
            physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("minor_version", DataType::UInt32, false),
            physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("file_size_bytes", DataType::UInt64, false),
            physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("base_id", DataType::UInt32, true),
            physical_dictionary,
        ),
    ])
}

fn encode_nested(fragments: &[Fragment], spec: EncodeSpec) -> Result<RecordBatch> {
    let mut ids = Vec::with_capacity(fragments.len());
    let mut fragment_meta = Vec::with_capacity(fragments.len());
    let mut offsets = Vec::with_capacity(fragments.len() + 1);
    offsets.push(0i32);
    let mut paths = Vec::new();
    let mut field_ids = Vec::new();
    let mut column_indices = Vec::new();
    let mut major = Vec::new();
    let mut minor = Vec::new();
    let mut sizes = Vec::new();
    let mut base_ids: Vec<Option<u32>> = Vec::new();
    let mut file_count = 0i32;
    for fragment in fragments {
        ids.push(fragment.id);
        fragment_meta.push(header_proto(fragment));
        for file in &fragment.files {
            paths.push(file.path.clone());
            field_ids.push(file.fields.to_vec());
            column_indices.push(file.column_indices.to_vec());
            major.push(file.file_major_version);
            minor.push(file.file_minor_version);
            sizes.push(
                file.file_size_bytes
                    .get()
                    .map(|size| size.get())
                    .unwrap_or_default(),
            );
            base_ids.push(file.base_id);
            file_count += 1;
        }
        offsets.push(file_count);
    }
    let struct_fields = file_struct_fields(spec.mapping, spec.physical_dictionary);
    let files = StructArray::new(
        struct_fields.clone(),
        vec![
            Arc::new(StringArray::from(paths)),
            list_column(spec.mapping, &field_ids, spec.physical_dictionary),
            list_column(spec.mapping, &column_indices, spec.physical_dictionary),
            Arc::new(UInt32Array::from(major)),
            Arc::new(UInt32Array::from(minor)),
            Arc::new(UInt64Array::from(sizes)),
            Arc::new(UInt32Array::from(base_ids)),
        ],
        None,
    );
    let files_list = arrow_array::ListArray::try_new(
        Arc::new(ArrowField::new(
            "item",
            DataType::Struct(struct_fields),
            false,
        )),
        OffsetBuffer::from_lengths(
            offsets
                .windows(2)
                .map(|window| (window[1] - window[0]) as usize),
        ),
        Arc::new(files),
        None,
    )?;
    let schema = ArrowSchema::new(vec![
        stamp_physical_dictionary(
            ArrowField::new("id", DataType::UInt64, false),
            spec.physical_dictionary,
        ),
        stamp_physical_dictionary(
            ArrowField::new("fragment_meta", DataType::Binary, false),
            spec.physical_dictionary,
        ),
        ArrowField::new("files", files_list.data_type().clone(), false),
    ]);
    Ok(RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(BinaryArray::from_iter_values(fragment_meta)),
            Arc::new(files_list),
        ],
    )?)
}

pub(super) fn batch_memory(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| column.get_array_memory_size())
        .sum()
}

async fn write_lance(batch: &RecordBatch) -> Result<Bytes> {
    let lance_schema = LanceSchema::try_from(batch.schema().as_ref())?;
    let memory = ObjectStore::memory();
    let path = Path::from("encoded-leaf");
    let writer = memory.create(&path).await?;
    let mut file_writer = create_writer(
        FILE_VERSION,
        writer,
        lance_schema,
        FileWriterOptions::default(),
    )?;
    file_writer.write_batch(batch).await?;
    file_writer.finish().await?;
    get_whole(&memory, &path).await
}

async fn read_lance(bytes: Bytes, columns: Option<&[&str]>) -> Result<RecordBatch> {
    let store = ObjectStore::memory();
    let path = Path::from("encoded-leaf");
    let size = bytes.len();
    store
        .inner
        .put_opts(&path, PutPayload::from(bytes), PutOptions::default())
        .await?;
    let store = Arc::new(store);
    let scheduler = ScanScheduler::new(store.clone(), SchedulerConfig::max_bandwidth(&store));
    let object_reader = Arc::new(SmallReader::new(store.inner.clone(), path, 3, size));
    let file_scheduler = scheduler.open_reader(object_reader);
    let cache = LanceCache::with_capacity(64 * 1024 * 1024);
    let reader = FileReader::try_open(
        file_scheduler,
        None,
        Arc::<DecoderPlugins>::default(),
        &cache,
        FileReaderOptions::default(),
    )
    .await?;
    let mut stream = match columns {
        Some(names) => {
            let projection = lance_file::versions::reader_projection_from_column_names(
                FILE_VERSION,
                reader.schema(),
                names,
            )?;
            reader
                .read_stream_projected(
                    ReadBatchParams::RangeFull,
                    16 * 1024,
                    16,
                    projection,
                    FilterExpression::no_filter(),
                )
                .await?
        }
        None => {
            reader
                .read_stream(
                    ReadBatchParams::RangeFull,
                    16 * 1024,
                    16,
                    FilterExpression::no_filter(),
                )
                .await?
        }
    };
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        batches.push(batch);
    }
    Ok(arrow::compute::concat_batches(
        &batches[0].schema(),
        &batches,
    )?)
}

fn decode_flat(batch: &RecordBatch, intern: bool) -> Vec<Fragment> {
    let mut interner = intern.then(DataFileFieldInterner::default);
    let row_kinds = batch["row_kind"].as_primitive::<arrow_array::types::UInt8Type>();
    let fragment_meta = batch["fragment_meta"].as_binary::<i32>();
    let paths = batch["path"].as_string::<i32>();
    let field_ids = batch["field_ids"].as_ref();
    let column_indices = batch["column_indices"].as_ref();
    let major = batch["major_version"].as_primitive::<UInt32Type>();
    let minor = batch["minor_version"].as_primitive::<UInt32Type>();
    let sizes = batch["file_size_bytes"].as_primitive::<UInt64Type>();
    let base_ids = batch["base_id"].as_primitive::<UInt32Type>();
    let mut fragments = Vec::new();
    for row in 0..batch.num_rows() {
        if row_kinds.value(row) == ROW_KIND_FRAGMENT {
            fragments.push(fragment_from_header(
                fragment_meta.value(row),
                Vec::new(),
                interner.as_mut(),
            ));
            continue;
        }
        let file = data_file_from_parts(
            pb::DataFile {
                path: paths.value(row).to_string(),
                fields: list_i32_at(field_ids, row),
                column_indices: list_i32_at(column_indices, row),
                file_major_version: major.value(row),
                file_minor_version: minor.value(row),
                file_size_bytes: sizes.value(row),
                base_id: (!base_ids.is_null(row)).then(|| base_ids.value(row)),
            },
            interner.as_mut(),
        );
        fragments
            .last_mut()
            .expect("DATA_FILE follows a FRAGMENT row")
            .files
            .push(file);
    }
    fragments
}

fn decode_nested(batch: &RecordBatch, intern: bool) -> Vec<Fragment> {
    let mut interner = intern.then(DataFileFieldInterner::default);
    let ids = batch["id"].as_primitive::<UInt64Type>();
    let fragment_meta = batch["fragment_meta"].as_binary::<i32>();
    let files = batch["files"].as_list::<i32>();
    let mut fragments = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let struct_array = files.value(row);
        let files_struct = struct_array.as_struct();
        let paths = files_struct
            .column_by_name("path")
            .unwrap()
            .as_string::<i32>();
        let field_ids = files_struct.column_by_name("field_ids").unwrap();
        let column_indices = files_struct.column_by_name("column_indices").unwrap();
        let major = files_struct
            .column_by_name("major_version")
            .unwrap()
            .as_primitive::<UInt32Type>();
        let minor = files_struct
            .column_by_name("minor_version")
            .unwrap()
            .as_primitive::<UInt32Type>();
        let sizes = files_struct
            .column_by_name("file_size_bytes")
            .unwrap()
            .as_primitive::<UInt64Type>();
        let base_ids = files_struct
            .column_by_name("base_id")
            .unwrap()
            .as_primitive::<UInt32Type>();
        let mut decoded_files = Vec::with_capacity(files_struct.len());
        for file_row in 0..files_struct.len() {
            decoded_files.push(data_file_from_parts(
                pb::DataFile {
                    path: paths.value(file_row).to_string(),
                    fields: list_i32_at(field_ids, file_row),
                    column_indices: list_i32_at(column_indices, file_row),
                    file_major_version: major.value(file_row),
                    file_minor_version: minor.value(file_row),
                    file_size_bytes: sizes.value(file_row),
                    base_id: (!base_ids.is_null(file_row)).then(|| base_ids.value(file_row)),
                },
                interner.as_mut(),
            ));
        }
        let mut fragment =
            fragment_from_header(fragment_meta.value(row), decoded_files, interner.as_mut());
        fragment.id = ids.value(row);
        fragments.push(fragment);
    }
    fragments
}

pub(super) fn wide_fragment(
    id: u64,
    fields: Arc<[i32]>,
    indices: Arc<[i32]>,
    adds: u64,
) -> Fragment {
    let mut fragment = Fragment::new(id);
    fragment.physical_rows = Some(2048);
    fragment.files.push(DataFile {
        path: data_file_path(id, 0),
        fields,
        column_indices: indices,
        file_major_version: 2,
        file_minor_version: 0,
        file_size_bytes: std::num::NonZero::new(4096).into(),
        base_id: None,
    });
    for add in 0..adds {
        fragment.files.push(make_backfill_data_file(id, add as u32));
    }
    fragment
}

fn wide_fragments(fragment_count: u64, columns: u32, adds: u64) -> Vec<Fragment> {
    let fields: Arc<[i32]> = (0..columns as i32).collect::<Vec<_>>().into();
    let indices = fields.clone();
    (0..fragment_count)
        .map(|id| wide_fragment(id, fields.clone(), indices.clone(), adds))
        .collect()
}

fn with_timestamp(fragments: &[Fragment]) -> Vec<Fragment> {
    fragments
        .iter()
        .cloned()
        .map(|mut fragment| {
            fragment.last_updated_at_version_meta =
                Some(RowDatasetVersionMeta::Inline(Arc::from([
                    1u8, 2, 3, 4, 5, 6, 7, 8,
                ])));
            fragment
        })
        .collect()
}

fn one_add_columns_buffer_bytes(fragment_count: u64) -> usize {
    (0..fragment_count)
        .map(|id| {
            let file = make_backfill_data_file(id, 0);
            pb::FragmentMetadataMutation {
                action_sequence: id + 1,
                action: Some(action::add_data_file(id, &file)),
                fragment_count_delta: 0,
                total_rows_delta: 0,
                visible_rows_delta: 0,
            }
            .encoded_len()
        })
        .sum()
}

fn build_batch(fragments: &[Fragment], spec: EncodeSpec) -> Result<RecordBatch> {
    match spec.layout {
        Layout::Flat => encode_flat(fragments, spec),
        Layout::Nested => encode_nested(fragments, spec),
    }
}

async fn encode_bytes(fragments: &[Fragment], spec: EncodeSpec) -> Result<(Bytes, u128, usize)> {
    let started = Instant::now();
    let batch = build_batch(fragments, spec)?;
    let arrow_bytes = batch_memory(&batch);
    let bytes = write_lance(&batch).await?;
    Ok((bytes, started.elapsed().as_nanos(), arrow_bytes))
}

async fn reconstruct(
    bytes: Bytes,
    spec: EncodeSpec,
    intern: bool,
) -> Result<(Vec<Fragment>, u128, usize)> {
    let started = Instant::now();
    let batch = read_lance(bytes, None).await?;
    let fragments = match spec.layout {
        Layout::Flat => decode_flat(&batch, intern),
        Layout::Nested => decode_nested(&batch, intern),
    };
    Ok((
        fragments,
        started.elapsed().as_nanos(),
        batch_memory(&batch),
    ))
}

async fn project(bytes: Bytes, spec: EncodeSpec) -> Result<(u128, usize, usize)> {
    let columns: &[&str] = match spec.layout {
        Layout::Flat => &["frag_id", "field_ids"],
        Layout::Nested => &["files"],
    };
    let started = Instant::now();
    let batch = read_lance(bytes, Some(columns)).await?;
    Ok((
        started.elapsed().as_nanos(),
        batch_memory(&batch),
        batch.num_rows(),
    ))
}

fn specs() -> Vec<EncodeSpec> {
    let mut specs = Vec::new();
    for layout in [Layout::Flat, Layout::Nested] {
        for mapping in [Mapping::RepeatedList, Mapping::DictionaryList] {
            specs.push(EncodeSpec {
                layout,
                mapping,
                physical_dictionary: true,
            });
            if matches!(mapping, Mapping::RepeatedList) {
                specs.push(EncodeSpec {
                    layout,
                    mapping,
                    physical_dictionary: false,
                });
            }
        }
    }
    specs
}

fn writable_specs() -> Vec<EncodeSpec> {
    specs()
        .into_iter()
        .filter(|spec| matches!(spec.mapping, Mapping::RepeatedList))
        .collect()
}

pub(super) fn unique_field_lists(fragments: &[Fragment]) -> usize {
    let mut unique = std::collections::HashSet::new();
    for fragment in fragments {
        for file in &fragment.files {
            unique.insert(file.fields.to_vec());
        }
    }
    unique.len()
}

async fn measure_workload(name: &str, fragments: &[Fragment]) -> Result<()> {
    let buffer_bytes = one_add_columns_buffer_bytes(fragments.len() as u64);
    println!(
        "LEAF_JSON {}",
        json!({
            "kind": "workload",
            "name": name,
            "fragments": fragments.len(),
            "files": fragments.iter().map(|fragment| fragment.files.len()).sum::<usize>(),
            "unique_field_lists": unique_field_lists(fragments),
            "buffer_add_data_file_bytes": buffer_bytes,
        })
    );
    for spec in specs() {
        let built = Instant::now();
        let batch = build_batch(fragments, spec)?;
        let arrow_bytes = batch_memory(&batch);
        let build_ns = built.elapsed().as_nanos();
        if matches!(spec.mapping, Mapping::DictionaryList) {
            println!(
                "LEAF_JSON {}",
                json!({
                    "kind": "arm",
                    "workload": name,
                    "layout": match spec.layout { Layout::Flat => "flat", Layout::Nested => "nested" },
                    "mapping": "arrow_dict",
                    "physical_dictionary": spec.physical_dictionary,
                    "skipped": "lance_schema_rejects_dictionary_of_lists",
                    "encode_arrow_bytes": arrow_bytes,
                    "encode_ns": build_ns,
                })
            );
            continue;
        }
        let started = Instant::now();
        let bytes = write_lance(&batch).await?;
        let encode_ns = started.elapsed().as_nanos() + build_ns;
        let encoded_bytes = bytes.len();
        let (project_ns, projected_arrow_bytes, projected_rows) =
            project(bytes.clone(), spec).await?;
        for intern in [false, true] {
            let (decoded, decode_ns, decoded_arrow_bytes) =
                reconstruct(bytes.clone(), spec, intern).await?;
            assert_eq!(decoded, fragments, "{}", spec.label());
            println!(
                "LEAF_JSON {}",
                json!({
                    "kind": "arm",
                    "workload": name,
                    "layout": match spec.layout { Layout::Flat => "flat", Layout::Nested => "nested" },
                    "mapping": "list",
                    "physical_dictionary": spec.physical_dictionary,
                    "intern": intern,
                    "encoded_bytes": encoded_bytes,
                    "encode_arrow_bytes": arrow_bytes,
                    "encode_ns": encode_ns,
                    "decode_ns": decode_ns,
                    "decode_arrow_bytes": decoded_arrow_bytes,
                    "decoded_fragment_bytes": decoded.deep_size_of(),
                    "project_ns": project_ns,
                    "projected_arrow_bytes": projected_arrow_bytes,
                    "projected_rows": projected_rows,
                })
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn flat_and_nested_leaves_round_trip_identical_fragments() {
    let fragments = wide_fragments(4, 8, 2);
    for spec in writable_specs() {
        let (bytes, _, _) = encode_bytes(&fragments, spec).await.unwrap();
        let (decoded, _, _) = reconstruct(bytes, spec, true).await.unwrap();
        assert_eq!(decoded, fragments, "{}", spec.label());
    }
}

#[test]
fn dictionary_of_int_lists_is_not_a_lance_schema() {
    let fragments = wide_fragments(1, 4, 0);
    let spec = EncodeSpec {
        layout: Layout::Flat,
        mapping: Mapping::DictionaryList,
        physical_dictionary: true,
    };
    let batch = encode_flat(&fragments, spec).unwrap();
    let converted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        LanceSchema::try_from(batch.schema().as_ref())
    }));
    if let Ok(Ok(_)) = converted {
        panic!("dictionary of lists must not convert to a Lance schema");
    }
}

#[ignore]
#[tokio::test]
async fn leaf_layouts_compare_encoded_size_and_decode() {
    let fragment_count = env_u64("LEAF_F", 2000);
    let columns = env_u64("LEAF_C", 100) as u32;
    let adds = env_u64("LEAF_ADDS", 5);
    let base = wide_fragments(fragment_count, columns, 0);
    let after_adds = wide_fragments(fragment_count, columns, adds);
    let timestamp_bump = with_timestamp(&after_adds);
    println!(
        "LEAF_JSON {}",
        json!({
            "kind": "config",
            "records": "synthetic",
            "storage": "in-memory",
            "fragments": fragment_count,
            "columns": columns,
            "adds": adds,
            "build": if cfg!(debug_assertions) { "debug" } else { "release" },
        })
    );
    measure_workload("homogeneous_base", &base).await.unwrap();
    measure_workload("after_adds", &after_adds).await.unwrap();
    measure_workload("timestamp_bump", &timestamp_bump)
        .await
        .unwrap();
}
