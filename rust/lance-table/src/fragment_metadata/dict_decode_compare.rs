// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Arrow reconstruct of synthetic fragment records.
//!
//! Compares dictionary-of-lists intern against copy-then-intern on in-process
//! Arrow batches. Not a Lance file decode. Dictionary-of-lists is not a
//! Lance file type. Reports Arrow batch memory, interned fragment heap, and
//! copy-then-intern temporary allocations separately. Does not measure
//! dataset-open memory.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, UInt32Type, UInt64Type};
use arrow_array::{Array, RecordBatch};
use lance_core::deepsize::DeepSizeOf;
use lance_io::utils::CachedFileSize;
use serde_json::json;

use super::leaf_layout_compare::{
    EncodeSpec, Layout, Mapping, batch_memory, data_file_from_parts, encode_flat, env_u64,
    fragment_from_header, list_i32_at, unique_field_lists, wide_fragment,
};
use crate::format::pb;
use crate::format::{DataFile, DataFileFieldInterner, Fragment};

const ROW_KIND_FRAGMENT: u8 = 0;

#[derive(Default)]
struct CopyMeter {
    copied_bytes: usize,
    copy_count: usize,
    live_bytes: usize,
    peak_live_bytes: usize,
}

impl CopyMeter {
    fn copy_row(&mut self, list: Vec<i32>) -> Vec<i32> {
        let bytes = list.len() * std::mem::size_of::<i32>();
        self.copied_bytes += bytes;
        self.copy_count += 1;
        self.live_bytes += bytes;
        self.peak_live_bytes = self.peak_live_bytes.max(self.live_bytes);
        list
    }

    fn release_row(&mut self, len: usize) {
        self.live_bytes = self
            .live_bytes
            .saturating_sub(len * std::mem::size_of::<i32>());
    }
}

fn intern_i32s(
    interner: &mut DataFileFieldInterner,
    list: Vec<i32>,
    as_fields: bool,
) -> Arc<[i32]> {
    if as_fields {
        interner
            .intern_data_file(pb::DataFile {
                path: String::new(),
                fields: list,
                column_indices: Vec::new(),
                file_major_version: 0,
                file_minor_version: 0,
                file_size_bytes: 0,
                base_id: None,
            })
            .expect("dummy data file intern")
            .fields
    } else {
        interner
            .intern_data_file(pb::DataFile {
                path: String::new(),
                fields: Vec::new(),
                column_indices: list,
                file_major_version: 0,
                file_minor_version: 0,
                file_size_bytes: 0,
                base_id: None,
            })
            .expect("dummy data file intern")
            .column_indices
    }
}

fn dictionary_column_parts(array: &dyn Array) -> serde_json::Value {
    match array.data_type() {
        arrow_schema::DataType::Dictionary(_, _) => {
            let dictionary = array.as_dictionary::<Int32Type>();
            json!({
                "kind": "dictionary",
                "rows": array.len(),
                "unique_values": dictionary.values().len(),
                "column_bytes": array.get_array_memory_size(),
                "buffer_bytes": array.get_buffer_memory_size(),
                "keys_bytes": dictionary.keys().get_array_memory_size(),
                "values_bytes": dictionary.values().get_array_memory_size(),
                "keys_plus_values_bytes": dictionary.keys().get_array_memory_size()
                    + dictionary.values().get_array_memory_size(),
            })
        }
        _ => json!({
            "kind": "list",
            "rows": array.len(),
            "column_bytes": array.get_array_memory_size(),
            "buffer_bytes": array.get_buffer_memory_size(),
        }),
    }
}

fn interned_dictionary_values(
    array: &dyn Array,
    interner: &mut DataFileFieldInterner,
    as_fields: bool,
    meter: &mut CopyMeter,
) -> Option<Vec<Arc<[i32]>>> {
    let arrow_schema::DataType::Dictionary(_, _) = array.data_type() else {
        return None;
    };
    let dictionary = array.as_dictionary::<Int32Type>();
    let values = dictionary.values();
    let mut interned = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        let list = meter.copy_row(list_i32_at(values, row));
        let len = list.len();
        interned.push(intern_i32s(interner, list, as_fields));
        meter.release_row(len);
    }
    Some(interned)
}

fn reconstruct_list_then_intern(
    batch: &RecordBatch,
    interner: &mut DataFileFieldInterner,
    meter: &mut CopyMeter,
) -> Vec<Fragment> {
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
                Some(interner),
            ));
            continue;
        }
        let fields = meter.copy_row(list_i32_at(field_ids, row));
        let field_len = fields.len();
        let cols = meter.copy_row(list_i32_at(column_indices, row));
        let col_len = cols.len();
        let file = data_file_from_parts(
            pb::DataFile {
                path: paths.value(row).to_string(),
                fields,
                column_indices: cols,
                file_major_version: major.value(row),
                file_minor_version: minor.value(row),
                file_size_bytes: sizes.value(row),
                base_id: (!base_ids.is_null(row)).then(|| base_ids.value(row)),
            },
            Some(interner),
        );
        meter.release_row(field_len);
        meter.release_row(col_len);
        fragments
            .last_mut()
            .expect("DATA_FILE follows a FRAGMENT row")
            .files
            .push(file);
    }
    fragments
}

fn reconstruct_dictionary_then_intern(
    batch: &RecordBatch,
    interner: &mut DataFileFieldInterner,
    meter: &mut CopyMeter,
) -> Vec<Fragment> {
    let field_ids = batch["field_ids"].as_ref();
    let column_indices = batch["column_indices"].as_ref();
    let interned_fields = interned_dictionary_values(field_ids, interner, true, meter)
        .expect("field_ids is a dictionary of lists");
    let interned_cols = interned_dictionary_values(column_indices, interner, false, meter)
        .expect("column_indices is a dictionary of lists");
    let field_keys = field_ids.as_dictionary::<Int32Type>().keys();
    let col_keys = column_indices.as_dictionary::<Int32Type>().keys();
    let row_kinds = batch["row_kind"].as_primitive::<arrow_array::types::UInt8Type>();
    let fragment_meta = batch["fragment_meta"].as_binary::<i32>();
    let paths = batch["path"].as_string::<i32>();
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
                Some(interner),
            ));
            continue;
        }
        let file = DataFile {
            path: paths.value(row).to_string(),
            fields: interned_fields[field_keys.value(row) as usize].clone(),
            column_indices: interned_cols[col_keys.value(row) as usize].clone(),
            file_major_version: major.value(row),
            file_minor_version: minor.value(row),
            file_size_bytes: CachedFileSize::new(sizes.value(row)),
            base_id: (!base_ids.is_null(row)).then(|| base_ids.value(row)),
        };
        fragments
            .last_mut()
            .expect("DATA_FILE follows a FRAGMENT row")
            .files
            .push(file);
    }
    fragments
}

fn mapping_vector(mapping_id: u64, columns: u32) -> Arc<[i32]> {
    let mut values = Vec::with_capacity(columns as usize);
    values.push(mapping_id as i32);
    values.extend(1..columns as i32);
    Arc::from(values)
}

fn fragments_with_mappings(
    fragment_count: u64,
    columns: u32,
    distinct_mappings: u64,
) -> Vec<Fragment> {
    let distinct_mappings = distinct_mappings.max(1);
    (0..fragment_count)
        .map(|id| {
            let mapping_id = id % distinct_mappings;
            let fields = mapping_vector(mapping_id, columns);
            wide_fragment(id, fields.clone(), fields, 0)
        })
        .collect()
}

fn split_leaves(fragments: &[Fragment], leaf_count: usize) -> Vec<Vec<Fragment>> {
    let leaf_count = leaf_count.max(1);
    let size = fragments.len().div_ceil(leaf_count);
    fragments.chunks(size).map(|chunk| chunk.to_vec()).collect()
}

fn median_ns(mut times: Vec<u128>) -> u128 {
    times.sort_unstable();
    times[times.len() / 2]
}

fn unique_field_arcs(fragments: &[Fragment]) -> usize {
    let mut pointers = std::collections::HashSet::new();
    for fragment in fragments {
        for file in &fragment.files {
            pointers.insert(Arc::as_ptr(&file.fields));
        }
    }
    pointers.len()
}

fn dict_spec() -> EncodeSpec {
    EncodeSpec {
        layout: Layout::Flat,
        mapping: Mapping::DictionaryList,
        physical_dictionary: true,
    }
}

fn list_spec() -> EncodeSpec {
    EncodeSpec {
        layout: Layout::Flat,
        mapping: Mapping::RepeatedList,
        physical_dictionary: true,
    }
}

fn measure_dictionary_reconstruct(name: &str, fragments: &[Fragment], leaf_count: usize) {
    let leaves = split_leaves(fragments, leaf_count);
    let list_batches: Vec<_> = leaves
        .iter()
        .map(|leaf| encode_flat(leaf, list_spec()).unwrap())
        .collect();
    let dict_batches: Vec<_> = leaves
        .iter()
        .map(|leaf| encode_flat(leaf, dict_spec()).unwrap())
        .collect();

    let list_arrow: usize = list_batches.iter().map(batch_memory).sum();
    let dict_arrow: usize = dict_batches.iter().map(batch_memory).sum();

    let mut list_meter = CopyMeter::default();
    let mut dict_meter = CopyMeter::default();
    let mut list_fragments = Vec::new();
    let mut dict_fragments = Vec::new();
    let mut list_times = Vec::new();
    let mut dict_times = Vec::new();
    for _ in 0..3 {
        let mut meter = CopyMeter::default();
        let mut intern = DataFileFieldInterner::default();
        let started = Instant::now();
        let mut decoded = Vec::new();
        for batch in &list_batches {
            decoded.extend(reconstruct_list_then_intern(batch, &mut intern, &mut meter));
        }
        list_times.push(started.elapsed().as_nanos());
        list_meter = meter;
        list_fragments = decoded;

        let mut meter = CopyMeter::default();
        let mut intern = DataFileFieldInterner::default();
        let started = Instant::now();
        let mut decoded = Vec::new();
        for batch in &dict_batches {
            decoded.extend(reconstruct_dictionary_then_intern(
                batch,
                &mut intern,
                &mut meter,
            ));
        }
        dict_times.push(started.elapsed().as_nanos());
        dict_meter = meter;
        dict_fragments = decoded;
    }
    assert_eq!(list_fragments, *fragments);
    assert_eq!(dict_fragments, *fragments);

    println!(
        "DICT_JSON {}",
        json!({
            "kind": "reconstruct",
            "workload": name,
            "fragments": fragments.len(),
            "files": fragments.iter().map(|fragment| fragment.files.len()).sum::<usize>(),
            "unique_field_lists": unique_field_lists(fragments),
            "leaves": leaf_count,
            "list": {
                "arrow_bytes": list_arrow,
                "copied_bytes": list_meter.copied_bytes,
                "copy_count": list_meter.copy_count,
                "peak_live_copy_bytes": list_meter.peak_live_bytes,
                "interned_fragment_bytes": list_fragments.deep_size_of(),
                "interned_unique_arcs": unique_field_arcs(&list_fragments),
                "resident_batch_plus_interned": list_arrow + list_fragments.deep_size_of(),
                "reconstruct_ns": median_ns(list_times),
            },
            "dictionary": {
                "arrow_bytes": dict_arrow,
                "copied_bytes": dict_meter.copied_bytes,
                "copy_count": dict_meter.copy_count,
                "peak_live_copy_bytes": dict_meter.peak_live_bytes,
                "interned_fragment_bytes": dict_fragments.deep_size_of(),
                "interned_unique_arcs": unique_field_arcs(&dict_fragments),
                "resident_batch_plus_interned": dict_arrow + dict_fragments.deep_size_of(),
                "reconstruct_ns": median_ns(dict_times),
            },
            "field_ids": {
                "list": dictionary_column_parts(list_batches[0].column_by_name("field_ids").unwrap()),
                "dictionary": dictionary_column_parts(dict_batches[0].column_by_name("field_ids").unwrap()),
            },
        })
    );
}

#[test]
fn dictionary_batch_memory_includes_keys_and_values_and_matches_list_records() {
    let fragments = fragments_with_mappings(12, 16, 4);
    let list_batch = encode_flat(&fragments, list_spec()).unwrap();
    let dict_batch = encode_flat(&fragments, dict_spec()).unwrap();
    let mut list_intern = DataFileFieldInterner::default();
    let mut dict_intern = DataFileFieldInterner::default();
    let mut list_meter = CopyMeter::default();
    let mut dict_meter = CopyMeter::default();
    let from_list = reconstruct_list_then_intern(&list_batch, &mut list_intern, &mut list_meter);
    let from_dict =
        reconstruct_dictionary_then_intern(&dict_batch, &mut dict_intern, &mut dict_meter);
    assert_eq!(from_list, fragments);
    assert_eq!(from_dict, fragments);

    let field_ids = dict_batch.column_by_name("field_ids").unwrap();
    let dictionary = field_ids.as_dictionary::<Int32Type>();
    let keys_bytes = dictionary.keys().get_array_memory_size();
    let values_bytes = dictionary.values().get_array_memory_size();
    let column_bytes = field_ids.get_array_memory_size();
    assert!(
        column_bytes >= keys_bytes + values_bytes,
        "dictionary column {column_bytes} must include keys {keys_bytes} and values {values_bytes}"
    );
    assert!(keys_bytes > 0, "dictionary keys must occupy memory");
    assert!(values_bytes > 0, "dictionary values must occupy memory");
}

#[ignore]
#[tokio::test]
async fn dictionary_reconstruct_vs_list_intern() {
    let fragment_count = env_u64("LEAF_F", 20000);
    let columns = env_u64("LEAF_C", 500) as u32;
    let leaf_count = env_u64("LEAF_LEAVES", 4) as usize;
    println!(
        "DICT_JSON {}",
        json!({
            "kind": "config",
            "records": "synthetic",
            "storage": "in-process",
            "fragments": fragment_count,
            "columns": columns,
            "leaves": leaf_count,
            "build": if cfg!(debug_assertions) { "debug" } else { "release" },
            "note": "Arrow reconstruct only. Dictionary-of-lists is not a Lance file type.",
        })
    );
    measure_dictionary_reconstruct(
        "homogeneous",
        &fragments_with_mappings(fragment_count, columns, 1),
        leaf_count,
    );
    measure_dictionary_reconstruct(
        "partial_32",
        &fragments_with_mappings(fragment_count, columns, 32),
        leaf_count,
    );
    measure_dictionary_reconstruct(
        "mostly_unique",
        &fragments_with_mappings(fragment_count, columns, fragment_count),
        leaf_count,
    );
}
