// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Local-disk measurement of wide-table metadata growth.
//!
//! Merge arms commit `Operation::Merge` with an updated schema, the path
//! add_columns uses. The replace arm commits `Operation::DataReplacement` with
//! file replacements only. It does not publish schema. That arm is a
//! metadata-only experiment, not add_columns. Routing add_columns through
//! DataReplacement remains an investigation, including schema publication and
//! validation.
//!
//! Synthetic fragment records, no 2048-row data files. Reports local I/O and
//! on-disk metadata sizes, not S3 and not dataset-open heap of a real table.
//! Ignored: run with `--ignored --nocapture`.
//!
//! Env: `WIDE_F` fragments (default 2000), `WIDE_C` columns (50),
//! `WIDE_ADDS` rounds (5). `WIDE_F=20000 WIDE_C=500 WIDE_ADDS=5` is the RFC
//! shape.

// The measurements are the deliverable of this ignored harness, read from
// `--nocapture` output rather than asserted.
#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::num::NonZero;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance_core::datatypes::Schema;
use lance_file::version::ConcreteFileVersion;
use lance_io::object_store::ObjectStoreParams;
use lance_io::utils::tracking_store::{IOTracker, IoStats};
use lance_table::format::{DataFile, Fragment, pb};
use lance_table::fragment_metadata::support::data_file_path;
use lance_table::fragment_metadata::{MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TREE};
use prost::Message;

use super::{MAX_LEAF_BYTES_KEY, MAX_NODE_BYTES_KEY};
use crate::dataset::Dataset;
use crate::dataset::builder::DatasetBuilder;
use crate::dataset::transaction::{DataReplacementGroup, Operation, Transaction};
use crate::dataset::write::CommitBuilder;
use crate::session::Session;

const FILE_VERSION: ConcreteFileVersion = ConcreteFileVersion::V2_0;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn wide_schema(cols: u32) -> Schema {
    Schema::try_from(&ArrowSchema::new(
        (0..cols)
            .map(|i| ArrowField::new(format!("c{i}"), DataType::Int32, true))
            .collect::<Vec<_>>(),
    ))
    .unwrap()
}

fn wide_fragment(id: u64, fields: Arc<[i32]>, indices: Arc<[i32]>) -> Fragment {
    let mut fragment = Fragment::new(id);
    fragment.physical_rows = Some(2048);
    fragment.files.push(DataFile {
        path: data_file_path(id, 0),
        fields,
        column_indices: indices,
        file_major_version: 2,
        file_minor_version: 0,
        file_size_bytes: NonZero::new(4096).into(),
        base_id: None,
    });
    fragment
}

fn backfill_file(frag_id: u64, field_id: i32) -> DataFile {
    DataFile::new(
        data_file_path(frag_id, field_id as u64),
        vec![field_id],
        vec![0],
        FILE_VERSION,
        NonZero::new(4096),
        None,
    )
}

fn tree_config() -> HashMap<String, String> {
    let node_bytes = env_u64("WIDE_NODE_BYTES", 1024 * 1024);
    HashMap::from([
        (
            MANIFEST_LAYOUT_KEY.to_string(),
            MANIFEST_LAYOUT_TREE.to_string(),
        ),
        (MAX_NODE_BYTES_KEY.to_string(), node_bytes.to_string()),
        (MAX_LEAF_BYTES_KEY.to_string(), (1024 * 1024).to_string()),
        (
            "lance.fragment_metadata.semantic_buffer_bytes".to_string(),
            node_bytes.to_string(),
        ),
        (
            "lance.fragment_metadata.hard_capacity_bytes".to_string(),
            (256 * 1024 * 1024).to_string(),
        ),
        (
            "lance.fragment_metadata.allow_deep_writer".to_string(),
            "true".to_string(),
        ),
        (
            "lance.fragment_metadata.materialization".to_string(),
            "buffered".to_string(),
        ),
    ])
}

fn dir_bytes(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

struct Disk {
    versions: u64,
    transactions: u64,
    tree: u64,
}

impl Disk {
    fn of(root: &Path) -> Self {
        Self {
            versions: dir_bytes(&root.join("_versions")),
            transactions: dir_bytes(&root.join("_transactions")),
            tree: dir_bytes(&root.join("_bt")),
        }
    }

    fn metadata(&self) -> u64 {
        self.versions + self.transactions + self.tree
    }

    fn fmt(&self) -> String {
        format!(
            "versions={:.2} MiB  txn={:.2} MiB  _bt={:.2} MiB  total={:.2} MiB",
            mib(self.versions),
            mib(self.transactions),
            mib(self.tree),
            mib(self.metadata())
        )
    }
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn fmt_io(stats: &IoStats) -> String {
    format!(
        "r={}/{}  w={}/{}  ({:.2} MiB read, {:.2} MiB write)",
        stats.read_iops,
        stats.read_bytes,
        stats.write_iops,
        stats.written_bytes,
        mib(stats.read_bytes),
        mib(stats.written_bytes)
    )
}

fn latest_manifest_bytes(root: &Path) -> u64 {
    let dir = root.join("_versions");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return 0;
    };
    let mut best: Option<(u64, u64)> = None;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(stem) = name.strip_suffix(".manifest") else {
            continue;
        };
        let Ok(version) = stem.parse::<u64>() else {
            continue;
        };
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if best.is_none_or(|(v, _)| version >= v) {
            best = Some((version, meta.len()));
        }
    }
    best.map(|(_, bytes)| bytes).unwrap_or(0)
}

fn leaf_gets(stats: &IoStats) -> u64 {
    stats
        .requests
        .iter()
        .filter(|request| {
            request.path.as_ref().contains("_bt/leaf/") && request.method.starts_with("get_")
        })
        .count() as u64
}

async fn create_dataset(
    uri: &str,
    fragments: Vec<Fragment>,
    schema: Schema,
    tree: bool,
) -> Dataset {
    let config = if tree { Some(tree_config()) } else { None };
    CommitBuilder::new(uri)
        .with_skip_auto_cleanup(true)
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments,
                schema,
                config_upsert_values: config,
                initial_bases: None,
            },
        ))
        .await
        .unwrap()
}

async fn open_tracked(uri: &str, tracker: &IOTracker) -> Dataset {
    let params = ObjectStoreParams {
        object_store_wrapper: Some(Arc::new(tracker.clone())),
        ..Default::default()
    };
    DatasetBuilder::from_uri(uri)
        .with_store_params(params)
        .with_session(Arc::new(Session::default()))
        .load()
        .await
        .unwrap()
}

/// `into_dataset` hydrates; keep the lazy handle for checkout that must not.
async fn open_lazy_tracked(uri: &str, tracker: &IOTracker) -> crate::dataset::LazyDataset {
    let params = ObjectStoreParams {
        object_store_wrapper: Some(Arc::new(tracker.clone())),
        ..Default::default()
    };
    DatasetBuilder::from_uri(uri)
        .with_store_params(params)
        .with_session(Arc::new(Session::default()))
        .load_lazy()
        .await
        .unwrap()
}

/// Updated schema plus one backfill file per fragment. This is the add_columns path.
fn merge_add(fragments: &[Fragment], schema: Schema, field_id: i32) -> Operation {
    Operation::Merge {
        preserves_nullability: false,
        schema,
        fragments: fragments
            .iter()
            .map(|fragment| {
                let mut fragment = fragment.clone();
                fragment.files.push(backfill_file(fragment.id, field_id));
                fragment
            })
            .collect(),
    }
}

/// File replacements only. No schema. Not add_columns.
fn replacement_add(n: u64, field_id: i32) -> Operation {
    Operation::DataReplacement {
        replacements: (0..n)
            .map(|id| DataReplacementGroup(id, backfill_file(id, field_id)))
            .collect(),
    }
}

struct LayoutRun {
    name: &'static str,
    uri: String,
    root: std::path::PathBuf,
    dataset: Dataset,
}

async fn run_adds(
    run: &mut LayoutRun,
    mut merge_fragments: Option<Vec<Fragment>>,
    n: u64,
    start_cols: u32,
    adds: u32,
) {
    let use_merge = merge_fragments.is_some();
    println!(
        "\n== {}  merge={}  F={n} start_C={start_cols} adds={adds} ==",
        run.name, use_merge
    );
    let bootstrap = Disk::of(&run.root);
    println!("  after create: {}", bootstrap.fmt());

    let reader_tracker = IOTracker::default();
    let mut eager = open_tracked(&run.uri, &reader_tracker).await;
    let eager_open = reader_tracker.incremental_stats();
    println!("  eager open:   {}", fmt_io(&eager_open));
    println!("  eager open leaf GETs: {}", leaf_gets(&eager_open));

    let lazy_tracker = IOTracker::default();
    let mut lazy = open_lazy_tracked(&run.uri, &lazy_tracker).await;
    let lazy_open = lazy_tracker.incremental_stats();
    println!("  lazy open:    {}", fmt_io(&lazy_open));
    println!("  lazy open leaf GETs:  {}", leaf_gets(&lazy_open));

    let mut prev_disk = bootstrap;

    for round in 0..adds {
        let field_id = start_cols as i32 + round as i32;
        let schema = wide_schema(start_cols + round + 1);
        let op = if let Some(fragments) = merge_fragments.as_ref() {
            merge_add(fragments, schema, field_id)
        } else {
            replacement_add(n, field_id)
        };
        let t0 = Instant::now();
        run.dataset = CommitBuilder::new(Arc::new(run.dataset.clone()))
            .with_skip_auto_cleanup(true)
            .execute(Transaction::new_from_version(
                run.dataset.manifest.version,
                op,
            ))
            .await
            .unwrap();
        let commit_ms = t0.elapsed().as_secs_f64() * 1000.0;
        if let Some(fragments) = merge_fragments.as_mut() {
            for fragment in fragments.iter_mut() {
                fragment.files.push(backfill_file(fragment.id, field_id));
            }
        }
        let disk = Disk::of(&run.root);
        let wrote = disk.metadata().saturating_sub(prev_disk.metadata());
        let latest = latest_manifest_bytes(&run.root);
        println!(
            "  add {round}: {commit_ms:.0} ms  wrote {:.2} MiB  latest_manifest={:.2} MiB  ({})",
            mib(wrote),
            mib(latest),
            disk.fmt()
        );

        let t1 = Instant::now();
        eager.checkout_latest().await.unwrap();
        let eager_refresh = reader_tracker.incremental_stats();
        let eager_ms = t1.elapsed().as_secs_f64() * 1000.0;
        println!(
            "    eager checkout_latest: {eager_ms:.0} ms  {}  leaf GETs={}",
            fmt_io(&eager_refresh),
            leaf_gets(&eager_refresh)
        );

        let t2 = Instant::now();
        lazy = lazy
            .checkout_version(run.dataset.manifest.version)
            .await
            .unwrap();
        let lazy_refresh = lazy_tracker.incremental_stats();
        let lazy_ms = t2.elapsed().as_secs_f64() * 1000.0;
        println!(
            "    lazy  checkout:         {lazy_ms:.0} ms  {}  leaf GETs={}",
            fmt_io(&lazy_refresh),
            leaf_gets(&lazy_refresh)
        );
        println!(
            "WIDE_JSON {{\"arm\":\"{}\",\"round\":{round},\"commit_ms\":{commit_ms:.1},\"wrote_bytes\":{wrote},\"latest_manifest_bytes\":{latest},\"eager_ms\":{eager_ms:.1},\"eager_read_bytes\":{},\"eager_leaf_gets\":{},\"lazy_ms\":{lazy_ms:.1},\"lazy_read_bytes\":{},\"lazy_leaf_gets\":{},\"versions_bytes\":{},\"txn_bytes\":{},\"bt_bytes\":{}}}",
            run.name,
            eager_refresh.read_bytes,
            leaf_gets(&eager_refresh),
            lazy_refresh.read_bytes,
            leaf_gets(&lazy_refresh),
            disk.versions,
            disk.transactions,
            disk.tree
        );

        prev_disk = disk;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn wide_table_add_columns_io() {
    let n = env_u64("WIDE_F", 2_000);
    let cols = env_u64("WIDE_C", 50) as u32;
    let adds = env_u64("WIDE_ADDS", 5) as u32;
    assert!(n > 0 && cols > 0 && adds > 0);

    let field_ids: Arc<[i32]> = (0..cols as i32).collect::<Vec<_>>().into();
    let indices: Arc<[i32]> = vec![0i32; cols as usize].into();
    let fragments: Vec<Fragment> = (0..n)
        .map(|id| wide_fragment(id, field_ids.clone(), indices.clone()))
        .collect();
    let schema = wide_schema(cols);

    let sample_file = backfill_file(0, cols as i32);
    let sample_frag = fragments[0].clone();
    println!(
        "encoded DataFile (1 field) = {} B  encoded bootstrap fragment = {} B  F={n} C={cols}",
        pb::DataFile::from(&sample_file).encoded_len(),
        pb::DataFragment::from(&sample_frag).encoded_len()
    );
    let descriptor_only: usize = (0..n)
        .map(|id| pb::DataFile::from(&backfill_file(id, cols as i32)).encoded_len())
        .sum();

    let arms = std::env::var("WIDE_ARMS").unwrap_or_else(|_| "flat,merge,replace".into());
    let run_flat = arms.split(',').any(|a| a == "flat");
    let run_merge = arms.split(',').any(|a| a == "merge");
    let run_replace = arms.split(',').any(|a| a == "replace");
    println!(
        "node/buffer bytes = {}",
        env_u64("WIDE_NODE_BYTES", 1024 * 1024)
    );

    let tmp = tempfile::tempdir().unwrap();
    if run_flat {
        let flat_root = tmp.path().join("flat");
        std::fs::create_dir_all(&flat_root).unwrap();
        let t0 = Instant::now();
        let flat = create_dataset(
            flat_root.to_str().unwrap(),
            fragments.clone(),
            schema.clone(),
            false,
        )
        .await;
        println!("flat create: {:.1}s", t0.elapsed().as_secs_f64());
        let mut flat = LayoutRun {
            name: "flat Merge",
            uri: flat_root.to_str().unwrap().to_string(),
            root: flat_root,
            dataset: flat,
        };
        run_adds(&mut flat, Some(fragments.clone()), n, cols, adds).await;
    }
    if run_merge {
        let tree_merge_root = tmp.path().join("tree_merge");
        std::fs::create_dir_all(&tree_merge_root).unwrap();
        let t1 = Instant::now();
        let tree_merge = create_dataset(
            tree_merge_root.to_str().unwrap(),
            fragments.clone(),
            schema.clone(),
            true,
        )
        .await;
        println!(
            "tree create (merge arm): {:.1}s",
            t1.elapsed().as_secs_f64()
        );
        let mut tree_merge = LayoutRun {
            name: "tree Merge",
            uri: tree_merge_root.to_str().unwrap().to_string(),
            root: tree_merge_root,
            dataset: tree_merge,
        };
        run_adds(&mut tree_merge, Some(fragments.clone()), n, cols, adds).await;
    }
    if run_replace {
        let tree_replace_root = tmp.path().join("tree_replace");
        std::fs::create_dir_all(&tree_replace_root).unwrap();
        let t2 = Instant::now();
        let tree_replace =
            create_dataset(tree_replace_root.to_str().unwrap(), fragments, schema, true).await;
        println!(
            "tree create (replace arm): {:.1}s",
            t2.elapsed().as_secs_f64()
        );
        let mut tree_replace = LayoutRun {
            name: "tree DataReplacement, metadata-only",
            uri: tree_replace_root.to_str().unwrap().to_string(),
            root: tree_replace_root,
            dataset: tree_replace,
        };
        run_adds(&mut tree_replace, None, n, cols, adds).await;
    }

    println!(
        "descriptor-only estimate: {:.2} MiB. Sum of encoded DataFile protobufs for F one-field files. No routing, leaves, or publication.",
        mib(descriptor_only as u64)
    );
}
