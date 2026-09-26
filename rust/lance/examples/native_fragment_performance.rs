// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema;
use lance_datafusion::exec::{LanceExecutionOptions, execute_plan};
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::tracking_store::IoStats;
use lance_table::fragment_metadata::{FragmentTree, FragmentTreeConfig};
use serde_json::{Value, json};

use lance::dataset::builder::DatasetBuilder;
use lance::dataset::fragment_metadata::FragmentMetadataOptions;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{
    CommitBuilder, Dataset, InsertBuilder, NewColumnTransform, WriteMode, WriteParams,
};
use lance::session::Session;

const ROWS_PER_FRAGMENT: usize = 4;
const QUERY_ROWS: usize = 128;

type Inventory = BTreeMap<String, u64>;

struct Run {
    output: PathBuf,
    root: PathBuf,
    uri: String,
    layout: String,
    dataset: Dataset,
    store: Arc<ObjectStore>,
    expected: BTreeSet<i32>,
    next_id: i32,
    seed_columns: usize,
    backfills: usize,
}

async fn open_tree(dataset: &Dataset) -> FragmentTree {
    let session = Session::default();
    let (store, base) = ObjectStore::from_uri_and_params(
        session.store_registry(),
        dataset.uri(),
        &Default::default(),
    )
    .await
    .unwrap();
    let scheduler = ScanScheduler::new(store.clone(), SchedulerConfig::max_bandwidth(&store));
    let manifest = dataset.manifest();
    assert!(
        manifest.base_paths.is_empty(),
        "this fixture has no foreign bases"
    );
    FragmentTree::open_snapshot(
        store,
        base,
        scheduler,
        Arc::new(LanceCache::with_capacity(0)),
        manifest.fragment_tree.as_ref().unwrap(),
        manifest.version,
        FragmentTreeConfig::default(),
        manifest.max_fragment_id.map_or(0, |id| u64::from(id) + 1),
    )
    .await
    .unwrap()
}

fn count(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

fn emit(output: &Path, value: Value) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output)
        .unwrap();
    writeln!(file, "{value}").unwrap();
}

fn category(path: &str) -> &'static str {
    if path.contains("/_bt/") {
        "tree"
    } else if path.contains("/data/") {
        "data"
    } else if path.contains("/_deletions/") {
        "deletion"
    } else if path.contains("/_transactions/") {
        "transaction"
    } else if path.contains("/_versions/") || path.ends_with("/_latest.manifest") {
        "manifest"
    } else {
        "other"
    }
}

fn inventory(root: &Path) -> Inventory {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        if !directory.exists() {
            continue;
        }
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                files.insert(entry.path().to_string_lossy().into_owned(), metadata.len());
            }
        }
    }
    files
}

fn io_json(stats: &IoStats) -> Value {
    let mut methods = BTreeMap::<String, u64>::new();
    for request in &stats.requests {
        *methods
            .entry(format!(
                "{}:{}",
                category(request.path.as_ref()),
                request.method
            ))
            .or_default() += 1;
    }
    json!({
        "read_requests": stats.read_iops,
        "read_bytes": stats.read_bytes,
        "write_requests": stats.write_iops,
        "written_bytes": stats.written_bytes,
        "requests_by_category_and_method": methods,
        "request_sample": stats.requests.iter().take(32).map(|request| json!({
            "method": request.method,
            "path": request.path.to_string(),
            "range": request.range.as_ref().map(|range| [range.start, range.end]),
        })).collect::<Vec<_>>(),
    })
}

fn written_files(before: &Inventory, after: &Inventory, stats: &IoStats) -> Value {
    let mut bytes = BTreeMap::<&str, u64>::new();
    let mut objects = BTreeMap::<&str, u64>::new();
    for (path, size) in after {
        if !before.contains_key(path) {
            *bytes.entry(category(path)).or_default() += size;
            *objects.entry(category(path)).or_default() += 1;
        }
    }
    let mut payload_bytes = BTreeMap::<&str, u64>::new();
    let mut unresolved = Vec::new();
    let mut multipart_paths = BTreeSet::new();
    for request in &stats.requests {
        if !matches!(request.method, "put" | "put_opts" | "put_part") {
            continue;
        }
        if request.method == "put_part" && !multipart_paths.insert(request.path.to_string()) {
            continue;
        }
        let path = format!("/{}", request.path.as_ref().trim_start_matches('/'));
        let final_path = path
            .split_once(".manifest-")
            .map_or(path.clone(), |(prefix, _)| format!("{prefix}.manifest"));
        if let Some(size) = after.get(&final_path) {
            *payload_bytes.entry(category(&final_path)).or_default() += size;
        } else {
            unresolved.push(path);
        }
    }
    let accounted: u64 = payload_bytes.values().sum();
    let total: u64 = bytes.values().sum();
    let missing: Vec<_> = before
        .keys()
        .filter(|path| !after.contains_key(*path))
        .collect();
    json!({
        "new_object_bytes": total,
        "new_bytes_by_category": bytes,
        "new_objects_by_category": objects,
        "removed_objects": missing,
        "tracked_write_minus_new_object_bytes": i128::from(stats.written_bytes) - i128::from(total),
        "accounted_write_payload_bytes": accounted,
        "write_payload_bytes_by_category": payload_bytes,
        "unresolved_write_paths": unresolved,
        "payload_bytes_reconcile": unresolved.is_empty() && stats.written_bytes == accounted,
    })
}

impl Run {
    async fn create(output: PathBuf, layout: String, seed_columns: usize) -> Self {
        let root = output.parent().unwrap().join("table");
        assert!(!root.exists(), "use a new output directory for each run");
        let uri = root.to_str().unwrap().to_string();
        let schema = Arc::new(ArrowSchema::new(
            std::iter::once(Field::new("id", DataType::Int32, false))
                .chain(
                    (0..seed_columns)
                        .map(|column| Field::new(format!("seed_{column}"), DataType::Int32, false)),
                )
                .collect::<Vec<_>>(),
        ));
        let config = if layout == "tree" {
            FragmentMetadataOptions::default()
                .into_table_config()
                .unwrap()
        } else {
            assert_eq!(layout, "flat");
            HashMap::new()
        };
        let dataset = CommitBuilder::new(uri.as_str())
            .with_skip_auto_cleanup(true)
            .execute(Transaction::new_from_version(
                0,
                Operation::Overwrite {
                    fragments: Vec::new(),
                    schema: Schema::try_from(schema.as_ref()).unwrap(),
                    config_upsert_values: Some(config),
                    initial_bases: None,
                },
            ))
            .await
            .unwrap();
        let store = dataset.object_store(None).await.unwrap();
        Self {
            output,
            root,
            uri,
            layout,
            dataset,
            store,
            expected: BTreeSet::new(),
            next_id: 0,
            seed_columns,
            backfills: 0,
        }
    }

    fn batch(&self, rows: usize) -> RecordBatch {
        let ids: Vec<i32> = (self.next_id..self.next_id + i32::try_from(rows).unwrap()).collect();
        let columns: Vec<ArrayRef> =
            std::iter::once(Arc::new(Int32Array::from(ids.clone())) as ArrayRef)
                .chain((0..self.seed_columns).map(|column| {
                    Arc::new(Int32Array::from(
                        ids.iter()
                            .map(|id| *id + column as i32 + 1)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                }))
                .chain((0..self.backfills).map(|round| {
                    Arc::new(Int32Array::from(
                        ids.iter()
                            .map(|id| *id + round as i32 + 1)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                }))
                .collect();
        RecordBatch::try_new(Arc::new(ArrowSchema::from(self.dataset.schema())), columns).unwrap()
    }

    async fn shape(&self) -> Value {
        if self.layout == "flat" {
            return json!({ "fragments": self.dataset.fragments().len(), "pending_actions": 0 });
        }
        let tree = open_tree(&self.dataset).await;
        let report = tree.shape_report().await.unwrap();
        let pending = report.root_buffer_len + report.node_buffer_lens.iter().sum::<u64>();
        json!({
            "fragments": tree.count_fragments(),
            "height_edges": report.height,
            "leaves": report.leaf_keys.len(),
            "interior_nodes_excluding_root": report.node_bytes.len(),
            "root_bytes": report.root_bytes,
            "leaf_object_bytes": report.leaf_object_bytes,
            "node_bytes": report.node_bytes,
            "pending_actions": pending,
            "pending_bytes": report.root_buffer_bytes + report.node_buffer_bytes.iter().sum::<u64>(),
        })
    }

    fn begin(&self) -> Inventory {
        let before = inventory(&self.root);
        self.store.io_stats_incremental();
        before
    }

    async fn finish(&self, operation: &str, elapsed_ms: f64, before: Inventory, details: Value) {
        let stats = self.store.io_stats_incremental();
        let current_store = self.dataset.object_store(None).await.unwrap();
        assert!(
            Arc::ptr_eq(&self.store, &current_store),
            "writer changed object store; accounting must follow it"
        );
        let after = inventory(&self.root);
        emit(
            &self.output,
            json!({
                "event": "operation", "layout": self.layout, "operation": operation,
                "version": self.dataset.version_id(), "operation_ms": elapsed_ms,
                "io": io_json(&stats), "files": written_files(&before, &after, &stats),
                "details": details,
                "rows": self.expected.len(), "files_referenced": self.dataset.fragments().iter().map(|fragment| fragment.files.len()).sum::<usize>(),
            }),
        );
    }

    async fn record_shape(&self, operation: &str) {
        emit(
            &self.output,
            json!({
                "event": "shape", "layout": self.layout, "version": self.dataset.version_id(),
                "operation": operation, "shape": self.shape().await,
            }),
        );
    }

    async fn append(&mut self, rows: usize, name: &str) {
        let batch = self.batch(rows);
        let before = self.begin();
        let params = WriteParams {
            max_rows_per_file: ROWS_PER_FRAGMENT,
            max_rows_per_group: ROWS_PER_FRAGMENT,
            mode: WriteMode::Append,
            ..Default::default()
        };
        let started = Instant::now();
        let transaction = InsertBuilder::new(Arc::new(self.dataset.clone()))
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .unwrap();
        let data_ms = started.elapsed().as_secs_f64() * 1000.0;
        let data_stats = self.store.io_stats_snapshot();
        let commit_started = Instant::now();
        self.dataset = CommitBuilder::new(Arc::new(self.dataset.clone()))
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .unwrap();
        let commit_ms = commit_started.elapsed().as_secs_f64() * 1000.0;
        let elapsed_ms = data_ms + commit_ms;
        self.expected
            .extend(self.next_id..self.next_id + rows as i32);
        self.next_id += rows as i32;
        self.finish(name, elapsed_ms, before, json!({ "data_write_ms": data_ms, "commit_ms": commit_ms, "data_stage_io": io_json(&data_stats) })).await;
    }

    async fn backfill(&mut self, name: &str) {
        let round = self.backfills;
        let file_counts: Vec<_> = self
            .dataset
            .fragments()
            .iter()
            .map(|fragment| (fragment.id, fragment.files.len()))
            .collect();
        let before = self.begin();
        let started = Instant::now();
        self.dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![(
                    format!("backfill_{round}"),
                    format!("CAST(id + {} AS INT)", round + 1),
                )]),
                None,
                None,
            )
            .await
            .unwrap();
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        self.backfills += 1;
        self.finish(name, elapsed_ms, before, json!({ "round": round, "timing_scope": "public operation including data rewrite and commit" })).await;
        assert_eq!(file_counts.len(), self.dataset.fragments().len());
        for ((id, count), fragment) in file_counts.iter().zip(self.dataset.fragments().iter()) {
            assert_eq!(*id, fragment.id);
            assert_eq!(fragment.files.len(), count + 1);
        }
    }

    async fn fresh_reader(&self) -> Dataset {
        let started = Instant::now();
        let reader = DatasetBuilder::from_uri(&self.uri)
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap();
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let reader_store = reader.object_store(None).await.unwrap();
        assert!(!Arc::ptr_eq(&self.store, &reader_store));
        let stats = reader_store.io_stats_incremental();
        emit(
            &self.output,
            json!({ "event": "reader_open", "reader": "fresh_session", "layout": self.layout, "version": reader.version_id(), "manifest_transaction_inline": reader.manifest().transaction_section.is_some(), "open_ms": elapsed_ms, "io": io_json(&stats) }),
        );
        reader
    }

    async fn read(&self, reader: &mut Dataset, kind: &str, operation: &str) {
        let store = reader.object_store(None).await.unwrap();
        store.io_stats_incremental();
        let version_before = reader.version_id();
        let started = Instant::now();
        reader.checkout_latest().await.unwrap();
        let checkout_ms = started.elapsed().as_secs_f64() * 1000.0;
        let current_store = reader.object_store(None).await.unwrap();
        assert!(Arc::ptr_eq(&store, &current_store));
        assert!(!Arc::ptr_eq(&self.store, &store));
        let checkout_io = store.io_stats_incremental();
        assert_eq!(reader.version_id(), self.dataset.version_id());
        let mut scanner = reader.scan();
        scanner
            .project(&["id"])
            .unwrap()
            .limit(Some(QUERY_ROWS as i64), None)
            .unwrap()
            .scan_in_order(true);
        let started = Instant::now();
        let plan = scanner.create_plan().await.unwrap();
        let plan_ms = started.elapsed().as_secs_f64() * 1000.0;
        let plan_io = store.io_stats_incremental();
        let started = Instant::now();
        let mut stream = execute_plan(plan, LanceExecutionOptions::default()).unwrap();
        let mut actual = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            actual.extend(ids.values().iter().copied());
        }
        let execution_ms = started.elapsed().as_secs_f64() * 1000.0;
        let execution_io = store.io_stats_incremental();
        assert_eq!(actual.len(), self.expected.len().min(QUERY_ROWS));
        let unique: BTreeSet<_> = actual.iter().copied().collect();
        assert_eq!(unique.len(), actual.len());
        assert!(unique.is_subset(&self.expected));
        emit(
            &self.output,
            json!({
                "event": "reader", "reader": kind, "after_operation": operation, "layout": self.layout, "version": reader.version_id(), "version_before": version_before,
                "checkout_ms": checkout_ms, "plan_ms": plan_ms, "execution_ms": execution_ms,
                "checkout_io": io_json(&checkout_io), "plan_io": io_json(&plan_io), "execution_io": io_json(&execution_io), "rows": actual.len(),
            }),
        );
    }

    async fn verify(&self, phase: &str) {
        let fresh = Dataset::open(&self.uri).await.unwrap();
        let mut scanner = fresh.scan();
        let mut projection = vec!["id".to_string()];
        projection.extend((0..self.backfills).map(|round| format!("backfill_{round}")));
        scanner.project(&projection).unwrap();
        let mut stream = scanner.try_into_stream().await.unwrap();
        let mut actual = BTreeSet::new();
        let mut row_count = 0;
        while let Some(batch) = stream.try_next().await.unwrap() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let id = ids.value(row);
                assert!(actual.insert(id), "duplicate row {id}");
                for round in 0..self.backfills {
                    let values = batch
                        .column(round + 1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    assert!(!values.is_null(row));
                    assert_eq!(values.value(row), id + round as i32 + 1);
                }
                row_count += 1;
            }
        }
        assert_eq!(actual, self.expected);
        assert_eq!(fresh.count_rows(None).await.unwrap(), row_count);
        if self.layout == "tree" {
            let tree = open_tree(&fresh).await;
            tree.verify_reachable().await.unwrap();
            assert_eq!(
                tree.materialize().await.unwrap(),
                fresh.fragments().as_ref().clone()
            );
        }
        emit(
            &self.output,
            json!({ "event": "verification", "layout": self.layout, "phase": phase, "rows": row_count, "row_id_sum": actual.iter().map(|id| i64::from(*id)).sum::<i64>(), "version": fresh.version_id(), "all_backfill_values_checked": true }),
        );
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let Some(output_dir) = std::env::var_os("NATIVE_PERF_OUT") else {
        return;
    };
    let output_dir = PathBuf::from(output_dir);
    std::fs::create_dir_all(&output_dir).unwrap();
    let layout = std::env::var("NATIVE_PERF_LAYOUT").unwrap_or_else(|_| "tree".to_string());
    let fragments = count("NATIVE_PERF_FRAGMENTS", 512);
    let backfills = count("NATIVE_PERF_BACKFILLS", 8);
    let setup_backfills = count("NATIVE_PERF_SETUP_BACKFILLS", 0);
    let appends = count("NATIVE_PERF_APPENDS", 12);
    let deletes = count("NATIVE_PERF_DELETES", 12);
    let seed_columns = count("NATIVE_PERF_SEED_COLUMNS", 0);
    assert!(fragments > 0 && fragments <= 262_144);
    assert!(deletes <= 17, "one selected row per original fragment");
    let output = output_dir.join("measurements.jsonl");
    assert!(!output.exists(), "use a new output directory for each run");
    emit(
        &output,
        json!({
            "event": "configuration", "binary_kind": "production-linked Cargo example", "inline_transaction_bytes": 20 * 1024 * 1024, "layout": layout, "fragments": fragments, "backfills": backfills, "setup_backfills": setup_backfills, "appends": appends, "deletes": deletes, "seed_columns": seed_columns,
            "rows_per_fragment": ROWS_PER_FRAGMENT, "query_projection": ["id"], "query_limit": QUERY_ROWS,
            "reader_api": "eager Dataset", "reader_cache": "Session::default; fresh session versus retained session", "storage": "local filesystem; OS cache not cleared",
            "fanout": 16, "leaf_bytes": 1_048_576, "node_bytes": 1_048_576, "semantic_buffer_bytes": 262_144, "inline_root_bytes": 65_536, "suffix_bytes": 32_768,
            "memory_scope": "measure externally; whole process includes harness and readers",
        }),
    );
    let mut run = Run::create(output, layout, seed_columns).await;
    run.append(fragments * ROWS_PER_FRAGMENT, "seed").await;
    run.record_shape("seed").await;
    run.verify("seed").await;
    for _ in 0..setup_backfills {
        run.backfill("setup_backfill").await;
        run.record_shape("setup_backfill").await;
    }
    run.verify("setup_complete").await;
    let mut retained = run.fresh_reader().await;
    run.read(&mut retained, "retained_warmup", "seed").await;
    for _ in 0..appends {
        let mut fresh = run.fresh_reader().await;
        run.append(8, "append").await;
        run.read(&mut fresh, "fresh_session", "append").await;
        run.read(&mut retained, "retained_session", "append").await;
        run.record_shape("append").await;
    }
    run.verify("appends").await;
    for _ in 0..backfills {
        let mut fresh = run.fresh_reader().await;
        run.backfill("add_columns").await;
        run.read(&mut fresh, "fresh_session", "add_columns").await;
        run.read(&mut retained, "retained_session", "add_columns")
            .await;
        run.record_shape("add_columns").await;
    }
    run.verify("backfills").await;
    for round in 0..deletes {
        let mut fresh = run.fresh_reader().await;
        let before = run.begin();
        let started = Instant::now();
        run.dataset
            .delete(&format!("id % 68 = {}", round * ROWS_PER_FRAGMENT))
            .await
            .unwrap();
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        run.expected
            .retain(|id| *id % 68 != (round * ROWS_PER_FRAGMENT) as i32);
        run.finish("delete_scattered", elapsed_ms, before, json!({ "round": round, "timing_scope": "public operation including scan, deletion files, and commit" })).await;
        run.read(&mut fresh, "fresh_session", "delete_scattered")
            .await;
        run.read(&mut retained, "retained_session", "delete_scattered")
            .await;
        run.record_shape("delete_scattered").await;
    }
    run.verify("deletes").await;
    let mut fresh = run.fresh_reader().await;
    let before = run.begin();
    let started = Instant::now();
    let compacted = compact_files(
        &mut run.dataset,
        CompactionOptions {
            target_rows_per_fragment: 256,
            max_rows_per_group: 256,
            num_threads: Some(4),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    run.finish("compact", elapsed_ms, before, json!({ "fragments_added": compacted.fragments_added, "fragments_removed": compacted.fragments_removed, "timing_scope": "public operation including planning, data rewrite, and commits" })).await;
    run.read(&mut fresh, "fresh_session", "compact").await;
    run.read(&mut retained, "retained_session", "compact").await;
    run.record_shape("compact").await;
    run.verify("compaction").await;
    if run.layout == "tree" {
        let before = run.begin();
        let started = Instant::now();
        run.dataset
            .update_config([("lance.fragment_metadata.materialization", "bulk")])
            .await
            .unwrap();
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        run.finish(
            "final_drain_config",
            elapsed_ms,
            before,
            json!({ "test_only": true }),
        )
        .await;
        run.record_shape("final_drain_config").await;
        let before = run.begin();
        let started = Instant::now();
        run.dataset
            .update_config([("native_perf.final_drain", "true")])
            .await
            .unwrap();
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        run.finish(
            "final_drain",
            elapsed_ms,
            before,
            json!({ "test_only": true }),
        )
        .await;
        run.record_shape("final_drain").await;
        assert_eq!(run.shape().await["pending_actions"], 0);
        run.verify("final_drain").await;
    }
    emit(
        &run.output,
        json!({ "event": "complete", "layout": run.layout, "rows": run.expected.len(), "version": run.dataset.version_id(), "shape": run.shape().await }),
    );
}
