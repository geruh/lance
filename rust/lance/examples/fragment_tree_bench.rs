// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Metadata-only fragment tree benchmarks against flat manifests.
//!
//! Fragments reference data files that are never written, so every measured
//! operation isolates manifest and fragment-tree work. `add_columns_real` is
//! the one case that writes real data, to expose metadata work hidden inside a
//! public operation.
//!
//! ```text
//! FRAGMENT_TREE_BENCH_OUT=<dir> fragment_tree_bench <case> [flat|tree|both]
//! ```
//!
//! Cases: checkout, append, merge, rewrite, delete, follow, hydrate, wide, deep,
//! add_columns_real.
//! Sizes come from `BENCH_*` environment variables; see [`Params`].

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::fragment_metadata::FragmentMetadataOptions;
use lance::dataset::transaction::{Operation, RewriteGroup, Transaction};
use lance::dataset::{
    CommitBuilder, Dataset, InsertBuilder, NewColumnTransform, WriteMode, WriteParams,
};
use lance::session::Session;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema;
use lance_file::version::ConcreteFileVersion;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, WrappingObjectStore};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::tracking_store::IoStats;
use lance_table::format::{DataFile, DeletionFile, DeletionFileType, Fragment, pb};
use lance_table::fragment_metadata::support::data_file_path;
use lance_table::fragment_metadata::{FragmentTree, FragmentTreeConfig};
use object_store::ObjectStore as OSObjectStore;
use object_store::list::PaginatedListStore;
use prost::Message;
use serde_json::{Value, json};

const ROWS_PER_FRAGMENT: u64 = 4;
const FILE_VERSION: ConcreteFileVersion = ConcreteFileVersion::V2_0;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Layout {
    Flat,
    Tree,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::Tree => "tree",
        }
    }
}

/// Sizes and tree budgets, read from `BENCH_*` environment variables.
struct Params {
    fragments: u64,
    files: u32,
    columns: u32,
    rounds: usize,
    append_fragments: usize,
    delete_stride: u64,
    rewrite_group: usize,
    /// Fragments each scattered delete touches; the stride default when unset.
    delete_count: Option<u64>,
    followers: Followers,
    writer: Writer,
    lookups: usize,
    tree_overrides: Vec<(&'static str, String)>,
}

/// Which handle tree-layout commits go through. A lazy writer resolves the
/// fragments a commit touches from the tree instead of resident metadata.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Writer {
    Eager,
    Lazy,
}

/// Which follower reads run after each commit.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Followers {
    All,
    Lazy,
    Eager,
}

/// Request latency added to every store the benchmark measures, from
/// `BENCH_GET_MS`, `BENCH_PUT_MS` and `BENCH_GET_MBPS`. Each GET and LIST
/// waits a fixed time, each PUT and copy waits the PUT time, and every GET and
/// PUT then waits for the bytes it moved. It adds round trips to a local directory,
/// and it does not model a real store's throughput limits, concurrency limits,
/// throttling or tail latency.
#[derive(Debug, Clone, Copy)]
struct Latency {
    get: Duration,
    put: Duration,
    per_byte: Duration,
}

impl Latency {
    fn transfer(&self, bytes: u64) -> Duration {
        Duration::from_nanos(self.per_byte.as_nanos() as u64 * bytes)
    }
}

impl WrappingObjectStore for Latency {
    fn wrap(&self, _: &str, original: Arc<dyn OSObjectStore>) -> Arc<dyn OSObjectStore> {
        Arc::new(DelayedStore {
            inner: original,
            latency: *self,
        })
    }

    fn wrap_paginated(
        &self,
        _: &str,
        _: Arc<dyn PaginatedListStore>,
    ) -> Option<Arc<dyn PaginatedListStore>> {
        // Listings then pass through the delayed store.
        None
    }
}

/// Requests the delayed store has seen, by kind, including failed ones such as
/// a HEAD that finds no newer version. The IO tracker records only successes.
static DELAYED_REQUESTS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
const GET: usize = 0;
const HEAD: usize = 1;
const PUT: usize = 2;
const LIST: usize = 3;

fn count(kind: usize) {
    DELAYED_REQUESTS[kind].fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug)]
struct DelayedStore {
    inner: Arc<dyn OSObjectStore>,
    latency: Latency,
}

impl std::fmt::Display for DelayedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DelayedStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl OSObjectStore for DelayedStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        count(PUT);
        let bytes = payload.content_length() as u64;
        tokio::time::sleep(self.latency.put + self.latency.transfer(bytes)).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        count(PUT);
        tokio::time::sleep(self.latency.put).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let head = options.head;
        count(if head { HEAD } else { GET });
        tokio::time::sleep(self.latency.get).await;
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            let bytes = result.range.end - result.range.start;
            tokio::time::sleep(self.latency.transfer(bytes)).await;
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        count(LIST);
        let listing = self.inner.list(prefix);
        let wait = self.latency.get;
        futures::stream::once(async move {
            tokio::time::sleep(wait).await;
            listing
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        count(LIST);
        tokio::time::sleep(self.latency.get).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        count(PUT);
        tokio::time::sleep(self.latency.put).await;
        self.inner.copy_opts(from, to, opts).await
    }
}

/// Store parameters for every measured handle. One shared wrapper keeps the
/// session registries resolving each handle to one store.
fn store_params() -> ObjectStoreParams {
    static WRAPPER: OnceLock<Option<Arc<dyn WrappingObjectStore>>> = OnceLock::new();
    let wrapper = WRAPPER.get_or_init(|| {
        let get_ms: u64 = env_or("BENCH_GET_MS", 0);
        if get_ms == 0 {
            return None;
        }
        let put_ms: u64 = env_or("BENCH_PUT_MS", get_ms);
        let megabytes_per_second: u64 = env_or("BENCH_GET_MBPS", 100);
        let latency = Latency {
            get: Duration::from_millis(get_ms),
            put: Duration::from_millis(put_ms),
            per_byte: Duration::from_nanos(1000 / megabytes_per_second.max(1)),
        };
        Some(Arc::new(latency) as Arc<dyn WrappingObjectStore>)
    });
    ObjectStoreParams {
        object_store_wrapper: wrapper.clone(),
        ..Default::default()
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name).map_or(default, |text| {
        text.parse()
            .unwrap_or_else(|_| panic!("{name}={text:?} is not a valid value"))
    })
}

impl Params {
    fn from_env(case: &str) -> Self {
        let (fragments, files, columns) = match case {
            "wide" => (1024, 1, 2000),
            "deep" => (16_384, 2, 2),
            "add_columns_real" => (2048, 1, 1),
            _ => (65_536, 3, 2),
        };
        let mut tree_overrides = Vec::new();
        for (variable, key) in [
            ("BENCH_LEAF_BYTES", "lance.fragment_metadata.max_leaf_bytes"),
            ("BENCH_NODE_BYTES", "lance.fragment_metadata.max_node_bytes"),
            (
                "BENCH_SEMANTIC_BYTES",
                "lance.fragment_metadata.semantic_buffer_bytes",
            ),
            (
                "BENCH_INLINE_BYTES",
                "lance.fragment_metadata.inline_manifest_budget",
            ),
            (
                "BENCH_SUFFIX_BYTES",
                "lance.fragment_metadata.publication_suffix_budget",
            ),
        ] {
            if let Ok(text) = std::env::var(variable) {
                tree_overrides.push((key, text));
            }
        }
        if case == "deep" && tree_overrides.is_empty() {
            tree_overrides = vec![
                ("lance.fragment_metadata.max_leaf_bytes", "8192".into()),
                ("lance.fragment_metadata.max_node_bytes", "2048".into()),
                (
                    "lance.fragment_metadata.semantic_buffer_bytes",
                    "1024".into(),
                ),
                ("lance.fragment_metadata.inline_manifest_budget", "0".into()),
                (
                    "lance.fragment_metadata.publication_suffix_budget",
                    "512".into(),
                ),
            ];
        }
        Self {
            fragments: env_or("BENCH_FRAGMENTS", fragments),
            files: env_or("BENCH_FILES", files),
            columns: env_or("BENCH_COLUMNS", columns),
            rounds: env_or("BENCH_ROUNDS", 5),
            append_fragments: env_or("BENCH_APPEND_FRAGMENTS", 8),
            delete_stride: env_or("BENCH_DELETE_STRIDE", 17),
            rewrite_group: env_or("BENCH_REWRITE_GROUP", 78),
            delete_count: std::env::var("BENCH_DELETE_COUNT")
                .ok()
                .map(|text| text.parse().expect("BENCH_DELETE_COUNT must be a count")),
            followers: match std::env::var("BENCH_FOLLOWERS").as_deref() {
                Ok("lazy") => Followers::Lazy,
                Ok("eager") => Followers::Eager,
                Ok("all") | Err(_) => Followers::All,
                Ok(other) => panic!("BENCH_FOLLOWERS must be all, lazy or eager, got {other:?}"),
            },
            writer: match std::env::var("BENCH_WRITER").as_deref() {
                Ok("lazy") => Writer::Lazy,
                Ok("eager") | Err(_) => Writer::Eager,
                Ok(other) => panic!("BENCH_WRITER must be eager or lazy, got {other:?}"),
            },
            lookups: env_or("BENCH_LOOKUPS", 16),
            tree_overrides,
        }
    }

    fn describe(&self) -> Value {
        json!({
            "fragments": self.fragments, "files": self.files, "columns": self.columns,
            "rounds": self.rounds, "append_fragments": self.append_fragments,
            "delete_stride": self.delete_stride, "rewrite_group": self.rewrite_group,
            "delete_count": self.delete_count, "lookups": self.lookups,
            "lazy_writer": self.writer == Writer::Lazy,
            "tree_overrides": self.tree_overrides.iter().cloned().collect::<HashMap<_, _>>(),
        })
    }
}

/// Field ids of the base file. Narrow tables use `id` and `name`; wide tables
/// put every column in one file.
fn base_fields(params: &Params) -> Vec<i32> {
    (0..params.columns as i32).collect()
}

fn table_schema(params: &Params) -> Schema {
    let base = (0..params.columns).map(|column| {
        if params.columns == 2 {
            [
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ][column as usize]
                .clone()
        } else {
            Field::new(format!("c{column}"), DataType::Int32, true)
        }
    });
    let backfills =
        (1..params.files).map(|file| Field::new(format!("b{file}"), DataType::Int32, true));
    Schema::try_from(&ArrowSchema::new(base.chain(backfills).collect::<Vec<_>>())).unwrap()
}

fn data_file(path: String, fields: Vec<i32>) -> DataFile {
    let column_indices = (0..fields.len() as i32).collect();
    DataFile::new(
        path,
        fields,
        column_indices,
        FILE_VERSION,
        std::num::NonZero::new(4096),
        None,
    )
}

/// A fragment whose base file holds every base column and whose remaining
/// files each hold one backfilled column, as repeated add-column produces.
fn fragment(params: &Params, id: u64, salt: u64) -> Fragment {
    let mut fragment = Fragment::new(id);
    fragment
        .files
        .push(data_file(data_file_path(id, salt), base_fields(params)));
    for file in 1..params.files {
        fragment.files.push(data_file(
            data_file_path(id, salt + u64::from(file)),
            vec![params.columns as i32 + file as i32 - 1],
        ));
    }
    fragment.physical_rows = Some(ROWS_PER_FRAGMENT as usize);
    fragment
}

fn cpu_time() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes a complete rusage for RUSAGE_SELF.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    let micros = |time: libc::timeval| time.tv_sec as u64 * 1_000_000 + time.tv_usec as u64;
    Duration::from_micros(micros(usage.ru_utime) + micros(usage.ru_stime))
}

struct Probe {
    started: Instant,
    cpu: Duration,
}

impl Probe {
    fn start() -> Self {
        Self {
            cpu: cpu_time(),
            started: Instant::now(),
        }
    }

    fn stop(self) -> Value {
        let wall = self.started.elapsed();
        json!({
            "wall_ms": wall.as_secs_f64() * 1000.0,
            "cpu_ms": (cpu_time() - self.cpu).as_secs_f64() * 1000.0,
        })
    }
}

fn category(path: &str) -> &'static str {
    if path.contains("_bt/leaf/") {
        "leaf"
    } else if path.contains("_bt/node/") {
        "node"
    } else if path.contains("_bt/root/") {
        "root"
    } else if path.contains("/data/") || path.starts_with("data/") {
        "data"
    } else if path.contains("_deletions/") {
        "deletion"
    } else if path.contains("_transactions/") {
        "transaction"
    } else if path.contains("_versions/") || path.ends_with("_latest.manifest") {
        "manifest"
    } else {
        "other"
    }
}

fn io_json(stats: &IoStats) -> Value {
    let mut requests = HashMap::<String, u64>::new();
    let mut written_paths = HashMap::<&str, &'static str>::new();
    for request in &stats.requests {
        let class = category(request.path.as_ref());
        *requests
            .entry(format!("{class}:{}", request.method))
            .or_default() += 1;
        if request.method.starts_with("put") {
            written_paths.insert(request.path.as_ref(), class);
        }
    }
    // Write records carry no sizes, so read them back from the local store
    // after the probe stops. Every run uses a local directory.
    let mut written_by_class = HashMap::<&'static str, u64>::new();
    for (path, class) in written_paths {
        let size = std::fs::metadata(Path::new("/").join(path)).map_or(0, |meta| meta.len());
        *written_by_class.entry(class).or_default() += size;
    }
    let delayed = |kind: usize| DELAYED_REQUESTS[kind].swap(0, Ordering::Relaxed);
    json!({
        "read_iops": stats.read_iops, "read_bytes": stats.read_bytes,
        "write_iops": stats.write_iops, "written_bytes": stats.written_bytes,
        "requests": requests, "written_by_class": written_by_class,
        "delayed_requests": {
            "get": delayed(GET), "head": delayed(HEAD), "put": delayed(PUT), "list": delayed(LIST),
        },
    })
}

async fn tree_shape(dataset: &Dataset) -> Value {
    let Some(descriptor) = dataset.manifest().fragment_tree.as_ref() else {
        return json!({ "fragments": dataset.count_fragments() });
    };
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
    let tree = FragmentTree::open_snapshot(
        store,
        base,
        scheduler,
        Arc::new(LanceCache::with_capacity(0)),
        descriptor,
        manifest.version,
        FragmentTreeConfig::default(),
        manifest.max_fragment_id.map_or(0, |id| u64::from(id) + 1),
    )
    .await
    .unwrap();
    let report = tree.shape_report().await.unwrap();
    let mut leaf_keys = report.leaf_keys.clone();
    leaf_keys.sort_unstable();
    let root_kind = match descriptor.root {
        Some(pb::fragment_tree::Root::InlineRoot(_)) => "inline",
        Some(pb::fragment_tree::Root::RootUuid(_)) => "external",
        None => "none",
    };
    json!({
        "fragments": tree.count_fragments(),
        "height": report.height,
        "leaves": report.leaf_keys.len(),
        "leaf_keys_median": leaf_keys.get(leaf_keys.len() / 2),
        "interior_nodes": report.node_bytes.len(),
        "root_kind": root_kind,
        "root_bytes": report.root_bytes,
        "root_actions": report.root_buffer_len,
        "node_actions": report.node_buffer_lens.iter().sum::<u64>(),
        "suffix_actions": descriptor.mutations_since_root.len(),
        "suffix_bytes": descriptor.mutations_since_root.iter().map(|m| m.encoded_len()).sum::<usize>(),
        "leaf_object_bytes": report.leaf_object_bytes.iter().sum::<u64>(),
        "pending_actions": report.root_buffer_len + report.node_buffer_lens.iter().sum::<u64>(),
        "pending_bytes": report.root_buffer_bytes + report.node_buffer_bytes.iter().sum::<u64>(),
    })
}

struct Bench {
    output: PathBuf,
    case: String,
    layout: Layout,
    params: Params,
    uri: String,
    writer: Dataset,
    writer_store: Arc<ObjectStore>,
    next_salt: u64,
    next_deletion_id: u64,
}

impl Bench {
    async fn create(output: &Path, case: &str, layout: Layout, root: &Path) -> Self {
        let params = Params::from_env(case);
        let path = root.join(layout.name()).to_str().unwrap().to_string();
        // `file-object-store` reads a local directory through the object store
        // API, so a latency wrapper sees manifest reads too. Plain paths take a
        // local fast path that bypasses wrappers.
        let uri = match std::env::var("BENCH_SCHEME") {
            Ok(scheme) => format!("{scheme}://{path}"),
            Err(_) => path,
        };
        let config = match layout {
            Layout::Flat => HashMap::new(),
            Layout::Tree => {
                let mut config = FragmentMetadataOptions::default()
                    .into_table_config()
                    .unwrap();
                for (key, text) in &params.tree_overrides {
                    config.insert((*key).to_string(), text.clone());
                }
                config
            }
        };
        let fragments = if case == "add_columns_real" {
            Vec::new()
        } else {
            (0..params.fragments)
                .map(|id| fragment(&params, id, 0))
                .collect()
        };
        let schema = if case == "add_columns_real" {
            Schema::try_from(&ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int32,
                false,
            )]))
            .unwrap()
        } else {
            table_schema(&params)
        };
        let probe = Probe::start();
        let writer = CommitBuilder::new(uri.as_str())
            .with_store_params(store_params())
            .with_skip_auto_cleanup(true)
            .execute(Transaction::new_from_version(
                0,
                Operation::Overwrite {
                    fragments,
                    schema,
                    config_upsert_values: Some(config),
                    initial_bases: None,
                },
            ))
            .await
            .unwrap();
        let cost = probe.stop();
        let writer_store = writer.object_store(None).await.unwrap();
        writer_store.io_stats_incremental();
        let bench = Self {
            output: output.to_path_buf(),
            case: case.to_string(),
            layout,
            params,
            uri,
            writer,
            writer_store,
            next_salt: 1 << 40,
            next_deletion_id: 1,
        };
        bench.emit(
            "create",
            0,
            cost,
            Value::Null,
            json!({ "params": bench.params.describe() }),
        );
        bench
    }

    fn emit(&self, operation: &str, round: usize, cost: Value, io: Value, extra: Value) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.output)
            .unwrap();
        writeln!(
            file,
            "{}",
            json!({
                "case": self.case, "layout": self.layout.name(), "operation": operation,
                "round": round, "version": self.writer.version_id(), "cost": cost, "io": io,
                "extra": extra,
            })
        )
        .unwrap();
    }

    fn salt(&mut self) -> u64 {
        self.next_salt += 1 << 20;
        self.next_salt
    }

    async fn commit(&mut self, operation_name: &str, round: usize, operation: Operation) {
        if self.params.writer == Writer::Lazy && self.layout == Layout::Tree {
            return self.commit_lazy(operation_name, round, operation).await;
        }
        self.writer_store.io_stats_incremental();
        for requests in &DELAYED_REQUESTS {
            requests.store(0, Ordering::Relaxed);
        }
        let transaction = Transaction::new_from_version(self.writer.version_id(), operation);
        let probe = Probe::start();
        self.writer = CommitBuilder::new(Arc::new(self.writer.clone()))
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .unwrap();
        let cost = probe.stop();
        let io = io_json(&self.writer_store.io_stats_incremental());
        assert!(Arc::ptr_eq(
            &self.writer_store,
            &self.writer.object_store(None).await.unwrap()
        ));
        let shape = tree_shape(&self.writer).await;
        self.emit(operation_name, round, cost, io, json!({ "shape": shape }));
    }

    /// Commit through a lazy handle opened in a fresh session, as a separate
    /// writer that never read the table would. Only the commit is timed and
    /// counted. The eager writer is reloaded afterwards for later inputs.
    async fn commit_lazy(&mut self, operation_name: &str, round: usize, operation: Operation) {
        let session = Arc::new(Session::default());
        let (store, _) =
            ObjectStore::from_uri_and_params(session.store_registry(), &self.uri, &store_params())
                .await
                .unwrap();
        let lazy = DatasetBuilder::from_uri(&self.uri)
            .with_store_params(store_params())
            .with_session(session)
            .with_version(self.writer.version_id())
            .load_lazy()
            .await
            .unwrap();
        store.io_stats_incremental();
        for requests in &DELAYED_REQUESTS {
            requests.store(0, Ordering::Relaxed);
        }
        let transaction = Transaction::new_from_version(lazy.version_id(), operation);
        let probe = Probe::start();
        let committed = lazy.commit(transaction).await.unwrap();
        let cost = probe.stop();
        let io = io_json(&store.io_stats_incremental());
        self.writer = DatasetBuilder::from_uri(&self.uri)
            .with_store_params(store_params())
            .with_session(self.writer.session())
            .with_version(committed.version_id())
            .load()
            .await
            .unwrap();
        self.writer_store = self.writer.object_store(None).await.unwrap();
        let shape = tree_shape(&self.writer).await;
        self.emit(operation_name, round, cost, io, json!({ "shape": shape }));
    }

    async fn append(&mut self, round: usize) {
        let fragments = (0..self.params.append_fragments)
            .map(|_| {
                let salt = self.salt();
                fragment(&self.params, 0, salt)
            })
            .collect();
        self.commit("append", round, Operation::Append { fragments })
            .await;
    }

    async fn merge_column(&mut self, round: usize) {
        let field_id = self.writer.manifest().max_field_id() + 1;
        let mut schema = self
            .writer
            .schema()
            .merge(&ArrowSchema::new(vec![Field::new(
                format!("merged_{round}"),
                DataType::Int32,
                true,
            )]))
            .unwrap();
        schema.set_field_id(Some(field_id - 1));
        let salt = self.salt();
        let fragments = self
            .writer
            .fragments()
            .iter()
            .map(|fragment| {
                let mut fragment = fragment.clone();
                fragment
                    .files
                    .push(data_file(data_file_path(fragment.id, salt), vec![field_id]));
                fragment
            })
            .collect();
        self.commit(
            "merge_column",
            round,
            Operation::Merge {
                fragments,
                schema,
                preserves_nullability: false,
            },
        )
        .await;
    }

    async fn delete_scattered(&mut self, round: usize) {
        let (stride, offset) = match self.params.delete_count {
            // Spread `count` deletes evenly; later rounds shift by one fragment.
            Some(count) => {
                let stride = (self.writer.fragments().len() as u64 / count).max(1);
                (stride, round as u64 % stride)
            }
            None => (
                self.params.delete_stride,
                round as u64 % self.params.delete_stride,
            ),
        };
        let version = self.writer.version_id();
        let updated_fragments = self
            .writer
            .fragments()
            .iter()
            .filter(|fragment| fragment.id % stride == offset)
            .map(|fragment| {
                let mut fragment = fragment.clone();
                let deleted = fragment
                    .deletion_file
                    .as_ref()
                    .map_or(0, |file| file.num_deleted_rows.unwrap());
                assert!((deleted as u64) + 1 < ROWS_PER_FRAGMENT);
                fragment.deletion_file = Some(DeletionFile {
                    read_version: version,
                    id: self.next_deletion_id,
                    file_type: DeletionFileType::Bitmap,
                    num_deleted_rows: Some(deleted + 1),
                    base_id: None,
                });
                self.next_deletion_id += 1;
                fragment
            })
            .collect();
        self.commit(
            "delete_scattered",
            round,
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids: Vec::new(),
                predicate: format!("scattered round {round}"),
            },
        )
        .await;
    }

    /// Remove runs of adjacent fragments at `clusters` spread positions. Each
    /// run retires enough of its default-sized leaf to drain that leaf at once,
    /// so a follower sees about `clusters` changed leaves.
    async fn remove_clustered(&mut self, operation_name: &str, clusters: usize) {
        const RUN: usize = 512;
        let fragments = self.writer.fragments();
        let spacing = fragments.len() / clusters.max(1);
        assert!(spacing >= RUN, "{clusters} runs of {RUN} do not fit");
        let deleted_fragment_ids = (0..clusters)
            .flat_map(|cluster| fragments[cluster * spacing..][..RUN].iter().map(|f| f.id))
            .collect();
        self.commit(
            operation_name,
            0,
            Operation::Delete {
                updated_fragments: Vec::new(),
                deleted_fragment_ids,
                predicate: format!("{clusters} clustered runs"),
            },
        )
        .await;
    }

    async fn rewrite(&mut self, operation_name: &str, group: usize) {
        let salt = self.salt();
        let groups = self
            .writer
            .fragments()
            .chunks(group)
            .map(|old| {
                let mut new = fragment(&self.params, 0, salt + old[0].id);
                new.physical_rows = Some(old.iter().map(|f| f.physical_rows.unwrap()).sum());
                RewriteGroup {
                    old_fragments: old.to_vec(),
                    new_fragments: vec![new],
                }
            })
            .collect();
        self.commit(
            operation_name,
            0,
            Operation::Rewrite {
                groups,
                rewritten_indices: Vec::new(),
                frag_reuse_index: None,
            },
        )
        .await;
    }

    /// Follower reads of the latest version: a fresh eager session, a retained
    /// eager session that follows one commit, the same session polling with no
    /// new commit, and a fresh lazy session.
    async fn readers(&self, retained: &mut Dataset, round: usize, after: &str) {
        if self.params.followers != Followers::Lazy {
            self.eager_readers(retained, round, after).await;
        }
        if self.params.followers != Followers::Eager {
            self.lazy_readers(round, after).await;
        }
    }

    async fn eager_readers(&self, retained: &mut Dataset, round: usize, after: &str) {
        let probe = Probe::start();
        let fresh = DatasetBuilder::from_uri(&self.uri)
            .with_store_params(store_params())
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap();
        let cost = probe.stop();
        let fresh_store = fresh.object_store(None).await.unwrap();
        assert!(!Arc::ptr_eq(&fresh_store, &self.writer_store));
        self.emit(
            "fresh_load",
            round,
            cost,
            io_json(&fresh_store.io_stats_incremental()),
            json!({ "after": after }),
        );
        assert_eq!(fresh.version_id(), self.writer.version_id());

        let retained_store = retained.object_store(None).await.unwrap();
        retained_store.io_stats_incremental();
        let probe = Probe::start();
        retained.checkout_latest().await.unwrap();
        let cost = probe.stop();
        let io = io_json(&retained_store.io_stats_incremental());
        let cache = retained.session().metadata_cache_stats().await;
        self.emit(
            "retained_checkout",
            round,
            cost,
            io,
            json!({
                "after": after,
                "session_cache": {
                    "hits": cache.hits, "misses": cache.misses,
                    "entries": cache.num_entries, "bytes": cache.size_bytes,
                },
            }),
        );
        assert_eq!(retained.version_id(), self.writer.version_id());

        let probe = Probe::start();
        retained.checkout_latest().await.unwrap();
        let cost = probe.stop();
        let io = io_json(&retained_store.io_stats_incremental());
        self.emit("noop_checkout", round, cost, io, json!({ "after": after }));

        let mut scanner = retained.scan();
        scanner
            .project(&[retained.schema().fields[0].name.as_str()])
            .unwrap()
            .limit(Some(128), None)
            .unwrap();
        let probe = Probe::start();
        let plan = scanner.create_plan().await;
        let cost = probe.stop();
        let io = io_json(&retained_store.io_stats_incremental());
        self.emit(
            "plan_narrow_limit",
            round,
            cost,
            io,
            json!({ "after": after, "ok": plan.is_ok() }),
        );
    }

    async fn lazy_readers(&self, round: usize, after: &str) {
        let session = Arc::new(Session::default());
        let probe = Probe::start();
        let lazy = DatasetBuilder::from_uri(&self.uri)
            .with_store_params(store_params())
            .with_session(session.clone())
            .load_lazy()
            .await
            .unwrap();
        let cost = probe.stop();
        let (lazy_store, _) =
            ObjectStore::from_uri_and_params(session.store_registry(), &self.uri, &store_params())
                .await
                .unwrap();
        self.emit(
            "fresh_lazy_load",
            round,
            cost,
            io_json(&lazy_store.io_stats_incremental()),
            json!({ "after": after }),
        );
        let fragments = self.writer.fragments();
        let lookup = |index: usize| {
            let target = fragments[index].id;
            let lazy = &lazy;
            async move {
                let found = lazy.get_fragment(target).await.unwrap();
                assert_eq!(found.as_ref(), Some(&fragments[index]));
            }
        };
        let probe = Probe::start();
        lookup(fragments.len() / 2).await;
        let cost = probe.stop();
        let io = io_json(&lazy_store.io_stats_incremental());
        self.emit(
            "lazy_point_lookup",
            round,
            cost,
            io,
            json!({ "after": after }),
        );

        let probe = Probe::start();
        lookup(fragments.len() / 4).await;
        let cost = probe.stop();
        let io = io_json(&lazy_store.io_stats_incremental());
        self.emit(
            "lazy_point_lookup_warm",
            round,
            cost,
            io,
            json!({ "after": after }),
        );

        let missing = self
            .writer
            .manifest()
            .max_fragment_id
            .map_or(0, |id| u64::from(id) + 1);
        let probe = Probe::start();
        assert!(lazy.get_fragment(missing).await.unwrap().is_none());
        let cost = probe.stop();
        let io = io_json(&lazy_store.io_stats_incremental());
        self.emit(
            "lazy_point_lookup_miss",
            round,
            cost,
            io,
            json!({ "after": after }),
        );

        let lookups = self.params.lookups.max(1);
        let probe = Probe::start();
        for step in 0..lookups {
            lookup(step * fragments.len() / lookups).await;
        }
        let cost = probe.stop();
        let io = io_json(&lazy_store.io_stats_incremental());
        self.emit(
            "lazy_point_lookup_sweep",
            round,
            cost,
            io,
            json!({ "after": after, "lookups": lookups }),
        );
    }

    async fn retained_reader(&self) -> Dataset {
        let mut reader = DatasetBuilder::from_uri(&self.uri)
            .with_store_params(store_params())
            .with_session(Arc::new(Session::default()))
            .load()
            .await
            .unwrap();
        reader.checkout_latest().await.unwrap();
        reader
    }

    async fn add_columns_real(&mut self) {
        let rows = self.params.fragments as usize;
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from_iter_values(0..rows as i32))],
        )
        .unwrap();
        let params = WriteParams {
            max_rows_per_file: 1,
            max_rows_per_group: 1,
            mode: WriteMode::Append,
            ..Default::default()
        };
        let transaction = InsertBuilder::new(Arc::new(self.writer.clone()))
            .with_params(&params)
            .execute_uncommitted(vec![batch])
            .await
            .unwrap();
        self.writer = CommitBuilder::new(Arc::new(self.writer.clone()))
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .unwrap();
        assert_eq!(self.writer.count_fragments(), rows);
        for round in 0..self.params.rounds {
            self.writer_store.io_stats_incremental();
            let probe = Probe::start();
            self.writer
                .add_columns(
                    NewColumnTransform::SqlExpressions(vec![(
                        format!("backfill_{round}"),
                        format!("id + {}", round + 1),
                    )]),
                    None,
                    None,
                )
                .await
                .unwrap();
            let cost = probe.stop();
            let io = io_json(&self.writer_store.io_stats_incremental());
            self.emit("add_columns", round, cost, io, Value::Null);
        }
    }

    async fn run(&mut self) {
        let rounds = self.params.rounds;
        match self.case.as_str() {
            "checkout" | "wide" | "deep" => {
                let mut retained = self.retained_reader().await;
                for round in 0..rounds {
                    self.append(round).await;
                    self.readers(&mut retained, round, "append").await;
                }
                for round in 0..rounds {
                    self.delete_scattered(round).await;
                    self.readers(&mut retained, round, "delete").await;
                }
            }
            "append" => {
                for round in 0..rounds {
                    self.append(round).await;
                }
            }
            "follow" => {
                // A retained session follows commits that change no leaf, one
                // leaf, several leaves, and every leaf.
                let mut retained = self.retained_reader().await;
                for round in 0..rounds {
                    self.append(round).await;
                    self.eager_readers(&mut retained, round, "append").await;
                    self.remove_clustered("remove_one_run", 1).await;
                    self.eager_readers(&mut retained, round, "one_leaf").await;
                    self.remove_clustered("remove_four_runs", 4).await;
                    self.eager_readers(&mut retained, round, "four_leaves")
                        .await;
                    self.merge_column(round).await;
                    self.eager_readers(&mut retained, round, "rewrite").await;
                }
            }
            "merge" => {
                let mut retained = self.retained_reader().await;
                for round in 0..rounds {
                    self.merge_column(round).await;
                    self.readers(&mut retained, round, "merge").await;
                }
            }
            "delete" => {
                let mut retained = self.retained_reader().await;
                for round in 0..rounds {
                    self.delete_scattered(round).await;
                    self.readers(&mut retained, round, "delete").await;
                }
            }
            "rewrite" => {
                let mut retained = self.retained_reader().await;
                self.rewrite("compact", self.params.rewrite_group).await;
                self.readers(&mut retained, 0, "compact").await;
                for round in 0..rounds {
                    self.append(round).await;
                    self.readers(&mut retained, round, "append_after_compact")
                        .await;
                }
                self.rewrite("compact_again", self.params.rewrite_group)
                    .await;
                self.readers(&mut retained, 0, "compact_again").await;
            }
            "hydrate" => {
                for round in 0..rounds {
                    let probe = Probe::start();
                    let fresh = DatasetBuilder::from_uri(&self.uri)
                        .with_store_params(store_params())
                        .with_session(Arc::new(Session::default()))
                        .load()
                        .await
                        .unwrap();
                    let cost = probe.stop();
                    let io = fresh
                        .object_store(None)
                        .await
                        .unwrap()
                        .io_stats_incremental();
                    self.emit(
                        "fresh_load",
                        round,
                        cost,
                        io_json(&io),
                        json!({ "after": "create" }),
                    );
                }
            }
            "add_columns_real" => self.add_columns_real().await,
            other => panic!("unknown case {other:?}"),
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut arguments = std::env::args().skip(1);
    let case = arguments
        .next()
        .expect("usage: fragment_tree_bench <case> [flat|tree|both]");
    let layouts = match arguments.next().as_deref().unwrap_or("both") {
        "flat" => vec![Layout::Flat],
        "tree" => vec![Layout::Tree],
        "both" => vec![Layout::Flat, Layout::Tree],
        other => panic!("layout must be flat, tree, or both, got {other:?}"),
    };
    let output_dir = PathBuf::from(
        std::env::var_os("FRAGMENT_TREE_BENCH_OUT").expect("set FRAGMENT_TREE_BENCH_OUT"),
    );
    std::fs::create_dir_all(&output_dir).unwrap();
    let output = output_dir.join(format!("{case}.jsonl"));
    let mut final_fragments = Vec::new();
    for layout in layouts {
        let root = tempfile::tempdir_in(&output_dir).unwrap();
        let mut bench = Bench::create(&output, &case, layout, root.path()).await;
        Box::pin(bench.run()).await;
        let fresh = Dataset::open(&bench.uri).await.unwrap();
        final_fragments.push((layout, fresh.fragments().as_ref().clone()));
    }
    if let [(_, flat), (_, tree)] = final_fragments.as_slice() {
        // Real data files have random names, so only their shape can match.
        let shape = |fragments: &[Fragment]| -> Vec<_> {
            fragments
                .iter()
                .map(|fragment| (fragment.id, fragment.files.len(), fragment.physical_rows))
                .collect()
        };
        if case == "add_columns_real" {
            assert_eq!(
                shape(flat),
                shape(tree),
                "flat and tree final shapes differ"
            );
        } else {
            assert!(flat == tree, "flat and tree final fragments differ");
        }
    }
}
