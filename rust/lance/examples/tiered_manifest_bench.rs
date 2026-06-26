// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// Reporting binary, same as the bench targets.
#![allow(clippy::print_stdout)]

//! Benchmark for the tiered (Bε) manifest layout. Produces the numbers behind
//! `devtools/tiered-manifest/BENCHMARK.md`.
//!
//! Scenario A ("jack"): real single-row appends, flat default vs tiered opt-in
//! with the default 100K buffer cap. Nothing seals at this scale, so the two
//! layouts must tie.
//!
//! Scenario B ("high-n"): a table with N fabricated fragment entries committed
//! through the real commit path, flat vs tiered ε = 100K. Measures the
//! migration commit, steady-state append commits (root PUT bytes), cold open,
//! and warm reopen IO. Metadata-only: fragments reference synthetic data files
//! that are never read.
//!
//! Local filesystem only, not S3; latency numbers are lower bounds while byte
//! and request counts transfer directly.
//!
//! ```bash
//! cargo run --release --example tiered_manifest_bench -- --jack 10000 --high-n 150000
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance::Dataset;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, WriteMode, WriteParams};
use lance_core::datatypes::Schema;
use lance_table::format::{
    DataFile, Fragment, MANIFEST_BUFFER_CAP_KEY, MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED,
};

const HIGH_N_BUFFER_CAP: &str = "100000";

fn single_row_batch(value: i32) -> impl arrow_array::RecordBatchReader + Send + 'static {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![value]))],
    )
    .unwrap();
    RecordBatchIterator::new(vec![Ok(batch)], schema)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

/// Bytes of the latest root manifest plus every child manifest on disk.
fn metadata_footprint(root: &std::path::Path) -> (u64, u64, usize) {
    let latest_root = std::fs::read_dir(root.join("_versions"))
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                // V2 naming encodes versions descending, so the lexically
                // smallest name is the newest manifest.
                .min_by_key(|e| e.file_name())
                .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
                .unwrap_or(0)
        })
        .unwrap_or(0);
    let (children_bytes, children_count) = std::fs::read_dir(root.join("_manifest_children"))
        .map(|rd| {
            let mut bytes = 0;
            let mut count = 0;
            for entry in rd.filter_map(|e| e.ok()) {
                bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
                count += 1;
            }
            (bytes, count)
        })
        .unwrap_or((0, 0));
    (latest_root, children_bytes, children_count)
}

async fn scenario_a_one_layout(appends: usize, tiered: bool) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();

    let mut ds = Dataset::write(
        single_row_batch(0),
        uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    if tiered {
        ds.update_config([
            (MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED),
            (MANIFEST_BUFFER_CAP_KEY, HIGH_N_BUFFER_CAP),
        ])
        .await
        .unwrap();
    }

    let mut commit_times = Vec::with_capacity(appends);
    let run_start = Instant::now();
    for i in 0..appends {
        let started = Instant::now();
        ds = Dataset::write(
            single_row_batch(i as i32),
            Arc::new(ds),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        commit_times.push(started.elapsed());
    }
    let total = run_start.elapsed();

    let open_start = Instant::now();
    let reopened = Dataset::open(uri).await.unwrap();
    let open_elapsed = open_start.elapsed();
    assert_eq!(reopened.manifest.fragments.len(), appends + 1);

    commit_times.sort();
    let (root_bytes, child_bytes, child_count) = metadata_footprint(dir.path());
    println!(
        "| {} | {appends} | {:.2} | {:.2} | {:.2} | {:.1} | {:.0} | {:.3} | {child_count} |",
        if tiered { "tiered" } else { "flat" },
        percentile(&commit_times, 0.50).as_secs_f64() * 1e3,
        percentile(&commit_times, 0.99).as_secs_f64() * 1e3,
        total.as_secs_f64(),
        open_elapsed.as_secs_f64() * 1e3,
        (root_bytes + child_bytes) as f64 / 1024.0,
        root_bytes as f64 / 1024.0 / 1024.0,
    );
}

/// A fragment entry shaped like Jack's benchmark rows: one data file with a
/// UUID-length path. Metadata only; the data file does not exist.
fn fabricated_fragment(id: u64) -> Fragment {
    let mut fragment = Fragment::new(id).with_physical_rows(1_000_000);
    fragment.files.push(DataFile::new(
        format!("{id:032x}.lance"),
        vec![0],
        vec![0],
        2,
        0,
        std::num::NonZero::new(64 * 1024 * 1024),
        None,
    ));
    fragment
}

fn bench_schema() -> Schema {
    let arrow_schema = ArrowSchema::new(vec![ArrowField::new("id", DataType::Int64, false)]);
    Schema::try_from(&arrow_schema).unwrap()
}

async fn fabricated_append(ds: Dataset, next_id: u64) -> (Dataset, Duration) {
    let txn = Transaction::new_from_version(
        ds.version().version,
        Operation::Append {
            fragments: vec![fabricated_fragment(next_id)],
        },
    );
    let started = Instant::now();
    let ds = CommitBuilder::new(Arc::new(ds)).execute(txn).await.unwrap();
    (ds, started.elapsed())
}

async fn root_manifest_bytes(ds: &Dataset) -> u64 {
    let (_, location) = ds.latest_manifest().await.unwrap();
    location.size.unwrap_or(0)
}

async fn scenario_b_one_layout(total_fragments: u64, tiered: bool) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let layout = if tiered { "tiered" } else { "flat" };

    // One overwrite commit installs the fabricated table (identical for both
    // layouts: the config is not tiered yet, so this writes a flat root).
    let fragments: Vec<Fragment> = (0..total_fragments).map(fabricated_fragment).collect();
    let txn = Transaction::new_from_version(
        0,
        Operation::Overwrite {
            fragments,
            schema: bench_schema(),
            config_upsert_values: None,
            initial_bases: None,
        },
    );
    let mut ds = CommitBuilder::new(uri).execute(txn).await.unwrap();

    if tiered {
        ds.update_config([
            (MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED),
            (MANIFEST_BUFFER_CAP_KEY, HIGH_N_BUFFER_CAP),
        ])
        .await
        .unwrap();
    }

    // First append after opt-in: for tiered this is the one-time migration
    // commit that seals the children.
    let mut next_id = total_fragments;
    let (ds_after, migration) = fabricated_append(ds, next_id).await;
    let mut ds = ds_after;
    next_id += 1;
    let migration_root = root_manifest_bytes(&ds).await;
    let (_, child_bytes, child_count) = metadata_footprint(dir.path());

    // Steady-state appends: root PUT only.
    let mut steady = Vec::new();
    let mut steady_root = 0;
    for _ in 0..3 {
        let (ds_after, elapsed) = fabricated_append(ds, next_id).await;
        ds = ds_after;
        next_id += 1;
        steady.push(elapsed);
        steady_root = root_manifest_bytes(&ds).await;
    }
    let (_, _, child_count_after) = metadata_footprint(dir.path());
    assert_eq!(
        child_count, child_count_after,
        "steady-state appends must not create children"
    );
    steady.sort();

    // Cold open in a fresh session.
    let open_start = Instant::now();
    let mut reader = Dataset::open(uri).await.unwrap();
    let cold_open = open_start.elapsed();
    let reader_store = reader.object_store(None).await.unwrap();
    let cold_stats = reader_store.io_stats_incremental();
    assert_eq!(
        reader.manifest.fragments.len() as u64,
        total_fragments + 4,
        "cold open must materialize every fragment"
    );

    // Warm reopen: one more append lands in the buffer, the reader checks out
    // the new version. Children are cached, so this is a root-only read.
    let (final_ds, _) = fabricated_append(ds, next_id).await;
    let warm_start = Instant::now();
    reader.checkout_latest().await.unwrap();
    let warm_reopen = warm_start.elapsed();
    let warm_stats = reader_store.io_stats_incremental();
    let final_root = root_manifest_bytes(&final_ds).await;

    println!("\n### {layout}, {total_fragments} fragments, ε = {HIGH_N_BUFFER_CAP}\n");
    println!("| metric | value |");
    println!("|--------|-------|");
    println!(
        "| first append after opt-in (migration) | {:.0} ms, root {:.2} MiB, {} children ({:.2} MiB) |",
        migration.as_secs_f64() * 1e3,
        migration_root as f64 / 1024.0 / 1024.0,
        child_count,
        child_bytes as f64 / 1024.0 / 1024.0,
    );
    println!(
        "| steady-state append (median of 3) | {:.0} ms, root PUT {:.2} MiB, 0 new children |",
        steady[1].as_secs_f64() * 1e3,
        steady_root as f64 / 1024.0 / 1024.0,
    );
    println!(
        "| cold open (fresh session) | {:.0} ms, {} GETs, {:.2} MiB read |",
        cold_open.as_secs_f64() * 1e3,
        cold_stats.read_iops,
        cold_stats.read_bytes as f64 / 1024.0 / 1024.0,
    );
    println!(
        "| warm reopen after 1 append | {:.0} ms, {} GETs, {:.2} MiB read (root is {:.2} MiB) |",
        warm_reopen.as_secs_f64() * 1e3,
        warm_stats.read_iops,
        warm_stats.read_bytes as f64 / 1024.0 / 1024.0,
        final_root as f64 / 1024.0 / 1024.0,
    );
}

#[tokio::main]
async fn main() {
    let mut jack_appends: usize = 10_000;
    let mut high_n: u64 = 150_000;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--jack" => jack_appends = args.next().unwrap().parse().unwrap(),
            "--high-n" => high_n = args.next().unwrap().parse().unwrap(),
            other => panic!("unknown argument {other}"),
        }
    }

    println!("## Scenario B: high fragment count (metadata-only commits)");
    scenario_b_one_layout(high_n, false).await;
    scenario_b_one_layout(high_n, true).await;

    println!("\n## Scenario A: Jack-shaped, {jack_appends} real single-row appends\n");
    println!(
        "| layout | appends | commit p50 ms | commit p99 ms | total s | open ms | active metadata KiB | root MiB | children |"
    );
    println!(
        "|--------|---------|---------------|---------------|---------|---------|---------------------|----------|----------|"
    );
    scenario_a_one_layout(jack_appends, false).await;
    scenario_a_one_layout(jack_appends, true).await;
}
