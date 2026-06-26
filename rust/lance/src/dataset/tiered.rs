// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Tiered manifest sealing on commit and materialization on open (#5947).

use object_store::path::Path;

use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_table::format::{
    DEFAULT_MANIFEST_BUFFER_CAP, MANIFEST_BUFFER_CAP_KEY, MANIFEST_LAYOUT_KEY,
    MANIFEST_LAYOUT_TIERED, Manifest, child_path, seal_run, spilled_rows,
};
use lance_table::io::manifest::{
    child_full_path, dataset_root_of_manifest, read_fragment_manifest, verify_child_fragment_count,
    write_fragment_manifest_file,
};

use crate::session::caches::{ChildManifestKey, DSMetadataCache};

/// Spill buffer overflow into `_manifest_children/` when tiered layout is configured.
///
/// Pure appends carry existing children forward. Other operations re-seal from
/// scratch. No-op for flat layout.
pub async fn seal_into_tiered(
    object_store: &ObjectStore,
    base_path: &Path,
    manifest: &mut Manifest,
    is_pure_append: bool,
) -> Result<()> {
    let Some(buffer_cap) = tiered_buffer_cap(manifest) else {
        return Ok(());
    };

    let fragments = manifest.fragments.clone();
    let mut children = if is_pure_append {
        manifest.child_manifests.clone()
    } else {
        Vec::new()
    };

    let mut buffer_start: usize = children.iter().map(|c| c.fragment_count as usize).sum();
    let version = manifest.version;

    while fragments.len() - buffer_start > buffer_cap {
        let run = &fragments[buffer_start..buffer_start + buffer_cap];
        let first = run.first().map_or(0, |f| f.id);
        let last = run.last().map_or(0, |f| f.id);
        let mut reference = seal_run(
            run,
            child_path(version, first, last),
            spilled_rows(&children),
        );
        let full = child_full_path(base_path, &reference.path);
        let written = write_fragment_manifest_file(object_store, &full, run).await?;
        reference.byte_size = written.size as u64;
        children.push(reference);
        buffer_start += buffer_cap;
    }

    manifest.child_manifests = children;
    Ok(())
}

pub async fn materialize_tiered_cached(
    object_store: &ObjectStore,
    manifest_path: &Path,
    manifest: &mut Manifest,
    cache: &DSMetadataCache,
) -> Result<()> {
    let root = dataset_root_of_manifest(manifest_path);
    materialize_tiered_at_root(object_store, &root, manifest, cache).await
}

pub async fn materialize_tiered_at_root(
    object_store: &ObjectStore,
    root: &Path,
    manifest: &mut Manifest,
    cache: &DSMetadataCache,
) -> Result<()> {
    if manifest.child_manifests.is_empty() {
        return Ok(());
    }
    let children = manifest.child_manifests.clone();

    let loads = children.iter().map(|child| {
        let full = child_full_path(root, &child.path);
        async move {
            let load_path = full.clone();
            let fragments = cache
                .get_or_insert_with_key(ChildManifestKey { path: &child.path }, || async move {
                    read_fragment_manifest(object_store, &load_path, child.size_hint()).await
                })
                .await?;
            verify_child_fragment_count(&full, child, fragments.len())?;
            Ok::<_, Error>(fragments)
        }
    });
    let runs = futures::future::try_join_all(loads).await?;

    let sealed: usize = children.iter().map(|c| c.fragment_count as usize).sum();
    let mut all = Vec::with_capacity(sealed + manifest.fragments.len());
    for run in &runs {
        all.extend(run.iter().cloned());
    }
    all.extend(manifest.fragments.iter().cloned());
    manifest.set_materialized_fragments(all);
    Ok(())
}

fn tiered_buffer_cap(manifest: &Manifest) -> Option<usize> {
    if manifest.config.get(MANIFEST_LAYOUT_KEY).map(String::as_str) != Some(MANIFEST_LAYOUT_TIERED)
    {
        return None;
    }
    let cap = manifest
        .config
        .get(MANIFEST_BUFFER_CAP_KEY)
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|c| *c >= 1)
        .unwrap_or(DEFAULT_MANIFEST_BUFFER_CAP);
    Some(cap)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lance_table::feature_flags::FLAG_TIERED_MANIFEST;
    use lance_table::format::{
        MANIFEST_BUFFER_CAP_KEY, MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED,
    };
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};

    use crate::Dataset;
    use crate::dataset::builder::DatasetBuilder;
    use crate::dataset::optimize::compact_files;
    use crate::dataset::{WriteMode, WriteParams};

    fn rows(n: i32) -> impl arrow_array::RecordBatchReader + Send + 'static {
        BatchGenerator::new()
            .col(Box::new(IncrementingInt32::new().named("id".to_owned())))
            .batch(n)
    }

    fn one_row() -> impl arrow_array::RecordBatchReader + Send + 'static {
        rows(1)
    }

    async fn append_rows(ds: Dataset, n: i32) -> Dataset {
        Dataset::write(
            rows(n),
            Arc::new(ds),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    async fn append(ds: Dataset) -> Dataset {
        append_rows(ds, 1).await
    }

    async fn tiered_table(uri: &str, fragments: usize, rows_per: i32, cap: &str) -> Dataset {
        let mut ds = Dataset::write(
            rows(rows_per),
            uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        ds.update_config([
            (MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED),
            (MANIFEST_BUFFER_CAP_KEY, cap),
        ])
        .await
        .unwrap();
        for _ in 1..fragments {
            ds = append_rows(ds, rows_per).await;
        }
        ds
    }

    fn writes_under(stats: &lance_io::utils::tracking_store::IoStats, prefix: &str) -> usize {
        stats
            .requests
            .iter()
            .filter(|r| r.method.starts_with("put") && r.path.as_ref().contains(prefix))
            .count()
    }

    fn reads_under(stats: &lance_io::utils::tracking_store::IoStats, prefix: &str) -> usize {
        stats
            .requests
            .iter()
            .filter(|r| !r.method.starts_with("put") && r.path.as_ref().contains(prefix))
            .count()
    }

    #[tokio::test]
    async fn append_seals_children_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let mut ds = Dataset::write(
            one_row(),
            uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        ds.update_config([
            (MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED),
            (MANIFEST_BUFFER_CAP_KEY, "3"),
        ])
        .await
        .unwrap();

        assert!(!ds.manifest.is_tiered());

        for _ in 0..9 {
            ds = append(ds).await;
        }

        assert_eq!(ds.count_rows(None).await.unwrap(), 10);
        assert!(ds.manifest.is_tiered());
        assert_eq!(ds.manifest.child_manifests.len(), 3);
        assert_eq!(ds.manifest.fragments.len(), 10);

        let reopened = Dataset::open(uri).await.unwrap();
        assert!(reopened.manifest.is_tiered());
        assert_eq!(reopened.manifest.fragments.len(), 10);
        assert_eq!(reopened.count_rows(None).await.unwrap(), 10);
        assert_eq!(
            reopened.manifest.child_manifests,
            ds.manifest.child_manifests
        );
        assert_ne!(
            reopened.manifest.reader_feature_flags & FLAG_TIERED_MANIFEST,
            0
        );
    }

    #[tokio::test]
    async fn flat_to_tiered_migration_preserves_fragments() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let mut ds = Dataset::write(
            one_row(),
            uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        for _ in 0..4 {
            ds = append(ds).await;
        }
        assert!(!ds.manifest.is_tiered());
        assert_eq!(ds.manifest.fragments.len(), 5);

        ds.update_config([
            (MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED),
            (MANIFEST_BUFFER_CAP_KEY, "2"),
        ])
        .await
        .unwrap();
        ds = append(ds).await;

        assert!(ds.manifest.is_tiered());
        assert_eq!(ds.count_rows(None).await.unwrap(), 6);

        let reopened = Dataset::open(uri).await.unwrap();
        let ids: Vec<u64> = reopened.manifest.fragments.iter().map(|f| f.id).collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn delete_on_tiered_reseal_preserves_deletions() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let mut ds = tiered_table(uri, 6, 2, "2").await;
        assert_eq!(ds.count_rows(None).await.unwrap(), 12);
        assert!(ds.manifest.is_tiered());

        ds.delete("id = 0").await.unwrap();
        assert_eq!(ds.count_rows(None).await.unwrap(), 6);
        assert!(ds.manifest.is_tiered());

        let reopened = Dataset::open(uri).await.unwrap();
        assert_eq!(reopened.count_rows(None).await.unwrap(), 6);
        assert!(reopened.manifest.is_tiered());
    }

    #[tokio::test]
    async fn appends_seal_children_immutably() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let children_dir = dir.path().join("_manifest_children");

        let count_children = || {
            std::fs::read_dir(&children_dir)
                .map(|rd| rd.count())
                .unwrap_or(0)
        };

        let mut ds = tiered_table(uri, 3, 1, "3").await;
        assert_eq!(count_children(), 0);
        assert!(!ds.manifest.is_tiered());

        ds = append(ds).await;
        assert_eq!(count_children(), 1);
        let child = std::fs::read_dir(&children_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let sealed_len = std::fs::metadata(&child).unwrap().len();
        let sealed_bytes = std::fs::read(&child).unwrap();

        ds = append(ds).await;
        ds = append(ds).await;
        assert_eq!(count_children(), 1);
        assert_eq!(std::fs::metadata(&child).unwrap().len(), sealed_len);
        assert_eq!(std::fs::read(&child).unwrap(), sealed_bytes);

        ds = append(ds).await;
        assert_eq!(count_children(), 2);
        assert_eq!(ds.count_rows(None).await.unwrap(), 7);
    }

    #[tokio::test]
    async fn append_under_cap_writes_no_child_paths() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let ds = tiered_table(uri, 2, 1, "1000").await;
        let _ = ds.object_store.io_stats_incremental();

        let mut ds = ds;
        for _ in 0..5 {
            ds = append(ds).await;
        }

        let stats = ds.object_store.io_stats_incremental();
        assert_eq!(writes_under(&stats, "_manifest_children"), 0);
        assert_eq!(writes_under(&stats, "_versions"), 5);
    }

    #[tokio::test]
    async fn overflow_seals_one_child_then_appends_write_root_only() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let ds = tiered_table(uri, 3, 1, "3").await;
        let _ = ds.object_store.io_stats_incremental();

        let ds = append(ds).await;
        let overflow = ds.object_store.io_stats_incremental();
        assert_eq!(writes_under(&overflow, "_manifest_children"), 1);

        let ds = append(ds).await;
        let ds = append(ds).await;
        let after = ds.object_store.io_stats_incremental();
        assert_eq!(writes_under(&after, "_manifest_children"), 0);
        assert_eq!(ds.count_rows(None).await.unwrap(), 6);
    }

    #[tokio::test]
    async fn cached_reopen_reads_root_only() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let ds = tiered_table(uri, 7, 1, "2").await;
        assert_eq!(ds.manifest.child_manifests.len(), 3);

        let mut reader = Dataset::open(uri).await.unwrap();
        let cold = reader.object_store.io_stats_incremental();
        assert_eq!(reads_under(&cold, "_manifest_children"), 3);

        let _ds = append(ds).await;

        reader.checkout_latest().await.unwrap();
        let warm = reader.object_store.io_stats_incremental();
        assert_eq!(reads_under(&warm, "_manifest_children"), 0);
        assert_eq!(reader.count_rows(None).await.unwrap(), 8);
    }

    #[tokio::test]
    async fn tiered_dataset_matches_flat_replay() {
        fn next(state: &mut u64) -> u64 {
            *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn fragment_shape(ds: &Dataset) -> Vec<(u64, Option<usize>)> {
            ds.manifest
                .fragments
                .iter()
                .map(|f| (f.id, f.num_rows()))
                .collect()
        }

        for seed in 0..4u64 {
            let mut rng = 0xBE_5947 ^ seed;
            let buffer_cap = 1 + next(&mut rng) % 5;
            let appends = 8 + next(&mut rng) % 7;

            let flat_dir = tempfile::tempdir().unwrap();
            let tiered_dir = tempfile::tempdir().unwrap();
            let flat_uri = flat_dir.path().to_str().unwrap();
            let tiered_uri = tiered_dir.path().to_str().unwrap();

            let first_rows = (1 + next(&mut rng) % 8) as i32;
            let mut flat = Dataset::write(
                rows(first_rows),
                flat_uri,
                Some(WriteParams {
                    mode: WriteMode::Create,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
            let mut tiered = Dataset::write(
                rows(first_rows),
                tiered_uri,
                Some(WriteParams {
                    mode: WriteMode::Create,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
            tiered
                .update_config([
                    (MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED),
                    (MANIFEST_BUFFER_CAP_KEY, buffer_cap.to_string().as_str()),
                ])
                .await
                .unwrap();

            for step in 0..appends {
                let num_rows = (1 + next(&mut rng) % 8) as i32;
                flat = append_rows(flat, num_rows).await;
                tiered = append_rows(tiered, num_rows).await;
                assert_eq!(
                    fragment_shape(&tiered),
                    fragment_shape(&flat),
                    "fragment mismatch (seed {seed}, cap {buffer_cap}, step {step})"
                );
            }

            let reopened = Dataset::open(tiered_uri).await.unwrap();
            assert_eq!(fragment_shape(&reopened), fragment_shape(&flat));
            assert_eq!(
                reopened.count_rows(None).await.unwrap(),
                flat.count_rows(None).await.unwrap(),
                "row count mismatch after reopen (seed {seed}, cap {buffer_cap})"
            );
        }
    }

    #[tokio::test]
    async fn compact_on_tiered_preserves_rows() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let mut ds = tiered_table(uri, 6, 2, "2").await;
        assert_eq!(ds.count_rows(None).await.unwrap(), 12);
        let fragments_before = ds.get_fragments().len();

        compact_files(&mut ds, Default::default(), None)
            .await
            .unwrap();

        assert!(ds.get_fragments().len() < fragments_before);
        assert_eq!(ds.count_rows(None).await.unwrap(), 12);

        let reopened = Dataset::open(uri).await.unwrap();
        assert_eq!(reopened.count_rows(None).await.unwrap(), 12);
    }

    #[tokio::test]
    async fn serialized_tiered_manifest_materializes_via_builder() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let ds = tiered_table(uri, 6, 1, "2").await;
        assert!(ds.manifest.is_tiered());
        let total = ds.count_rows(None).await.unwrap();

        let serialized = ds.manifest.serialized();
        let rebuilt = DatasetBuilder::from_uri(uri)
            .with_serialized_manifest(&serialized)
            .unwrap()
            .load()
            .await
            .unwrap();

        assert_eq!(
            rebuilt.manifest.fragments.len(),
            ds.manifest.fragments.len()
        );
        assert_eq!(rebuilt.count_rows(None).await.unwrap(), total);
    }

    #[tokio::test]
    async fn shallow_clone_of_tiered_is_readable() {
        let src_dir = tempfile::tempdir().unwrap();
        let src_uri = src_dir.path().to_str().unwrap();
        let mut ds = tiered_table(src_uri, 6, 1, "2").await;
        assert!(ds.manifest.is_tiered());
        let total = ds.count_rows(None).await.unwrap();

        let dst_dir = tempfile::tempdir().unwrap();
        let dst_uri = dst_dir.path().to_str().unwrap();
        let version = ds.manifest.version;
        let clone = ds.shallow_clone(dst_uri, version, None).await.unwrap();

        assert_eq!(clone.count_rows(None).await.unwrap(), total);

        let reopened = Dataset::open(dst_uri).await.unwrap();
        assert_eq!(reopened.count_rows(None).await.unwrap(), total);
    }
}
