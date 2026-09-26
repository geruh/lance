// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::publication::{descriptor, read_version};
use super::test_support::{commit_target_for_uri, open_tree};
use super::*;
use crate::dataset::rowids::{
    INLINE_ROW_LINEAGE_MAX_BYTES_CONFIG_KEY, RowLineage, RowVersionKind,
    SPILL_ROW_LINEAGE_CONFIG_KEY, load_row_id_sequence, load_row_version_sequence,
    place_row_lineage, read_spilled_versions,
};
use crate::dataset::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use arrow_array::record_batch;
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance_core::datatypes::Schema;
use lance_file::version::ConcreteFileVersion;
use lance_io::object_store::{ObjectStoreParams, ObjectStoreRegistry, WrappingObjectStore};
use lance_io::utils::failpoint::{FailOn, FailWhen, Failpoint, FailpointController};
use lance_io::utils::tracking_store::{IOTracker, IoRequestRecord};
use lance_table::feature_flags::{FLAG_FRAGMENT_TREE, FLAG_UNSTABLE_SPILLED_ROW_LINEAGE};
use lance_table::format::{
    BasePath, DataFile, Fragment, ROW_CREATED_AT_VERSION_FIELD_ID, ROW_ID_FIELD_ID,
    ROW_LAST_UPDATED_AT_VERSION_FIELD_ID, RowDatasetVersionMeta, RowDatasetVersionSequence,
    RowIdMeta,
};
use lance_table::fragment_metadata::support::{
    data_file_path, make_fragment, make_replacement_data_file,
};
use lance_table::fragment_metadata::{MANIFEST_LAYOUT_KEY, SnapshotPolicy};
use lance_table::io::commit::{
    CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestLocation, ManifestWriter,
};
use object_store::ObjectStoreExt;
use prost::Message;
use rstest::rstest;
use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn fixture(bulk: bool) -> (CommitTarget, FailpointController, Dataset) {
    fixture_with_budgets(bulk, 65536, 32768, 512).await
}

async fn fixture_with_budgets(
    bulk: bool,
    node_bytes: u64,
    leaf_bytes: u64,
    fragment_count: u64,
) -> (CommitTarget, FailpointController, Dataset) {
    let failpoints = FailpointController::default();
    let params = ObjectStoreParams {
        object_store_wrapper: Some(Arc::new(failpoints.clone())),
        ..Default::default()
    };
    let (object_store, base_path) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        "memory://",
        &params,
    )
    .await
    .unwrap();
    let target = CommitTarget {
        context: None,
        materialize_fragments: false,
        object_store,
        base_path,
        uri: "memory://".to_string(),
        session: Arc::new(Session::default()),
        commit_handler: Arc::new(ConditionalPutCommitHandler),
    };
    let config = HashMap::from([
        (MANIFEST_LAYOUT_KEY.to_string(), "tree".to_string()),
        (MAX_NODE_BYTES_KEY.to_string(), node_bytes.to_string()),
        (MAX_LEAF_BYTES_KEY.to_string(), leaf_bytes.to_string()),
        (
            publication::INLINE_ROOT_BYTES_KEY.to_string(),
            "0".to_string(),
        ),
        (
            publication::SUFFIX_BYTES_KEY.to_string(),
            "2048".to_string(),
        ),
        (
            "lance.fragment_metadata.materialization".to_string(),
            if bulk { "bulk" } else { "buffered" }.to_string(),
        ),
    ]);
    let transaction = Transaction::new_from_version(
        0,
        Operation::Overwrite {
            schema: differential::table_schema(),
            fragments: (0..fragment_count).map(make_fragment).collect(),
            config_upsert_values: Some(config),
            initial_bases: None,
        },
    );
    let dataset = execute_create(
        target.clone(),
        &transaction,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();
    (target, failpoints, dataset)
}

async fn assert_transaction_files_match_versions(target: &CommitTarget, latest: u64) {
    let mut expected = Vec::new();
    for version in 1..=latest {
        let (_, manifest) = open_tree(target, Some(version)).await.unwrap();
        expected.push(manifest.transaction_file.unwrap());
    }
    let mut actual: Vec<_> = target
        .object_store
        .list(Some(target.base_path.clone().join("_transactions")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|object| object.location.filename().unwrap().to_string())
        .collect();
    expected.sort();
    actual.sort();
    assert_eq!(actual, expected);
}

fn replacement(version: u64) -> Transaction {
    Transaction::new_from_version(
        version,
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(
                7,
                make_replacement_data_file(7, version.try_into().unwrap()),
            )],
        },
    )
}

#[tokio::test]
async fn deletion_only_changes_buffer_deletion_file_actions() {
    use lance_table::format::pb::fragment_action::Action;
    let (mut target, _, original) = fixture(false).await;
    target.base_path = target.base_path.join("deletion");
    target.uri = "memory:///deletion".to_string();
    let fragments: Vec<_> = (0..64)
        .map(|id| {
            let mut fragment = make_fragment(id);
            fragment.physical_rows = Some(10);
            fragment
        })
        .collect();
    let created = execute_create(
        target.clone(),
        &Transaction::new_from_version(
            0,
            Operation::Overwrite {
                schema: differential::table_schema(),
                fragments: fragments.clone(),
                config_upsert_values: Some(original.manifest.config.clone()),
                initial_bases: None,
            },
        ),
        &Default::default(),
    )
    .await
    .unwrap();
    let mut expected = fragments[7].clone();
    expected.deletion_file = Some(differential::deletion_file(1, created.manifest.version));
    let committed = execute_commit(
        target.clone(),
        &CommitConfig::default(),
        &Transaction::new_from_version(
            created.manifest.version,
            Operation::Delete {
                updated_fragments: vec![expected.clone()],
                deleted_fragment_ids: Vec::new(),
                predicate: "id = 1".to_string(),
            },
        ),
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();

    let buffered: Vec<_> = committed
        .manifest
        .fragment_tree
        .as_ref()
        .unwrap()
        .mutations_since_root
        .iter()
        .filter_map(|mutation| mutation.action.as_ref()?.action.as_ref())
        .collect();
    assert!(
        matches!(buffered.as_slice(), [Action::AddDeletionFile(file)] if file.frag_id == 7),
        "{buffered:?}"
    );
    let (tree, _) = open_tree(&target, None).await.unwrap();
    let fragments = tree.materialize().await.unwrap();
    assert_eq!(fragments[7], expected);
}

#[tokio::test]
async fn hydration_overlaps_leaf_reads_and_keeps_state_on_failure() {
    let (mut target, failpoints, original) = fixture(false).await;
    target.base_path = target.base_path.join("hydration");
    target.uri = "memory:///hydration".to_string();
    let io = IOTracker::default();
    let mut store = target.object_store.as_ref().clone();
    store.apply_wrapper(&io);
    target.object_store = Arc::new(store);
    let mut config = original.manifest.config.clone();
    config.insert(MAX_LEAF_BYTES_KEY.to_string(), "4096".to_string());
    let expected: Vec<_> = (0..128).map(make_fragment).collect();
    execute_create(
        target.clone(),
        &Transaction::new_from_version(
            0,
            Operation::Overwrite {
                schema: differential::table_schema(),
                fragments: expected.clone(),
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ),
        &Default::default(),
    )
    .await
    .unwrap();
    let mut dataset = publication::open_dataset(&target, Some(1)).await.unwrap();
    // Leaves sit under interior nodes once the fanout cap splits the root,
    // so count them through the shape report rather than the root's children.
    let leaves = dataset
        .lazy_fragments
        .as_ref()
        .unwrap()
        .tree()
        .shape_report()
        .await
        .unwrap()
        .leaf_object_bytes
        .len();
    assert!(leaves > 1);
    let manifest = dataset.manifest.clone();
    let bitmap = dataset.fragment_bitmap.clone();
    let leaf = target
        .object_store
        .inner
        .list(Some(&target.base_path.join("_bt").join("leaf")))
        .try_next()
        .await
        .unwrap()
        .unwrap()
        .location;
    let bytes = target
        .object_store
        .inner
        .get(&leaf)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    target.object_store.inner.delete(&leaf).await.unwrap();
    let error = dataset
        .hydrate_fragments_for_maintenance()
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::IO { .. } | Error::NotFound { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains(leaf.as_ref()), "{error}");
    assert!(Arc::ptr_eq(&dataset.manifest, &manifest));
    assert!(Arc::ptr_eq(&dataset.fragment_bitmap, &bitmap));
    assert!(dataset.lazy_fragments.is_some());
    target
        .object_store
        .inner
        .put(&leaf, bytes.into())
        .await
        .unwrap();
    failpoints.set_get_latency(std::time::Duration::from_millis(2));
    io.incremental_stats();
    dataset.hydrate_fragments_for_maintenance().await.unwrap();
    assert_eq!(dataset.manifest.fragments.as_ref(), &expected);
    assert!(dataset.lazy_fragments.is_none());
    let measured = io.incremental_stats();
    assert!(measured.num_stages < measured.read_iops, "{measured:?}");
}

/// Latest-version commits reuse the cached manifest. Overwrites read retired
/// leaf headers to validate removals.
#[rstest]
#[case::append("append")]
#[case::replace("replace")]
#[case::delete("delete")]
#[case::overwrite("overwrite")]
#[case::lazy("lazy")]
#[tokio::test]
async fn retained_commit_reuses_current_metadata(#[case] operation: &str) {
    let (mut target, _, _) = fixture(false).await;
    let io = IOTracker::default();
    let mut store = target.object_store.as_ref().clone();
    store.apply_wrapper(&io);
    target.object_store = Arc::new(store);
    let mut initial = publication::open_dataset(&target, None).await.unwrap();
    initial.hydrate_fragments_for_maintenance().await.unwrap();
    // A load through DatasetBuilder keeps the manifest in the session cache.
    Dataset::get_manifest(
        &target.object_store,
        &initial.manifest_location,
        &target.uri,
        &target.session,
    )
    .await
    .unwrap();
    let initial = Arc::new(initial);
    let original = initial.manifest.fragments.clone();
    let transaction = match operation {
        "append" => Transaction::new_from_version(1, differential::production_style_append(2)),
        "delete" => Transaction::new_from_version(1, differential::delete_fragments([7, 300])),
        "overwrite" => Transaction::new_from_version(
            1,
            Operation::Overwrite {
                schema: differential::table_schema(),
                fragments: vec![make_fragment(0), make_fragment(0)],
                config_upsert_values: None,
                initial_bases: None,
            },
        ),
        "replace" | "lazy" => Transaction::new_from_version(
            1,
            Operation::DataReplacement {
                replacements: [7, 128, 300, 500]
                    .into_iter()
                    .map(|id| DataReplacementGroup(id, make_replacement_data_file(id, 1)))
                    .collect(),
            },
        ),
        _ => unreachable!(),
    };
    let builder = crate::dataset::write::CommitBuilder::new(initial.clone());
    let reads_a_manifest = |requests: &[IoRequestRecord]| {
        requests.iter().any(|request| {
            !request.method.starts_with("put") && request.path.as_ref().ends_with(".manifest")
        })
    };
    io.incremental_stats();
    let committed = if operation == "lazy" {
        let lazy = builder.execute_lazy(transaction).await.unwrap();
        let reads = io.incremental_stats();
        assert!(
            reads
                .requests
                .iter()
                .all(|request| !request.path.as_ref().contains("_bt/leaf/")),
            "{reads:?}"
        );
        assert!(!reads_a_manifest(&reads.requests), "{reads:?}");
        let dataset = lazy.into_dataset().await.unwrap();
        assert!(io.incremental_stats().read_iops > 0);
        dataset
    } else {
        let committed = builder.execute(transaction).await.unwrap();
        let reads = io.incremental_stats();
        let mut leaf_reads: Vec<_> = reads
            .requests
            .iter()
            .filter(|request| {
                request.method.starts_with("get_") && request.path.as_ref().contains("_bt/leaf/")
            })
            .map(|request| request.path.to_string())
            .collect();
        leaf_reads.sort();
        if operation == "overwrite" {
            // An overwrite retires every stored fragment. The drain reads each
            // retired leaf once, for the record headers that check its removals.
            let (historic, _) = open_tree(&target, Some(1)).await.unwrap();
            let mut leaves: Vec<_> = historic
                .node_paths()
                .await
                .unwrap()
                .into_iter()
                .filter(|path| path.starts_with("_bt/leaf/"))
                .collect();
            leaves.sort();
            assert_eq!(leaf_reads, leaves, "{reads:?}");
        } else {
            assert!(leaf_reads.is_empty(), "{reads:?}");
        }
        assert!(!reads_a_manifest(&reads.requests), "{reads:?}");
        committed
    };
    let (fresh, _) = open_tree(&target, None).await.unwrap();
    assert_eq!(
        committed.manifest.fragments.as_ref(),
        &fresh.materialize().await.unwrap()
    );
    assert_eq!(
        committed.fragment_bitmap.len(),
        committed.manifest.fragments.len() as u64
    );
    let (historic, _) = open_tree(&target, Some(1)).await.unwrap();
    assert_eq!(
        historic.materialize().await.unwrap(),
        original.as_ref().clone()
    );
    assert_eq!(initial.manifest.fragments, original);
}

/// A changed entity tag invalidates the session's cached manifest.
#[tokio::test]
async fn cached_manifest_is_read_again_after_the_stored_object_changes() {
    let (mut target, _, _) = fixture(false).await;
    let io = IOTracker::default();
    let mut store = target.object_store.as_ref().clone();
    store.apply_wrapper(&io);
    target.object_store = Arc::new(store);
    let mut initial = publication::open_dataset(&target, None).await.unwrap();
    initial.hydrate_fragments_for_maintenance().await.unwrap();
    Dataset::get_manifest(
        &target.object_store,
        &initial.manifest_location,
        &target.uri,
        &target.session,
    )
    .await
    .unwrap();
    let initial = Arc::new(initial);
    let path = initial.manifest_location.path.clone();
    let stored = target.object_store.inner.get(&path).await.unwrap();
    let bytes = stored.bytes().await.unwrap();
    target
        .object_store
        .inner
        .put(&path, bytes.into())
        .await
        .unwrap();

    io.incremental_stats();
    let committed = crate::dataset::write::CommitBuilder::new(initial.clone())
        .execute(replacement(1))
        .await
        .unwrap();
    let reads = io.incremental_stats();
    assert!(
        reads
            .requests
            .iter()
            .any(|request| { request.method.starts_with("get") && request.path == path }),
        "{reads:?}"
    );
    let (fresh, _) = open_tree(&target, None).await.unwrap();
    assert_eq!(
        committed.manifest.fragments.as_ref(),
        &fresh.materialize().await.unwrap()
    );
}

#[tokio::test]
async fn retained_commit_rebases_without_reusing_stale_metadata() {
    let (mut target, _, _) = fixture(false).await;
    let io = IOTracker::default();
    let mut store = target.object_store.as_ref().clone();
    store.apply_wrapper(&io);
    target.object_store = Arc::new(store);
    let mut original = publication::open_dataset(&target, None).await.unwrap();
    original.hydrate_fragments_for_maintenance().await.unwrap();
    let original = Arc::new(original);
    let other = crate::dataset::write::CommitBuilder::new(original.clone())
        .execute(Transaction::new_from_version(
            1,
            differential::production_style_append(2),
        ))
        .await
        .unwrap();
    io.incremental_stats();
    let committed = crate::dataset::write::CommitBuilder::new(original.clone())
        .execute(replacement(1))
        .await
        .unwrap();
    assert_eq!(committed.version_id(), 3);
    assert_eq!(
        committed.manifest.fragments.len(),
        other.manifest.fragments.len()
    );
    assert_eq!(
        committed.manifest.fragments.last(),
        other.manifest.fragments.last()
    );
    let reads = io.incremental_stats();
    assert!(
        reads.requests.iter().any(|request| {
            request.method.starts_with("get_") && request.path.as_ref().contains("_bt/leaf/")
        }),
        "{reads:?}"
    );
    let (fresh, _) = open_tree(&target, None).await.unwrap();
    assert_eq!(
        committed.manifest.fragments.as_ref(),
        &fresh.materialize().await.unwrap()
    );
    assert_eq!(original.version_id(), 1);
}

#[rstest]
#[case::descriptor("descriptor")]
#[case::base("base")]
#[case::object_store("object_store")]
#[tokio::test]
async fn retained_metadata_requires_matching_identity(#[case] mismatch: &str) {
    let (mut target, _, _) = fixture(false).await;
    let io = IOTracker::default();
    let mut store = target.object_store.as_ref().clone();
    store.apply_wrapper(&io);
    target.object_store = Arc::new(store);
    let mut context = publication::open_dataset(&target, None).await.unwrap();
    context.hydrate_fragments_for_maintenance().await.unwrap();
    match mismatch {
        "descriptor" => Arc::make_mut(&mut context.manifest).fragment_tree = None,
        "base" => context.base = Path::from("another_dataset"),
        "object_store" => context.object_store = Arc::new(ObjectStore::memory()),
        _ => unreachable!(),
    }
    target.context = Some(Arc::new(context));
    target.materialize_fragments = true;
    io.incremental_stats();
    let mut committed = execute_commit(
        target.clone(),
        &Default::default(),
        &replacement(1),
        None,
        &Default::default(),
    )
    .await
    .unwrap();
    committed.hydrate_fragments_for_maintenance().await.unwrap();
    let measured = io.incremental_stats();
    assert!(
        measured.requests.iter().any(|request| {
            request.method.starts_with("get_") && request.path.as_ref().contains("_bt/leaf/")
        }),
        "{measured:?}"
    );
    let (fresh, _) = open_tree(&target, None).await.unwrap();
    assert_eq!(
        committed.manifest.fragments.as_ref(),
        &fresh.materialize().await.unwrap()
    );
}

#[derive(Debug)]
struct ConcurrentPublication {
    barrier: tokio::sync::Barrier,
    attempts: AtomicUsize,
}

#[async_trait::async_trait]
impl CommitHandler for ConcurrentPublication {
    async fn commit(
        &self,
        manifest: &mut lance_table::format::Manifest,
        indices: Option<Vec<lance_table::format::IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<lance_table::format::Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if manifest.version == 2 {
            self.barrier.wait().await;
        }
        ConditionalPutCommitHandler
            .commit(
                manifest,
                indices,
                base_path,
                object_store,
                manifest_writer,
                naming_scheme,
                transaction,
            )
            .await
    }
}

#[rstest]
#[case::append_and_replace(false)]
#[case::deep_disjoint_replacements(true)]
#[tokio::test]
async fn retained_commit_retries_after_concurrent_publication(#[case] deep: bool) {
    let (mut target, _, _) = if deep {
        fixture_with_budgets(false, 512, 1024, 128).await
    } else {
        fixture(false).await
    };
    let handler = Arc::new(ConcurrentPublication {
        barrier: tokio::sync::Barrier::new(2),
        attempts: AtomicUsize::new(0),
    });
    target.commit_handler = handler.clone();
    let io = IOTracker::default();
    let mut store = target.object_store.as_ref().clone();
    store.apply_wrapper(&io);
    target.object_store = Arc::new(store);
    let mut initial = publication::open_dataset(&target, None).await.unwrap();
    if deep {
        assert!(initial.lazy_fragments.as_ref().unwrap().tree().height() > 1);
    }
    initial.hydrate_fragments_for_maintenance().await.unwrap();
    let initial = Arc::new(initial);
    let original = initial.manifest.fragments.clone();
    let first_operation = if deep {
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(
                100,
                make_replacement_data_file(100, 1),
            )],
        }
    } else {
        differential::production_style_append(2)
    };
    let mut expected = original.as_ref().clone();
    if let Operation::Append { fragments } = &first_operation {
        let next_id = expected.len() as u64;
        expected.extend(
            fragments
                .iter()
                .cloned()
                .enumerate()
                .map(|(offset, mut fragment)| {
                    fragment.id = next_id + offset as u64;
                    fragment
                }),
        );
    } else {
        expected[100].files[0] = make_replacement_data_file(100, 1);
    }
    expected[7].files[0] = make_replacement_data_file(7, 1);
    let first = crate::dataset::write::CommitBuilder::new(initial.clone())
        .with_skip_auto_cleanup(true)
        .execute(Transaction::new_from_version(1, first_operation));
    let replace = crate::dataset::write::CommitBuilder::new(initial.clone())
        .with_skip_auto_cleanup(true)
        .execute(replacement(1));
    io.incremental_stats();
    let (first, replace) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(first, replace)
    })
    .await
    .unwrap();
    let measured = io.incremental_stats();
    let first = first.unwrap();
    let replace = replace.unwrap();
    assert_ne!(first.version_id(), replace.version_id());
    assert_eq!(handler.attempts.load(Ordering::Relaxed), 3);
    assert_eq!(
        measured
            .requests
            .iter()
            .filter(|request| {
                request.method == "put_opts"
                    && request.path.as_ref().contains("_versions/")
                    && request.path.as_ref().ends_with(".manifest")
            })
            .count(),
        3,
        "include the losing publication attempt in I/O accounting: {measured:?}"
    );
    assert!(
        measured.read_bytes > 0 && measured.written_bytes > 0,
        "{measured:?}"
    );
    for committed in [&first, &replace] {
        let (fresh, _) = open_tree(&target, Some(committed.version_id()))
            .await
            .unwrap();
        assert_eq!(
            committed.manifest.fragments.as_ref(),
            &fresh.materialize().await.unwrap()
        );
    }
    let latest = if first.version_id() == 3 {
        first
    } else {
        replace
    };
    assert_eq!(latest.manifest.fragments.as_ref(), &expected);
    let (fresh, _) = open_tree(&target, None).await.unwrap();
    if deep {
        assert!(fresh.height() > 1);
    }
    assert_eq!(fresh.materialize().await.unwrap(), expected);
    let (historical, _) = open_tree(&target, Some(1)).await.unwrap();
    assert_eq!(historical.materialize().await.unwrap(), *original);
    assert_eq!(initial.version_id(), 1);
    assert_eq!(initial.manifest.fragments, original);
    assert_transaction_files_match_versions(&target, 3).await;
}

#[rstest]
#[case::leaf_before("_bt/leaf/", FailWhen::Before, false)]
#[case::leaf_after("_bt/leaf/", FailWhen::After, false)]
#[case::base_before("_bt/root/", FailWhen::Before, false)]
#[case::base_after("_bt/root/", FailWhen::After, false)]
#[case::manifest_before("_versions/", FailWhen::Before, false)]
#[case::manifest_response_lost("_versions/", FailWhen::After, true)]
#[tokio::test]
async fn publication_failure_preserves_snapshot_and_retry(
    #[case] path: &str,
    #[case] when: FailWhen,
    #[case] published: bool,
) {
    let (mut target, failpoints, mut initial) = fixture(true).await;
    initial.hydrate_fragments_for_maintenance().await.unwrap();
    target.context = Some(Arc::new(initial.clone()));
    target.materialize_fragments = true;
    let original = initial
        .fragment_source()
        .stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let transaction = replacement(1);
    failpoints.arm(Failpoint {
        on: FailOn::Put,
        when,
        path_contains: path.to_string(),
        nth: 1,
    });
    let result = execute_commit(
        target.clone(),
        &CommitConfig::default(),
        &transaction,
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await;
    assert!(failpoints.tripped(), "Failpoint {path} did not execute");
    failpoints.disarm();
    assert_eq!(
        result.is_ok(),
        published,
        "{path}: {:?}",
        result.as_ref().err()
    );
    let (latest, _) = open_tree(&target, None).await.unwrap();
    assert_eq!(latest.version(), if published { 2 } else { 1 });
    assert_transaction_files_match_versions(&target, latest.version()).await;
    let (historic, _) = open_tree(&target, Some(1)).await.unwrap();
    assert_eq!(historic.materialize().await.unwrap(), original);
    let committed = if published {
        result.unwrap()
    } else {
        execute_commit(
            target.clone(),
            &CommitConfig::default(),
            &transaction,
            None,
            &crate::dataset::ManifestWriteConfig::default(),
        )
        .await
        .unwrap()
    };
    assert_eq!(committed.manifest.version, 2);
    assert_eq!(
        committed.read_transaction().await.unwrap().unwrap().uuid,
        transaction.uuid
    );
    assert_ne!(
        committed
            .fragment_source()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        original
    );
    let roots = target
        .object_store
        .list(Some(target.base_path.clone().join("_bt").join("root")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(!roots.is_empty());
    for root in roots {
        let name = root.location.filename().unwrap();
        let uuid = name.strip_suffix(".root").unwrap();
        assert!(
            uuid::Uuid::parse_str(uuid).is_ok(),
            "version-addressed root: {name}"
        );
    }
}

#[tokio::test]
async fn suffix_pressure_writes_external_roots_and_historical_versions_open() {
    let (target, _, _) = fixture(false).await;
    let mut bases = std::collections::BTreeSet::new();
    let mut has_suffix = false;
    let mut states = Vec::new();
    for version in 1..=40 {
        if version > 1 {
            execute_commit(
                target.clone(),
                &CommitConfig::default(),
                &replacement(version - 1),
                None,
                &crate::dataset::ManifestWriteConfig::default(),
            )
            .await
            .unwrap();
        }
        let (tree, manifest) = open_tree(&target, Some(version)).await.unwrap();
        let snapshot = descriptor(&manifest).unwrap();
        let Some(pb::fragment_tree::Root::RootUuid(path)) = snapshot.root else {
            panic!("expected external root");
        };
        bases.insert(path);
        has_suffix |= !snapshot.mutations_since_root.is_empty();
        assert!(
            pb::FragmentTreeNode {
                children: Vec::new(),
                buffer: snapshot.mutations_since_root
            }
            .encoded_len()
                <= 2048
        );
        states.push(tree.resolve_fragment(7).await.unwrap());
    }
    assert!(has_suffix);
    // Repeated replacement can reduce to one message, so force distinct IDs
    // until the bounded cumulative suffix writes a new external root.
    for version in 41..=80 {
        let transaction = Transaction::new_from_version(
            version - 1,
            Operation::DataReplacement {
                replacements: vec![DataReplacementGroup(
                    version,
                    make_replacement_data_file(version, 1),
                )],
            },
        );
        execute_commit(
            target.clone(),
            &CommitConfig::default(),
            &transaction,
            None,
            &crate::dataset::ManifestWriteConfig::default(),
        )
        .await
        .unwrap();
        let (manifest, _) = read_version(
            &target.object_store,
            &target.base_path,
            target.commit_handler.as_ref(),
            Some(version),
        )
        .await
        .unwrap();
        if let Some(pb::fragment_tree::Root::RootUuid(path)) = descriptor(&manifest).unwrap().root {
            bases.insert(path);
        }
    }
    assert!(
        bases.len() > 1,
        "suffix pressure never wrote a second external root"
    );
    for (index, state) in states.into_iter().enumerate() {
        let (tree, _) = open_tree(&target, Some(index as u64 + 1)).await.unwrap();
        assert_eq!(tree.resolve_fragment(7).await.unwrap(), state);
    }
}

#[tokio::test]
async fn cleanup_traces_retained_tree_and_removes_abandoned_base() {
    let (target, _, initial) = fixture(true).await;
    let old_snapshot = descriptor(&initial.manifest).unwrap();
    let latest = execute_commit(
        target.clone(),
        &CommitConfig::default(),
        &replacement(1),
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();
    let orphan = target
        .base_path
        .clone()
        .join("_bt")
        .join("root")
        .join("abandoned.root");
    target
        .object_store
        .put(&orphan, b"unpublished")
        .await
        .unwrap();
    let policy = super::super::cleanup::CleanupPolicy {
        before_version: Some(2),
        delete_unverified: true,
        ..Default::default()
    };
    super::super::cleanup::cleanup_old_versions(&latest, policy)
        .await
        .unwrap();
    assert!(target.object_store.inner.head(&orphan).await.is_err());
    if let Some(pb::fragment_tree::Root::RootUuid(path)) = old_snapshot.root {
        assert!(
            target
                .object_store
                .inner
                .head(&Path::from_iter(
                    target.base_path.parts().chain(
                        Path::from(
                            lance_table::fragment_metadata::store::root_path(&path).unwrap()
                        )
                        .parts()
                    )
                ))
                .await
                .is_err()
        );
    }
    let (tree, _) = open_tree(&target, Some(2)).await.unwrap();
    assert_eq!(tree.count_fragments(), 512);
    assert!(tree.resolve_fragment(7).await.unwrap().is_some());
    assert_eq!(tree.materialize().await.unwrap().len(), 512);
    assert!(open_tree(&target, Some(1)).await.is_err());
}

#[tokio::test]
async fn opted_in_tree_grows_replays_and_shrinks_through_native_commits() {
    let (target, _, created) = fixture_with_budgets(false, 512, 1024, 1).await;
    assert_eq!(created.lazy_fragments.as_ref().unwrap().tree().height(), 1);
    let mut expected = vec![make_fragment(0)];
    let mut snapshots = vec![expected.clone()];
    let append = differential::production_style_append(127);
    let Operation::Append { fragments } = &append else {
        unreachable!()
    };
    expected.extend(
        fragments
            .iter()
            .cloned()
            .enumerate()
            .map(|(offset, mut fragment)| {
                fragment.id = 1 + offset as u64;
                fragment
            }),
    );
    execute_commit(
        target.clone(),
        &CommitConfig::default(),
        &Transaction::new_from_version(1, append),
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();
    let (grown, _) = open_tree(&target, None).await.unwrap();
    assert!(
        grown.height() >= 3,
        "append must grow nested interior nodes: height {}",
        grown.height()
    );
    assert_eq!(grown.materialize().await.unwrap(), expected);
    snapshots.push(expected.clone());

    execute_commit(
        target.clone(),
        &CommitConfig::default(),
        &replacement(2),
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();
    expected[7].files[0] = make_replacement_data_file(7, 2);
    let (buffered, _) = open_tree(&target, None).await.unwrap();
    assert!(buffered.buffered_action_keys().await.unwrap().contains(&7));
    assert_eq!(buffered.materialize().await.unwrap(), expected);
    snapshots.push(expected.clone());

    let mut deleted = execute_commit(
        target.clone(),
        &CommitConfig::default(),
        &Transaction::new_from_version(
            3,
            differential::delete_fragments((0..128).filter(|id| *id != 7)),
        ),
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();
    expected.retain(|fragment| fragment.id == 7);
    let (after_delete, _) = open_tree(&target, None).await.unwrap();
    assert_eq!(after_delete.materialize().await.unwrap(), expected);
    snapshots.push(expected.clone());

    // Small deletion buckets may stay buffered. Bulk materialization makes
    // the shrink deterministic without claiming a background drain guarantee.
    deleted
        .update_config([("lance.fragment_metadata.materialization", "bulk")])
        .await
        .unwrap();
    assert_eq!(deleted.manifest.version, 5);
    snapshots.push(expected.clone());
    execute_commit(
        target.clone(),
        &CommitConfig::default(),
        &replacement(5),
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();
    expected[0].files[0] = make_replacement_data_file(7, 5);
    snapshots.push(expected);
    let (shrunk, _) = open_tree(&target, None).await.unwrap();
    assert_eq!(shrunk.height(), 1);
    assert!(shrunk.buffered_action_keys().await.unwrap().is_empty());
    for (offset, expected) in snapshots.into_iter().enumerate() {
        let version = 1 + offset as u64;
        let (tree, _) = open_tree(&target, Some(version)).await.unwrap();
        tree.verify_watermarks().await.unwrap();
        assert_eq!(
            tree.materialize().await.unwrap(),
            expected,
            "version {version}"
        );
        let mut dataset = publication::open_dataset(&target, Some(version))
            .await
            .unwrap();
        dataset.hydrate_fragments_for_maintenance().await.unwrap();
        assert_eq!(
            dataset.manifest.fragments.as_ref(),
            &expected,
            "version {version}"
        );
    }
}

#[rstest]
#[tokio::test]
async fn legacy_deep_writer_setting_is_ignored(#[values("false", "true", "unused")] value: &str) {
    let key = "lance.fragment_metadata.allow_deep_writer";
    let target = test_support::commit_target_for_uri("memory://")
        .await
        .unwrap();
    let mut config = FragmentMetadataOptions::default()
        .into_table_config()
        .unwrap();
    assert!(!config.contains_key(key));
    config.insert(key.to_string(), value.to_string());
    config.insert(MAX_NODE_BYTES_KEY.to_string(), "512".to_string());
    config.insert(MAX_LEAF_BYTES_KEY.to_string(), "1024".to_string());
    execute_create(
        target.clone(),
        &Transaction::new_from_version(
            0,
            Operation::Overwrite {
                schema: differential::table_schema(),
                fragments: vec![make_fragment(0)],
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ),
        &Default::default(),
    )
    .await
    .unwrap();
    let original = publication::open_dataset(&target, Some(1)).await.unwrap();
    assert_eq!(original.manifest.config[key], value);
    assert_eq!(original.lazy_fragments.as_ref().unwrap().tree().height(), 1);
    execute_commit(
        target.clone(),
        &Default::default(),
        &Transaction::new_from_version(1, differential::production_style_append(127)),
        None,
        &Default::default(),
    )
    .await
    .unwrap();
    let grown = publication::open_dataset(&target, Some(2)).await.unwrap();
    assert!(grown.lazy_fragments.as_ref().unwrap().tree().height() > 1);
    execute_commit(
        target.clone(),
        &Default::default(),
        &replacement(2),
        None,
        &Default::default(),
    )
    .await
    .unwrap();
    let reopened = publication::open_dataset(&target, Some(3)).await.unwrap();
    let tree = reopened.lazy_fragments.as_ref().unwrap().tree();
    assert_eq!(reopened.manifest.config[key], value);
    assert!(tree.height() > 1);
    assert_eq!(tree.count_fragments(), 128);
    assert_eq!(
        tree.resolve_fragment(7).await.unwrap().unwrap().files[0],
        make_replacement_data_file(7, 2)
    );
    assert_eq!(original.count_fragments(), 1);
}

#[tokio::test]
async fn named_manifest_open_has_no_history_or_list_fanout() {
    let (mut target, _, _) = fixture(false).await;
    for version in 1..=12 {
        execute_commit(
            target.clone(),
            &CommitConfig::default(),
            &replacement(version),
            None,
            &crate::dataset::ManifestWriteConfig::default(),
        )
        .await
        .unwrap();
    }
    let tracker = IOTracker::default();
    let mut store = (*target.object_store).clone();
    store.inner = tracker.wrap("", store.inner);
    target.object_store = Arc::new(store);
    let (tree, _) = open_tree(&target, Some(13)).await.unwrap();
    let io = tracker.incremental_stats();
    assert!(
        io.requests
            .iter()
            .all(|request| !request.method.starts_with("list")),
        "{io:?}"
    );
    let gets: Vec<_> = io
        .requests
        .iter()
        .filter(|request| request.method.starts_with("get_"))
        .collect();
    // Conditional manifest resolution adds a HEAD, represented by get_opts
    // with zero bytes. Named-version open reads the manifest and at most one root.
    assert!(gets.len() <= 3, "{io:?}");
    assert!(
        io.requests
            .iter()
            .all(|request| !request.path.as_ref().contains("_transactions/")),
        "{io:?}"
    );
    assert!(tree.resolve_fragment(7).await.unwrap().is_some());
    let io = tracker.incremental_stats();
    assert!(
        io.read_iops <= 1,
        "point lookup issued more than one leaf GET: {io:?}"
    );
}

#[tokio::test]
async fn checkout_latest_refreshes_the_fragment_source_atomically() {
    let (target, failpoints, mut original) = fixture(false).await;
    let old = original
        .fragment_source()
        .stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let latest = execute_commit(
        target,
        &CommitConfig::default(),
        &replacement(1),
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();
    failpoints.arm(Failpoint {
        on: FailOn::Get,
        when: FailWhen::Before,
        path_contains: "_bt/root/".to_string(),
        nth: 1,
    });
    assert!(original.checkout_latest().await.is_err());
    assert!(failpoints.tripped());
    assert_eq!(original.manifest.version, 1);
    assert_eq!(
        original
            .fragment_source()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        old
    );
    failpoints.disarm();
    original.checkout_latest().await.unwrap();
    assert_eq!(original.manifest.version, 2);
    let current = original
        .fragment_source()
        .stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_ne!(current, old);
    assert_eq!(
        current,
        latest
            .fragment_source()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn indexed_local_delete_resolves_only_touched_leaf_and_retention_witness() {
    let (mut target, _, original) = fixture(false).await;
    target.base_path = target.base_path.join("indexed-local");
    target.uri = "memory:///indexed-local".to_string();
    let mut config = original.manifest.config.clone();
    config.insert(MAX_LEAF_BYTES_KEY.to_string(), "8192".to_string());
    let initial = execute_create(
        target.clone(),
        &Transaction::new_from_version(
            0,
            Operation::Overwrite {
                schema: differential::table_schema(),
                fragments: (0..4096).map(make_fragment).collect(),
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ),
        &Default::default(),
    )
    .await
    .unwrap();
    let leaves = initial
        .lazy_fragments
        .as_ref()
        .unwrap()
        .tree()
        .shape_report()
        .await
        .unwrap()
        .leaf_keys
        .len();
    assert!(leaves > 4);
    let index = lance_table::format::IndexMetadata {
        covering_fields: Vec::new(),
        uuid: uuid::Uuid::new_v4(),
        fields: vec![0],
        name: "id_idx".to_string(),
        dataset_version: 1,
        fragment_bitmap: Some((0..4096).collect()),
        index_details: Some(Arc::new(prost_types::Any {
            type_url: "type.googleapis.com/lance.index.BTreeIndexDetails".to_string(),
            value: Vec::new(),
        })),
        index_version: 0,
        created_at: None,
        base_id: None,
        files: Some(Vec::new()),
    };
    let indexed = execute_commit(
        target.clone(),
        &Default::default(),
        &Transaction::new_from_version(
            1,
            Operation::CreateIndex {
                new_indices: vec![index.clone()],
                removed_indices: Vec::new(),
            },
        ),
        None,
        &Default::default(),
    )
    .await
    .unwrap();
    let index = crate::index::load_all_indices(&indexed).await.unwrap()[0].clone();
    let io = IOTracker::default();
    let mut store = target.object_store.as_ref().clone();
    store.inner = io.wrap("", store.inner.clone());
    target.object_store = Arc::new(store);
    let deleted = execute_commit(
        target,
        &Default::default(),
        &Transaction::new_from_version(
            2,
            Operation::Delete {
                updated_fragments: Vec::new(),
                deleted_fragment_ids: vec![4095],
                predicate: "id = 4095".to_string(),
            },
        ),
        None,
        &Default::default(),
    )
    .await
    .unwrap();
    let measured = io.incremental_stats();
    let leaf_gets = measured
        .requests
        .iter()
        .filter(|request| {
            request.method.starts_with("get_") && request.path.as_ref().contains("_bt/leaf/")
        })
        .count();
    assert_eq!(
        leaf_gets, 2,
        "one touched leaf and one live index-retention witness"
    );
    assert_eq!(deleted.count_fragments(), 4095);
    assert_eq!(
        crate::index::load_all_indices(&deleted)
            .await
            .unwrap()
            .as_slice(),
        &[index]
    );
}

#[tokio::test]
async fn allocation_frontier_rejects_exhaustion_without_publication() {
    let (target, _, original) = fixture(false).await;
    let reserved = execute_commit(
        target.clone(),
        &Default::default(),
        &Transaction::new_from_version(
            1,
            Operation::ReserveFragments {
                num_fragments: u32::MAX - 511,
            },
        ),
        None,
        &Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(reserved.manifest.max_fragment_id, Some(u32::MAX));
    assert_eq!(
        reserved
            .lazy_fragments
            .as_ref()
            .unwrap()
            .tree()
            .next_fragment_id(),
        u64::from(u32::MAX) + 1
    );
    for operation in [
        Operation::Append {
            fragments: vec![make_fragment(0)],
        },
        Operation::ReserveFragments { num_fragments: 1 },
    ] {
        let error = execute_commit(
            target.clone(),
            &Default::default(),
            &Transaction::new_from_version(2, operation),
            None,
            &Default::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
        assert!(error.to_string().contains("u32 address space"));
    }
    let latest = publication::open_dataset(&target, None).await.unwrap();
    assert_eq!(latest.version().version, 2);
    assert_eq!(latest.count_fragments(), original.count_fragments());
}

#[tokio::test]
async fn restore_resolves_inherited_tree_nodes_in_the_source_store() {
    let source_uri = "shared-memory://fragment-tree-restore-source/table";
    let clone_uri = "shared-memory://fragment-tree-restore-destination/table";
    let source = crate::dataset::write::CommitBuilder::new(source_uri)
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                schema: differential::table_schema(),
                fragments: (0..3).map(make_fragment).collect(),
                config_upsert_values: Some(
                    FragmentMetadataOptions::default()
                        .into_table_config()
                        .unwrap(),
                ),
                initial_bases: None,
            },
        ))
        .await
        .unwrap();
    let cloned = crate::dataset::write::CommitBuilder::new(clone_uri)
        .with_source_store(source.object_store.clone())
        .execute(Transaction::new_from_version(
            source.version_id(),
            Operation::Clone {
                is_shallow: true,
                ref_name: None,
                ref_version: source.version_id(),
                ref_path: source_uri.to_string(),
                branch_name: None,
            },
        ))
        .await
        .unwrap();
    assert_ne!(
        source.object_store.store_prefix,
        cloned.object_store.store_prefix
    );
    let expected = cloned.manifest.fragments.clone();
    assert_eq!(expected.len(), 3);
    assert!(
        expected
            .iter()
            .all(|fragment| fragment.files[0].base_id.is_some())
    );

    let changed = crate::dataset::write::CommitBuilder::new(Arc::new(cloned))
        .execute(Transaction::new_from_version(
            1,
            Operation::Delete {
                updated_fragments: Vec::new(),
                deleted_fragment_ids: vec![1],
                predicate: "id = 1".to_string(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(changed.count_fragments(), 2);
    let mut restored = changed.checkout_version(1).await.unwrap();
    assert_eq!(restored.manifest.fragments, expected);
    restored.restore().await.unwrap();
    assert_eq!(restored.version_id(), 3);
    assert_eq!(restored.manifest.fragments, expected);
    let reopened = crate::dataset::builder::DatasetBuilder::from_uri(clone_uri)
        .load()
        .await
        .unwrap();
    assert_eq!(reopened.manifest.fragments, expected);
    assert_eq!(source.count_fragments(), 3);
    assert_eq!(source.latest_version_id().await.unwrap(), 1);
}

#[tokio::test]
async fn shallow_clone_stamps_source_tree_references() {
    let (_target, _, mut original) = fixture(false).await;
    let cloned = original
        .shallow_clone("memory:///clone", 1, None)
        .await
        .unwrap();
    let snapshot = cloned.manifest.fragment_tree.as_ref().unwrap();
    let Some(pb::fragment_tree::Root::RootUuid(root_path)) = &snapshot.root else {
        panic!("clone must own a dest root");
    };
    let root_path = lance_table::fragment_metadata::store::root_path(root_path).unwrap();
    assert!(root_path.starts_with("_bt/root/"));
    let tree = tree_from_manifest(
        cloned.object_store.clone(),
        cloned.session.store_registry(),
        cloned.base.clone(),
        &cloned.manifest,
    )
    .await
    .unwrap();
    assert!(
        tree.node_paths()
            .await
            .unwrap()
            .iter()
            .all(|path| path.starts_with("_bt/"))
    );
    let source_id = cloned
        .manifest
        .base_paths
        .iter()
        .find(|(_, base)| base.path.contains("memory:"))
        .map(|(id, _)| *id)
        .expect("clone must retain a source BasePath");
    let cloned_root = cloned
        .object_store
        .inner
        .get(&Path::from_iter(
            cloned
                .base
                .parts()
                .chain(Path::from(root_path.as_str()).parts()),
        ))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let root = pb::FragmentTreeRoot::decode(cloned_root.as_ref()).unwrap();
    assert!(
        root.children
            .iter()
            .all(|child| child.base_id == Some(source_id)),
        "{:?}",
        root.children
    );
    let fragments = tree.materialize().await.unwrap();
    assert_eq!(fragments.len(), 512);
    assert!(
        fragments
            .iter()
            .all(|fragment| fragment.files.iter().all(|file| file.base_id.is_some()))
    );
}

#[tokio::test]
async fn config_update_after_memory_clone_keeps_fragment_count() {
    let (_target, _, mut original) = fixture(false).await;
    let mut cloned = original
        .shallow_clone("memory:///clone-commit", 1, None)
        .await
        .unwrap();
    cloned
        .update_config([("app.note", "after-clone")])
        .await
        .unwrap();
    assert_eq!(cloned.count_fragments(), 512);
}

#[rstest]
#[tokio::test]
async fn structural_settings_require_rebuild_but_byte_targets_may_change(
    #[values(
        ("lance.fragment_metadata.hard_capacity_bytes", "134217728"),
        ("lance.manifest.layout", "flat")
    )]
    change: (&str, &str),
) {
    let (key, value) = change;
    let (target, _, mut dataset) = fixture(false).await;
    dataset
        .update_config([("lance.fragment_metadata.semantic_buffer_bytes", "4096")])
        .await
        .unwrap();
    assert_eq!(dataset.version().version, 2);
    let error = dataset.update_config([(key, value)]).await.unwrap_err();
    assert!(matches!(error, Error::InvalidInput { .. }));
    assert!(error.to_string().contains(key));
    assert!(error.to_string().contains("rebuild"));
    assert_eq!(dataset.version().version, 2);
    assert_eq!(
        read_version(
            &target.object_store,
            &target.base_path,
            target.commit_handler.as_ref(),
            None
        )
        .await
        .unwrap()
        .0
        .version,
        2
    );
    dataset
        .update_config([("app.note", "allowed")])
        .await
        .unwrap();
    assert_eq!(
        dataset.manifest.config.get("app.note").map(String::as_str),
        Some("allowed")
    );
}

#[test]
fn native_leaf_and_buffer_budgets_do_not_follow_directory_override() {
    let original = tree_config_from(&HashMap::new()).unwrap();
    let changed = tree_config_from(&HashMap::from([(
        MAX_NODE_BYTES_KEY.to_string(),
        "65536".to_string(),
    )]))
    .unwrap();
    assert_ne!(changed.max_node_bytes, original.max_node_bytes);
    assert_eq!(changed.max_leaf_bytes, original.max_leaf_bytes);
    assert_eq!(
        changed.semantic_buffer_bytes,
        original.semantic_buffer_bytes
    );
}

#[rstest]
#[case("lance.fragment_metadata.max_root_delta_tail", "4")]
#[case("lance.fragment_metadata.max_children_per_node", "16")]
#[case("lance.fragment_metadata.materialization", "force_flush")]
#[test]
fn invalid_metadata_settings_are_rejected(#[case] key: &str, #[case] value: &str) {
    let error = tree_config_from(&HashMap::from([(key.into(), value.into())])).unwrap_err();
    assert!(matches!(error, Error::InvalidInput { .. }));
    assert!(error.to_string().contains(key));
}

#[tokio::test]
async fn inline_history_survives_a_lost_publish_response() {
    let (target, failpoints, _) = fixture(true).await;
    let config = crate::dataset::ManifestWriteConfig::default().with_transaction_file_disabled();
    let transaction = replacement(1);
    failpoints.arm(Failpoint {
        on: FailOn::Put,
        when: FailWhen::After,
        path_contains: "_versions/".into(),
        nth: 1,
    });
    let committed = execute_commit(
        target.clone(),
        &Default::default(),
        &transaction,
        None,
        &config,
    )
    .await
    .unwrap();
    assert!(failpoints.tripped());
    assert!(committed.manifest.transaction_file.is_none());
    assert_eq!(
        committed.read_transaction().await.unwrap().unwrap().uuid,
        transaction.uuid
    );
    let history = publication::history(&target, 1, 2).await.unwrap();
    assert_eq!(history[0].1.uuid, transaction.uuid);
}

#[rstest]
#[case::shallow(false, false)]
#[case::deep(true, false)]
#[case::chained_shallow(false, true)]
#[case::chained_deep(true, true)]
#[tokio::test]
async fn clone_preserves_column_lineage_carrier_ownership(
    #[case] is_deep: bool,
    #[case] is_chained: bool,
) {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source");
    let source_uri = source_path.to_str().unwrap();
    let intermediate_path = dir.path().join("intermediate");
    let intermediate_uri = intermediate_path.to_str().unwrap();
    let clone_path = dir.path().join("clone");
    let clone_uri = clone_path.to_str().unwrap();
    let batch = record_batch!(("id", Int32, [0, 1, 2, 3])).unwrap();
    let mut config = FragmentMetadataOptions::default()
        .into_table_config()
        .unwrap();
    config.extend([
        (SPILL_ROW_LINEAGE_CONFIG_KEY.to_string(), "true".to_string()),
        (
            INLINE_ROW_LINEAGE_MAX_BYTES_CONFIG_KEY.to_string(),
            "0".to_string(),
        ),
    ]);
    let empty = execute_create(
        commit_target_for_uri(source_uri).await.unwrap(),
        &Transaction::new_from_version(
            0,
            Operation::Overwrite {
                schema: Schema::try_from(batch.schema().as_ref()).unwrap(),
                fragments: Vec::new(),
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ),
        &crate::dataset::ManifestWriteConfig {
            use_stable_row_ids: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let source = InsertBuilder::new(Arc::new(empty))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            max_rows_per_file: 2,
            max_rows_per_group: 2,
            ..Default::default()
        })
        .execute(vec![batch])
        .await
        .unwrap();
    let mut fragments = source.manifest.fragments.as_ref().clone();
    assert_eq!(fragments.len(), 2);
    for fragment in &mut fragments {
        let lineage = RowLineage {
            row_ids: load_row_id_sequence(&source, fragment)
                .await
                .unwrap()
                .as_ref()
                .clone(),
            created_at: RowDatasetVersionSequence::from_uniform_row_count(2, 1),
            last_updated_at: RowDatasetVersionSequence::from_uniform_row_count(2, 2),
        };
        place_row_lineage(&source, &lineage)
            .await
            .unwrap()
            .apply(fragment);
    }
    let transaction = Transaction::new_from_version(
        source.version_id(),
        Operation::Merge {
            fragments,
            schema: source.schema().clone(),
            preserves_nullability: true,
        },
    );
    let mut source = CommitBuilder::new(Arc::new(source))
        .execute(transaction)
        .await
        .unwrap();
    // Merge refreshes last-updated versions when it adds the carrier file.
    let last_updated_version = source.version_id();
    let last_updated_meta = RowDatasetVersionMeta::from_sequence(
        &RowDatasetVersionSequence::from_uniform_row_count(2, last_updated_version),
    )
    .unwrap();
    let expected_flags = FLAG_FRAGMENT_TREE | FLAG_UNSTABLE_SPILLED_ROW_LINEAGE;
    assert_eq!(
        source.manifest.reader_feature_flags & expected_flags,
        expected_flags
    );
    assert_eq!(
        source.manifest.writer_feature_flags & expected_flags,
        expected_flags
    );
    if is_chained {
        source = source
            .shallow_clone(intermediate_uri, source.version_id(), None)
            .await
            .unwrap();
    }
    let cloned = if is_deep {
        source
            .deep_clone(clone_uri, source.version_id(), None)
            .await
            .unwrap()
    } else {
        source
            .shallow_clone(clone_uri, source.version_id(), None)
            .await
            .unwrap()
    };
    assert!(cloned.manifest.fragment_tree.is_some());
    let cloned_version = cloned.version_id();
    let cloned_fragments = cloned.manifest.fragments.clone();
    for dataset in [&source, &cloned] {
        for fragment in dataset.manifest.fragments.iter() {
            assert_eq!(fragment.row_id_meta, Some(RowIdMeta::Column));
            assert_eq!(
                fragment.created_at_version_meta,
                Some(RowDatasetVersionMeta::Column)
            );
            assert_eq!(
                fragment.last_updated_at_version_meta,
                Some(last_updated_meta.clone())
            );
            let versions =
                load_row_version_sequence(dataset, fragment, RowVersionKind::LastUpdatedAt)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(
                versions.versions().collect::<Vec<_>>(),
                vec![last_updated_version; 2]
            );
        }
    }
    if is_deep {
        assert!(cloned.manifest.base_paths.is_empty());
        std::fs::remove_dir_all(&source_path).unwrap();
        if is_chained {
            std::fs::remove_dir_all(&intermediate_path).unwrap();
        }
    }
    let mut reopened = Dataset::open(clone_uri).await.unwrap();
    assert_eq!(
        reopened.manifest.reader_feature_flags & expected_flags,
        expected_flags
    );
    assert_eq!(
        reopened.manifest.writer_feature_flags & expected_flags,
        expected_flags
    );
    reopened
        .update_config([("app.note", "after-clone")])
        .await
        .unwrap();
    assert_eq!(
        reopened.manifest.reader_feature_flags & expected_flags,
        expected_flags
    );
    assert_eq!(
        reopened.manifest.writer_feature_flags & expected_flags,
        expected_flags
    );
    let mut restored = reopened.checkout_version(cloned_version).await.unwrap();
    restored.restore().await.unwrap();
    let reopened = Dataset::open(clone_uri).await.unwrap();
    assert_eq!(reopened.version_id(), cloned_version + 2);
    assert_eq!(reopened.manifest.fragments, cloned_fragments);
    assert_eq!(
        reopened.manifest.reader_feature_flags & expected_flags,
        expected_flags
    );
    assert_eq!(
        reopened.manifest.writer_feature_flags & expected_flags,
        expected_flags
    );
    assert_eq!(reopened.count_fragments(), 2);
    for fragment in reopened.manifest.fragments.iter() {
        assert_eq!(fragment.row_id_meta, Some(RowIdMeta::Column));
        assert_eq!(
            fragment.created_at_version_meta,
            Some(RowDatasetVersionMeta::Column)
        );
        assert_eq!(
            fragment.last_updated_at_version_meta,
            Some(last_updated_meta.clone())
        );
        let carrier = fragment.row_lineage_file(ROW_ID_FIELD_ID).unwrap().unwrap();
        for field_id in [
            ROW_CREATED_AT_VERSION_FIELD_ID,
            ROW_LAST_UPDATED_AT_VERSION_FIELD_ID,
        ] {
            assert_eq!(fragment.row_lineage_file(field_id).unwrap(), Some(carrier));
        }
        if is_deep {
            assert_eq!(carrier.base_id, None);
        } else {
            let base = &reopened.manifest.base_paths[&carrier.base_id.unwrap()];
            assert_eq!(base.path, source_uri);
            assert!(!clone_path.join("data").join(&carrier.path).exists());
        }
        let row_ids = load_row_id_sequence(&reopened, fragment).await.unwrap();
        assert_eq!(
            row_ids.iter().collect::<Vec<_>>(),
            vec![fragment.id * 2, fragment.id * 2 + 1]
        );
        for (kind, version) in [
            (RowVersionKind::CreatedAt, 1),
            (RowVersionKind::LastUpdatedAt, last_updated_version),
        ] {
            let versions = load_row_version_sequence(&reopened, fragment, kind)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(versions.versions().collect::<Vec<_>>(), vec![version; 2]);
        }
        // The shared carrier retains its original columns even when the
        // manifest's inline metadata supersedes the last-updated column.
        for (field_id, version) in [
            (ROW_CREATED_AT_VERSION_FIELD_ID, 1),
            (ROW_LAST_UPDATED_AT_VERSION_FIELD_ID, 2),
        ] {
            let versions = read_spilled_versions(&reopened, fragment, field_id)
                .await
                .unwrap();
            assert_eq!(versions.versions().collect::<Vec<_>>(), vec![version; 2]);
        }
    }
}

#[rstest]
#[case::temporarily_hidden(false)]
#[case::verification_outage(true)]
#[tokio::test]
async fn ambiguous_publication_uses_native_verification(#[case] outage: bool) {
    let (mut target, _, _) = fixture(false).await;
    let handler = Arc::new(crate::utils::test::AmbiguousCommitHandler::default());
    handler.fail_next(crate::utils::test::AmbiguousFailure::LandAndError);
    if outage {
        handler
            .fail_resolve
            .store(true, std::sync::atomic::Ordering::SeqCst);
    } else {
        handler.fail_next_resolves_with_not_found(2);
    }
    target.commit_handler = handler.clone();
    let result = execute_commit(
        target.clone(),
        &Default::default(),
        &replacement(1),
        None,
        &Default::default(),
    )
    .await;
    if outage {
        let error = result.unwrap_err();
        assert!(error.is_commit_status_unknown(), "{error}");
    } else {
        assert_eq!(result.unwrap().version_id(), 2);
    }
    handler
        .fail_resolve
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let (tree, manifest) = open_tree(&target, Some(2)).await.unwrap();
    assert_eq!(tree.version(), 2);
    assert!(manifest.transaction_file.is_some());
    assert_transaction_files_match_versions(&target, 2).await;
}

#[derive(Debug)]
struct ReleaseErrorLock;

#[async_trait::async_trait]
impl lance_table::io::commit::CommitLease for ReleaseErrorLock {
    async fn release(
        &self,
        success: bool,
    ) -> std::result::Result<(), lance_table::io::commit::CommitError> {
        if success {
            Err(lance_table::io::commit::CommitError::OtherError(Error::io(
                "lease release failed",
            )))
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl lance_table::io::commit::CommitLock for ReleaseErrorLock {
    type Lease = Self;
    async fn lock(
        &self,
        _version: u64,
    ) -> std::result::Result<Self, lance_table::io::commit::CommitError> {
        Ok(Self)
    }
}

#[tokio::test]
async fn published_commit_preserves_custom_handler_release_errors() {
    let (mut target, _, _) = fixture(false).await;
    target.commit_handler = Arc::new(ReleaseErrorLock);
    let error = execute_commit(
        target.clone(),
        &Default::default(),
        &replacement(1),
        None,
        &Default::default(),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("lease release failed"),
        "{error}"
    );
    assert_eq!(open_tree(&target, Some(2)).await.unwrap().0.version(), 2);
    assert_transaction_files_match_versions(&target, 2).await;
}

#[tokio::test]
async fn detached_creation_is_rejected_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let error = crate::dataset::write::CommitBuilder::new(uri)
        .with_detached(true)
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: vec![make_fragment(0)],
                schema: super::differential::table_schema(),
                config_upsert_values: Some(
                    FragmentMetadataOptions::default()
                        .into_table_config()
                        .unwrap(),
                ),
                initial_bases: None,
            },
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NotSupported { .. }), "{error}");
    assert!(!dir.path().join("_versions").exists());
    assert!(!dir.path().join("_bt").exists());
}

#[tokio::test]
async fn unknown_dataset_root_base_does_not_publish_a_version() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let error = crate::dataset::write::CommitBuilder::new(uri)
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: vec![make_fragment(0)],
                schema: super::differential::table_schema(),
                config_upsert_values: Some(
                    FragmentMetadataOptions::default()
                        .into_table_config()
                        .unwrap(),
                ),
                initial_bases: Some(vec![BasePath::new(
                    1,
                    "noscheme://nowhere".into(),
                    None,
                    true,
                )]),
            },
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Unknown scheme"), "{error}");
    assert!(!dir.path().join("_versions").exists());
}

#[rstest]
#[case::flat(None)]
#[case::tree(Some(FragmentMetadataOptions::default().into_table_config().unwrap()))]
#[tokio::test]
async fn dataset_root_base_resolves_through_the_session_registry(
    #[case] config: Option<HashMap<String, String>>,
) {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("table");
    let uri = uri.to_str().unwrap();
    let registry = Arc::new(ObjectStoreRegistry::default());
    registry.insert(
        "session-file",
        Arc::new(lance_io::object_store::providers::local::FileStoreProvider),
    );
    let session = Arc::new(Session::new(0, 0, registry));
    let base = format!("session-file://{}", dir.path().join("origin").display());
    let created = crate::dataset::write::CommitBuilder::new(uri)
        .with_session(session.clone())
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: vec![make_fragment(0)],
                schema: super::differential::table_schema(),
                config_upsert_values: config,
                initial_bases: Some(vec![BasePath::new(1, base, None, true)]),
            },
        ))
        .await
        .unwrap();
    crate::dataset::write::CommitBuilder::new(Arc::new(created))
        .execute(Transaction::new_from_version(
            1,
            Operation::Append {
                fragments: vec![make_fragment(1)],
            },
        ))
        .await
        .unwrap();
    let reopened = crate::dataset::builder::DatasetBuilder::from_uri(uri)
        .with_session(session)
        .load()
        .await
        .unwrap();
    assert_eq!(reopened.version().version, 2);
    assert_eq!(reopened.get_fragments().len(), 2);
}

#[tokio::test]
async fn shallow_clone_rejects_base_id_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let destination = dir.path().join("destination");
    let mut dataset = crate::dataset::write::CommitBuilder::new(source.to_str().unwrap())
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: vec![make_fragment(0)],
                schema: super::differential::table_schema(),
                config_upsert_values: Some(
                    FragmentMetadataOptions::default()
                        .into_table_config()
                        .unwrap(),
                ),
                initial_bases: Some(vec![BasePath::new(
                    u32::MAX,
                    source.to_str().unwrap().into(),
                    None,
                    true,
                )]),
            },
        ))
        .await
        .unwrap();
    let error = dataset
        .shallow_clone(destination.to_str().unwrap(), 1, None)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidInput { .. }));
    assert!(error.to_string().contains("cannot allocate clone base ID"));
    assert!(!destination.join("_versions").exists());
}

/// Columns in the wide schema. One thousand single field files fit one
/// fragment, and ten or one hundred files slice the same schema more coarsely.
const WIDE_COLUMNS: i32 = 1000;

fn wide_schema() -> Schema {
    Schema::try_from(&ArrowSchema::new(
        (0..WIDE_COLUMNS)
            .map(|column| ArrowField::new(format!("c{column}"), DataType::Int32, true))
            .collect::<Vec<_>>(),
    ))
    .unwrap()
}

/// One data file covering `fields`, with a column index per field and a real
/// Lance data path.
fn wide_file(fragment_id: u64, file_index: u64, fields: Vec<i32>) -> DataFile {
    let width = fields.len() as i32;
    DataFile::new(
        data_file_path(fragment_id, file_index),
        fields,
        (0..width).collect(),
        ConcreteFileVersion::V2_0,
        NonZero::new(4096),
        None,
    )
}

/// A fragment with one file over the first two columns, a record small enough
/// that several share one leaf once the leaf's fixed Lance file overhead is
/// paid.
fn narrow_fragment(id: u64) -> Fragment {
    let mut fragment = Fragment::new(id);
    fragment.physical_rows = Some(2048);
    fragment.files.push(wide_file(id, 0, vec![0, 1]));
    fragment
}

/// A fragment whose columns are split over `file_count` files of equal width.
/// `offset` rotates which columns land in which file, so fragments with the
/// same offset repeat one mapping and fragments with different offsets do not.
fn sliced_fragment(id: u64, file_count: i32, offset: i32) -> Fragment {
    let width = WIDE_COLUMNS / file_count;
    let mut fragment = Fragment::new(id);
    fragment.physical_rows = Some(2048);
    for file in 0..file_count {
        let fields = (0..width)
            .map(|column| (offset + file * width + column) % WIDE_COLUMNS)
            .collect();
        fragment.files.push(wide_file(id, file as u64, fields));
    }
    fragment
}

/// A wide-schema fixture with configurable tree budgets and materialization.
/// Every version uses an external root.
async fn wide_fixture(
    dir: &std::path::Path,
    tree: FragmentTreeConfig,
    materialization: Materialization,
    fragments: Vec<Fragment>,
) -> (CommitTarget, Dataset) {
    let target = commit_target_for_uri(dir.to_str().unwrap()).await.unwrap();
    let config = FragmentMetadataOptions {
        tree,
        publication: SnapshotPolicy {
            inline_root_bytes: 0,
            ..SnapshotPolicy::default()
        },
        materialization,
    }
    .into_table_config()
    .unwrap();
    let dataset = execute_create(
        target.clone(),
        &Transaction::new_from_version(
            0,
            Operation::Overwrite {
                schema: wide_schema(),
                fragments,
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ),
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap();
    (target, dataset)
}

async fn append_fragments(
    target: &CommitTarget,
    read_version: u64,
    fragments: Vec<Fragment>,
) -> Result<Dataset> {
    execute_commit(
        target.clone(),
        &CommitConfig::default(),
        &Transaction::new_from_version(read_version, Operation::Append { fragments }),
        None,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
}

/// Fragments read back record for record, files included, through a
/// hydrated open, a materialized tree and point resolution of every id.
async fn assert_fragments_read_back(target: &CommitTarget, expected: &[Fragment]) {
    let mut hydrated = publication::open_dataset(target, None).await.unwrap();
    hydrated.hydrate_fragments_for_maintenance().await.unwrap();
    assert_eq!(hydrated.manifest.fragments.as_slice(), expected);
    let (tree, _) = open_tree(target, None).await.unwrap();
    assert_eq!(tree.materialize().await.unwrap(), expected);
    for fragment in expected {
        assert_eq!(
            tree.resolve_fragment(fragment.id).await.unwrap().as_ref(),
            Some(fragment),
            "fragment {}",
            fragment.id
        );
    }
    tree.verify_reachable().await.unwrap();
}

#[tokio::test]
async fn wide_fragments_read_back_with_exact_file_mappings() {
    let dir = tempfile::tempdir().unwrap();
    let bootstrap = vec![
        sliced_fragment(0, 1, 0),
        sliced_fragment(1, 1, 0),
        sliced_fragment(2, 1, 0),
        sliced_fragment(3, 10, 0),
        sliced_fragment(4, 10, 0),
        sliced_fragment(5, 100, 0),
        sliced_fragment(6, 1000, 0),
        sliced_fragment(7, 10, 37),
    ];
    let (target, created) = wide_fixture(
        dir.path(),
        FragmentTreeConfig::default(),
        Materialization::Buffered,
        bootstrap.clone(),
    )
    .await;
    assert_fragments_read_back(&target, &bootstrap).await;

    let appended = vec![
        sliced_fragment(8, 1, 0),
        sliced_fragment(9, 10, 0),
        sliced_fragment(10, 10, 0),
        sliced_fragment(11, 100, 250),
        sliced_fragment(12, 1000, 500),
        sliced_fragment(13, 10, 0),
    ];
    append_fragments(&target, created.version_id(), appended.clone())
        .await
        .unwrap();
    let mut expected = bootstrap;
    expected.extend(appended);
    assert_fragments_read_back(&target, &expected).await;
    let (tree, _) = open_tree(&target, None).await.unwrap();
    let widest = tree.resolve_fragment(12).await.unwrap().unwrap();
    assert_eq!(widest.files.len(), 1000);
    assert_eq!(widest.files[0].fields.as_ref(), &[500]);
    assert_eq!(widest.files[999].fields.as_ref(), &[499]);
}

#[tokio::test]
async fn single_fragment_over_the_leaf_target_publishes_as_an_oversized_leaf() {
    // A leaf is a nested Lance file with a few KiB of fixed overhead, so a
    // sixteen KiB target still packs the narrow fragments several to a leaf
    // while a one thousand file fragment is indivisible and larger than any
    // leaf the policy would otherwise write.
    const LEAF_TARGET: u64 = 16 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let mut budgets = FragmentTreeConfig::default();
    budgets.max_leaf_bytes = LEAF_TARGET;
    budgets.semantic_buffer_bytes = LEAF_TARGET;
    let bootstrap: Vec<Fragment> = (0..4).map(narrow_fragment).collect();
    let (target, created) = wide_fixture(
        dir.path(),
        budgets,
        Materialization::Bulk,
        bootstrap.clone(),
    )
    .await;
    let appended = vec![
        narrow_fragment(4),
        sliced_fragment(5, 1000, 0),
        narrow_fragment(6),
    ];
    append_fragments(&target, created.version_id(), appended.clone())
        .await
        .unwrap();
    let (tree, _) = open_tree(&target, None).await.unwrap();
    let report = tree.shape_report().await.unwrap();
    let oversized: Vec<(u64, u64)> = report
        .leaf_object_bytes
        .iter()
        .zip(&report.leaf_keys)
        .filter(|(bytes, _)| **bytes > LEAF_TARGET)
        .map(|(bytes, keys)| (*bytes, *keys))
        .collect();
    assert_eq!(oversized.len(), 1, "{report:?}");
    assert_eq!(
        oversized[0].1, 1,
        "a leaf over the target holds exactly one fragment"
    );
    assert!(
        report.leaf_object_bytes.len() > 1,
        "the narrow fragments pack into leaves of their own: {report:?}"
    );
    let mut expected = bootstrap;
    expected.extend(appended);
    assert_fragments_read_back(&target, &expected).await;
}

/// An oversized append leaves the dataset unchanged and a later valid append succeeds.
#[tokio::test]
async fn fragment_past_hard_capacity_fails_before_publication_and_the_next_append_lands() {
    // Thirty two KiB hard capacity over a sixteen KiB leaf target. The one
    // thousand file fragment encodes above the capacity while the narrow
    // fragments ahead of it in the same range encode below the target.
    let dir = tempfile::tempdir().unwrap();
    let mut budgets = FragmentTreeConfig::default();
    budgets.max_node_bytes = 16 * 1024;
    budgets.max_leaf_bytes = 16 * 1024;
    budgets.semantic_buffer_bytes = 16 * 1024;
    budgets.hard_capacity_bytes = 32 * 1024;
    let bootstrap: Vec<Fragment> = (0..4).map(narrow_fragment).collect();
    let (target, created) = wide_fixture(
        dir.path(),
        budgets,
        Materialization::Bulk,
        bootstrap.clone(),
    )
    .await;
    let version = created.version_id();
    let rejected = vec![
        narrow_fragment(4),
        narrow_fragment(5),
        narrow_fragment(6),
        sliced_fragment(7, 1000, 0),
    ];
    let error = append_fragments(&target, version, rejected)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidInput { .. }), "{error:?}");
    assert!(error.to_string().contains("fragment 7"), "{error}");
    assert!(
        error
            .to_string()
            .contains("exceeding hard_capacity_bytes=32768"),
        "{error}"
    );
    // The rejected commit published nothing and the tree it restored still
    // resolves every fragment.
    let latest = publication::open_dataset(&target, None).await.unwrap();
    assert_eq!(latest.version_id(), version);
    assert_fragments_read_back(&target, &bootstrap).await;

    let accepted = vec![narrow_fragment(4), sliced_fragment(5, 10, 0)];
    let landed = append_fragments(&target, version, accepted.clone())
        .await
        .unwrap();
    assert_eq!(landed.version_id(), version + 1);
    let mut expected = bootstrap;
    expected.extend(accepted);
    assert_fragments_read_back(&target, &expected).await;
}

/// A commit keeps the manifest it writes in its session, so the next commit
/// through its result reads no manifest.
#[tokio::test]
async fn commit_after_a_commit_in_the_same_session_reads_no_manifest() {
    let (mut target, _, _) = fixture(false).await;
    let io = IOTracker::default();
    let mut store = target.object_store.as_ref().clone();
    store.apply_wrapper(&io);
    target.object_store = Arc::new(store);
    let initial = Arc::new(publication::open_dataset(&target, None).await.unwrap());
    let first = crate::dataset::write::CommitBuilder::new(initial)
        .execute(replacement(1))
        .await
        .unwrap();
    io.incremental_stats();
    let second = crate::dataset::write::CommitBuilder::new(Arc::new(first))
        .execute(Transaction::new_from_version(
            2,
            differential::production_style_append(1),
        ))
        .await
        .unwrap();
    let reads = io.incremental_stats();
    assert_eq!(second.version_id(), 3);
    assert!(
        reads.requests.iter().all(|request| {
            request.method.starts_with("put") || !request.path.as_ref().ends_with(".manifest")
        }),
        "{reads:?}"
    );
}

async fn recreated_serialized_handle(
    tree: bool,
    stored_tree: bool,
) -> (tempfile::TempDir, Dataset) {
    let dir = tempfile::tempdir().unwrap();
    // The object store API reports entity tags, as S3 does. Plain local paths
    // resolve versions without them.
    let uri = format!("file-object-store://{}", dir.path().to_str().unwrap());
    let uri = uri.as_str();
    let create = |fragments: Vec<Fragment>, tree: bool| {
        Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments,
                schema: super::differential::table_schema(),
                config_upsert_values: tree.then(|| {
                    FragmentMetadataOptions::default()
                        .into_table_config()
                        .unwrap()
                }),
                initial_bases: None,
            },
        )
    };
    let dropped = crate::dataset::write::CommitBuilder::new(uri)
        .execute(create(Vec::new(), tree))
        .await
        .unwrap();
    let serialized = dropped.manifest().serialized();
    std::fs::remove_dir_all(dir.path()).unwrap();
    let stored = crate::dataset::write::CommitBuilder::new(uri)
        .execute(create((0..10).map(make_fragment).collect(), stored_tree))
        .await
        .unwrap();
    assert_eq!(stored.version_id(), dropped.version_id());

    let handle = crate::dataset::builder::DatasetBuilder::from_uri(uri)
        .with_serialized_manifest(&serialized)
        .unwrap()
        .load()
        .await
        .unwrap();
    (dir, handle)
}

#[rstest]
#[tokio::test]
async fn serialized_handle_checkout_reads_the_stored_version(#[values(false, true)] tree: bool) {
    let (_dir, mut handle) = recreated_serialized_handle(tree, tree).await;
    assert_eq!(handle.count_fragments(), 0);
    handle.checkout_latest().await.unwrap();
    assert_eq!(handle.count_fragments(), 10);
}

#[rstest]
#[tokio::test]
async fn commit_through_a_serialized_handle_builds_on_the_stored_version(
    #[values(false, true)] tree: bool,
) {
    let (_dir, handle) = recreated_serialized_handle(tree, tree).await;
    let uri = handle.uri().to_string();
    let committed = crate::dataset::write::CommitBuilder::new(Arc::new(handle))
        .execute(Transaction::new_from_version(
            1,
            Operation::Append {
                fragments: vec![make_fragment(0)],
            },
        ))
        .await
        .unwrap();
    let fresh = crate::dataset::builder::DatasetBuilder::from_uri(&uri)
        .load()
        .await
        .unwrap();
    assert_eq!(fresh.count_fragments(), 11);
    assert_eq!(committed.count_fragments(), 11);
}

#[rstest]
#[tokio::test]
async fn strict_overwrite_through_a_serialized_handle_uses_the_stored_frontier(
    #[values(false, true)] tree: bool,
) {
    let (_dir, handle) = recreated_serialized_handle(tree, tree).await;
    let committed = crate::dataset::write::CommitBuilder::new(Arc::new(handle))
        .with_max_retries(0)
        .execute(Transaction::new_from_version(
            1,
            Operation::Overwrite {
                fragments: vec![make_fragment(0)],
                schema: differential::table_schema(),
                config_upsert_values: None,
                initial_bases: None,
            },
        ))
        .await
        .unwrap();
    assert_eq!(committed.count_fragments(), 1);
    assert_eq!(committed.manifest.fragments[0].id, 10);
}

#[rstest]
#[tokio::test]
async fn recreated_handle_cannot_commit_across_metadata_layouts(#[values(false, true)] tree: bool) {
    let (_dir, handle) = recreated_serialized_handle(tree, !tree).await;
    let uri = handle.uri().to_string();
    let error = crate::dataset::write::CommitBuilder::new(Arc::new(handle))
        .execute(Transaction::new_from_version(
            1,
            Operation::Append {
                fragments: vec![make_fragment(0)],
            },
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
    assert!(
        error.to_string().contains("metadata layout changed"),
        "{error}"
    );
    let stored = crate::dataset::builder::DatasetBuilder::from_uri(&uri)
        .load()
        .await
        .unwrap();
    assert_eq!(stored.version_id(), 1);
    assert_eq!(stored.count_fragments(), 10);
}

#[rstest]
#[tokio::test]
async fn transaction_cache_does_not_cross_dataset_generations(#[values(false, true)] tree: bool) {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("file-object-store://{}", dir.path().to_str().unwrap());
    let session = Arc::new(Session::default());
    let create = |padding: bool| {
        let mut config = if tree {
            FragmentMetadataOptions::default()
                .into_table_config()
                .unwrap()
        } else {
            HashMap::new()
        };
        if padding {
            config.insert(
                "test.transaction_padding".into(),
                "x".repeat(crate::io::commit::MAX_INLINE_TRANSACTION_BYTES),
            );
        }
        Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: Vec::new(),
                schema: differential::table_schema(),
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        )
    };
    let mut dropped = crate::dataset::write::CommitBuilder::new(uri.as_str())
        .with_session(session.clone())
        .execute(create(false))
        .await
        .unwrap();
    let old_transaction = dropped.read_transaction().await.unwrap().unwrap();
    dropped.manifest_location.e_tag = None;
    dropped
        .metadata_cache
        .insert_with_key(
            &crate::session::caches::ManifestKey {
                version: 1,
                e_tag: None,
            },
            dropped.manifest.clone(),
        )
        .await;
    assert_eq!(
        dropped.read_transaction().await.unwrap().unwrap().uuid,
        old_transaction.uuid
    );
    assert!(
        dropped
            .metadata_cache
            .get_with_key(&crate::session::caches::TransactionKey {
                version: 1,
                e_tag: None,
            })
            .await
            .is_none()
    );
    std::fs::remove_dir_all(dir.path()).unwrap();
    let current_transaction = create(true);
    let expected_uuid = current_transaction.uuid.clone();
    assert_ne!(old_transaction.uuid, expected_uuid);
    let stored = crate::dataset::write::CommitBuilder::new(uri.as_str())
        .execute(current_transaction)
        .await
        .unwrap();
    assert!(stored.manifest.transaction_section.is_none());
    let mut untagged_location = stored.manifest_location.clone();
    assert!(untagged_location.size.is_some());
    untagged_location.e_tag = None;
    let manifest = Dataset::get_manifest(
        &stored.object_store,
        &untagged_location,
        &stored.uri,
        &session,
    )
    .await
    .unwrap();
    assert_eq!(
        manifest
            .config
            .get("test.transaction_padding")
            .map(String::len),
        Some(crate::io::commit::MAX_INLINE_TRANSACTION_BYTES)
    );
    let mut reopened = crate::dataset::builder::DatasetBuilder::from_uri(&uri)
        .with_session(session)
        .load()
        .await
        .unwrap();
    assert_eq!(
        reopened.read_transaction().await.unwrap().unwrap().uuid,
        expected_uuid
    );
    reopened.manifest_location.e_tag = None;
    assert_eq!(
        reopened.read_transaction().await.unwrap().unwrap().uuid,
        expected_uuid
    );
}

#[rstest]
#[case::stale(1)]
#[case::future(3)]
#[case::exhausted(u64::MAX)]
#[tokio::test]
async fn strict_overwrite_version_checks_match_flat(
    #[values(false, true)] tree: bool,
    #[case] read_version: u64,
) {
    let (_dir, handle) = recreated_serialized_handle(tree, tree).await;
    let uri = handle.uri().to_string();
    let current = crate::dataset::write::CommitBuilder::new(uri.as_str())
        .execute(Transaction::new_from_version(
            1,
            differential::production_style_append(1),
        ))
        .await
        .unwrap();
    let error = crate::dataset::write::CommitBuilder::new(Arc::new(current))
        .with_max_retries(0)
        .execute(Transaction::new_from_version(
            read_version,
            Operation::Overwrite {
                fragments: vec![make_fragment(0)],
                schema: differential::table_schema(),
                config_upsert_values: None,
                initial_bases: None,
            },
        ))
        .await
        .unwrap_err();
    if read_version > 2 {
        assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
    } else {
        assert!(matches!(error, Error::CommitConflict { .. }), "{error}");
    }
    assert!(
        error
            .to_string()
            .contains(&format!("version {read_version}")),
        "{error}"
    );
    let stored = crate::dataset::builder::DatasetBuilder::from_uri(&uri)
        .load()
        .await
        .unwrap();
    assert_eq!(stored.version_id(), 2);
    assert_eq!(stored.count_fragments(), 11);
}
