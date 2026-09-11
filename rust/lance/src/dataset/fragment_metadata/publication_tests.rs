// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::publication::{descriptor, read_version};
use super::test_support::open_tree;
use super::*;
use futures::TryStreamExt;
use lance_io::object_store::{ObjectStoreParams, ObjectStoreRegistry, WrappingObjectStore};
use lance_io::utils::failpoint::{FailOn, FailWhen, Failpoint, FailpointController};
use lance_io::utils::tracking_store::IOTracker;
use lance_table::format::BasePath;
use lance_table::fragment_metadata::MANIFEST_LAYOUT_KEY;
use lance_table::fragment_metadata::support::{make_fragment, make_replacement_data_file};
use lance_table::io::commit::{
    CommitError, CommitHandler, ConditionalPutCommitHandler, ManifestLocation, ManifestWriter,
};
use object_store::ObjectStoreExt;
use prost::Message;
use rstest::rstest;

async fn fixture(bulk: bool) -> (CommitTarget, FailpointController, Dataset) {
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
        (MAX_NODE_BYTES_KEY.to_string(), "65536".to_string()),
        (MAX_LEAF_BYTES_KEY.to_string(), "32768".to_string()),
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
            fragments: (0..512).map(make_fragment).collect(),
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
    let leaves = dataset
        .lazy_fragments
        .as_ref()
        .unwrap()
        .tree()
        .leaf_object_sizes()
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
        let dataset = lazy.into_dataset().await.unwrap();
        assert!(io.incremental_stats().read_iops > 0);
        dataset
    } else {
        let committed = builder.execute(transaction).await.unwrap();
        let reads = io.incremental_stats();
        assert!(
            reads.requests.iter().all(|request| {
                !request.method.starts_with("get_") || !request.path.as_ref().contains("_bt/leaf/")
            }),
            "{reads:?}"
        );
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
        "descriptor" => Arc::make_mut(&mut context.manifest).fragment_metadata = None,
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
struct ConcurrentPublication(tokio::sync::Barrier);

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
        if manifest.version == 2 {
            self.0.wait().await;
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

#[tokio::test]
async fn retained_commit_retries_after_concurrent_publication() {
    let (mut target, _, _) = fixture(false).await;
    target.commit_handler = Arc::new(ConcurrentPublication(tokio::sync::Barrier::new(2)));
    let mut initial = publication::open_dataset(&target, None).await.unwrap();
    initial.hydrate_fragments_for_maintenance().await.unwrap();
    let initial = Arc::new(initial);
    let append = crate::dataset::write::CommitBuilder::new(initial.clone())
        .with_skip_auto_cleanup(true)
        .execute(Transaction::new_from_version(
            1,
            differential::production_style_append(2),
        ));
    let replace = crate::dataset::write::CommitBuilder::new(initial.clone())
        .with_skip_auto_cleanup(true)
        .execute(replacement(1));
    let (append, replace) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::join!(append, replace)
    })
    .await
    .unwrap();
    let append = append.unwrap();
    let replace = replace.unwrap();
    assert_ne!(append.version_id(), replace.version_id());
    for committed in [&append, &replace] {
        let (fresh, _) = open_tree(&target, Some(committed.version_id()))
            .await
            .unwrap();
        assert_eq!(
            committed.manifest.fragments.as_ref(),
            &fresh.materialize().await.unwrap()
        );
    }
    let latest = if append.version_id() == 3 {
        append
    } else {
        replace
    };
    assert_eq!(latest.manifest.fragments.len(), 514);
    assert_eq!(
        latest.manifest.fragments[7].files[0].path,
        make_replacement_data_file(7, 1).path
    );
    assert_eq!(initial.version_id(), 1);
    assert_eq!(initial.manifest.fragments[7], make_fragment(7));
}

#[rstest]
#[case::leaf_before("_bt/leaf/", FailWhen::Before, false)]
#[case::leaf_after("_bt/leaf/", FailWhen::After, false)]
#[case::base_before("_bt/base/", FailWhen::Before, false)]
#[case::base_after("_bt/base/", FailWhen::After, false)]
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
    assert!(
        roots.is_empty(),
        "A second snapshot authority was published"
    );
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
        let Some(pb::fragment_metadata_tree::Root::RootPath(path)) = snapshot.root else {
            panic!("expected external root");
        };
        bases.insert(path);
        has_suffix |= !snapshot.mutations_since_root.is_empty();
        assert!(
            pb::FragmentMetadataNode {
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
        if let Some(pb::fragment_metadata_tree::Root::RootPath(path)) =
            descriptor(&manifest).unwrap().root
        {
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
        .join("base")
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
    if let Some(pb::fragment_metadata_tree::Root::RootPath(path)) = old_snapshot.root {
        assert!(
            target
                .object_store
                .inner
                .head(&Path::from_iter(
                    target.base_path.parts().chain(Path::from(path).parts())
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
async fn deep_writer_requires_its_own_experimental_gate() {
    let (target, _, dataset) = fixture(false).await;
    let mut config = dataset.manifest.config.clone();
    config.insert(MAX_NODE_BYTES_KEY.to_string(), "512".to_string());
    config.insert(MAX_LEAF_BYTES_KEY.to_string(), "1024".to_string());
    let isolated = CommitTarget {
        base_path: target.base_path.clone().join("deep-gate"),
        ..target
    };
    let transaction = Transaction::new_from_version(
        0,
        Operation::Overwrite {
            fragments: (0..128).map(make_fragment).collect(),
            schema: differential::table_schema(),
            config_upsert_values: Some(config),
            initial_bases: None,
        },
    );
    let error = execute_create(
        isolated.clone(),
        &transaction,
        &crate::dataset::ManifestWriteConfig::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::NotSupported { .. }), "{error}");
    assert!(error.to_string().contains("allow_deep_writer"));
    assert!(
        read_version(
            &isolated.object_store,
            &isolated.base_path,
            isolated.commit_handler.as_ref(),
            None
        )
        .await
        .is_err()
    );
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
        path_contains: "_bt/base/".to_string(),
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
async fn shallow_clone_stamps_source_tree_references() {
    let (_target, _, mut original) = fixture(false).await;
    let cloned = original
        .shallow_clone("memory:///clone", 1, None)
        .await
        .unwrap();
    let snapshot = cloned.manifest.fragment_metadata.as_ref().unwrap();
    let Some(pb::fragment_metadata_tree::Root::RootPath(root_path)) = &snapshot.root else {
        panic!("clone must own a dest root");
    };
    assert!(root_path.starts_with("_bt/"));
    let tree = tree_from_manifest(
        cloned.object_store.clone(),
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
    let root = pb::FragmentMetadataRoot::decode(cloned_root.as_ref()).unwrap();
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
#[case("lance.fragment_metadata.allow_deep_writer", "yes")]
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

#[tokio::test]
async fn clone_inlines_external_lineage_slices_from_the_source() {
    let store = ObjectStore::memory();
    let base = Path::from("source");
    store
        .put(
            &base.clone().join("_bt").join("lineage"),
            &[0, 1, 2, 3, 4, 5],
        )
        .await
        .unwrap();
    let slice = lance_table::format::ExternalFile {
        path: "_bt/lineage".into(),
        offset: 2,
        size: 3,
    };
    let mut fragments = vec![make_fragment(0)];
    fragments[0].row_id_meta = Some(lance_table::format::RowIdMeta::External(slice.clone()));
    fragments[0].created_at_version_meta = Some(
        lance_table::format::RowDatasetVersionMeta::External(slice.clone()),
    );
    fragments[0].last_updated_at_version_meta =
        Some(lance_table::format::RowDatasetVersionMeta::External(slice));
    inline_clone_lineage(&store, &base, &mut fragments)
        .await
        .unwrap();
    assert_eq!(
        fragments[0].row_id_meta,
        Some(lance_table::format::RowIdMeta::Inline(vec![2, 3, 4].into()))
    );
    assert_eq!(
        fragments[0].created_at_version_meta,
        Some(lance_table::format::RowDatasetVersionMeta::Inline(
            Arc::from([2, 3, 4])
        ))
    );
    assert_eq!(
        fragments[0].last_updated_at_version_meta,
        fragments[0].created_at_version_meta
    );
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
