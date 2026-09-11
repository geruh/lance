// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;
use crate::fragment_metadata::action;
use crate::fragment_metadata::support::{
    make_backfill_data_file, make_fragment, make_fragment_with_files, make_replacement_data_file,
};
use lance_io::object_store::{ObjectStoreParams, ObjectStoreRegistry};
use lance_io::scheduler::SchedulerConfig;
use lance_io::utils::failpoint::{FailOn, FailWhen, Failpoint, FailpointController};
use lance_io::utils::tracking_store::IOTracker;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use rstest::rstest;

struct Fixture {
    tree: FragmentMetadataTree,
    config: FragmentMetadataTreeConfig,
    snapshot: pb::FragmentMetadataTree,
    store: Arc<ObjectStore>,
    base: Path,
    scheduler: Arc<ScanScheduler>,
    io: IOTracker,
}

#[rstest]
#[case::root_envelope(false)]
#[case::child_object(true)]
#[tokio::test]
async fn snapshot_rejects_objects_over_its_hard_limit(#[case] oversized_child: bool) {
    let fixture = Fixture::new(
        2,
        FragmentMetadataTreeConfig::default(),
        SnapshotPolicy::default(),
    )
    .await;
    let mut snapshot = fixture.snapshot.clone();
    let Some(pb::fragment_metadata_tree::Root::InlineRoot(root)) = &mut snapshot.root else {
        panic!("expected inline root");
    };
    if oversized_child {
        root.children[0].object_size = 0;
    } else {
        root.next_action_sequence = 0;
    }
    let result = FragmentMetadataTree::open_snapshot(
        fixture.store,
        fixture.base,
        fixture.scheduler,
        Arc::new(LanceCache::with_capacity(0)),
        &snapshot,
        1,
        fixture.config.clone(),
        fixture.tree.next_fragment_id(),
    )
    .await;
    let error = result
        .err()
        .expect("oversized snapshot must be rejected at open");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
    if oversized_child {
        assert!(
            error.to_string().contains("object_size") || error.to_string().contains("child"),
            "{error}"
        );
    } else {
        assert!(
            error.to_string().contains("next_action_sequence"),
            "{error}"
        );
    }
}

impl Fixture {
    async fn new(count: u64, config: FragmentMetadataTreeConfig, policy: SnapshotPolicy) -> Self {
        let io = IOTracker::default();
        let (store, base) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            "memory://",
            &ObjectStoreParams {
                object_store_wrapper: Some(Arc::new(io.clone())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let scheduler = ScanScheduler::new(store.clone(), SchedulerConfig::default_for_testing());
        let (tree, snapshot, _) = FragmentMetadataTree::bootstrap_snapshot(
            store.clone(),
            base.clone(),
            scheduler.clone(),
            Arc::new(LanceCache::with_capacity(0)),
            config.clone(),
            (0..count).map(make_fragment).collect(),
            1,
            policy,
        )
        .await
        .unwrap();
        Self {
            tree,
            config,
            snapshot,
            store,
            base,
            scheduler,
            io,
        }
    }

    async fn open(
        &self,
        snapshot: &pb::FragmentMetadataTree,
        version: u64,
    ) -> FragmentMetadataTree {
        FragmentMetadataTree::open_snapshot(
            self.store.clone(),
            self.base.clone(),
            self.scheduler.clone(),
            Arc::new(LanceCache::with_capacity(0)),
            snapshot,
            version,
            self.config.clone(),
            self.tree.next_fragment_id(),
        )
        .await
        .unwrap()
    }

    async fn commit(
        &mut self,
        actions: Vec<pb::FragmentAction>,
        policy: SnapshotPolicy,
        bulk: bool,
    ) -> CommitStats {
        let ids: Vec<_> = actions.iter().filter_map(action::target_frag_id).collect();
        let touched = if bulk {
            self.tree.resolve_touched_for_bulk(&ids).await.unwrap()
        } else {
            self.tree.resolve_touched(&ids).await.unwrap()
        };
        let (snapshot, stats) = self
            .tree
            .prepare_snapshot(
                ValidatedCommit::fragment_actions(actions),
                &touched,
                &self.snapshot,
                policy,
                bulk,
            )
            .await
            .unwrap();
        self.snapshot = snapshot;
        stats
    }
}

#[rstest]
#[case::fragments("Fragments")]
#[case::physical_rows("physical rows")]
#[case::visible_rows("visible rows")]
#[tokio::test]
async fn snapshot_counts_are_checked_against_children_and_pending_changes(#[case] count: &str) {
    let checkpoint = SnapshotPolicy {
        inline_root_bytes: 0,
        max_suffix_bytes: 0,
    };
    let mut fixture = Fixture::new(3, FragmentMetadataTreeConfig::default(), checkpoint).await;
    fixture
        .commit(vec![action::remove_fragment(0)], checkpoint, false)
        .await;
    assert_eq!(fixture.tree.root_buffer_len(), 1);
    fixture
        .commit(
            vec![action::remove_fragment(1)],
            SnapshotPolicy {
                max_suffix_bytes: 1024,
                ..checkpoint
            },
            false,
        )
        .await;
    assert_eq!(fixture.snapshot.mutations_since_root.len(), 1);
    let reopened = fixture.open(&fixture.snapshot, 3).await;
    assert_eq!(
        reopened.materialize().await.unwrap(),
        vec![make_fragment(2)]
    );

    let mut snapshot = fixture.snapshot.clone();
    match count {
        "Fragments" => snapshot.mutations_since_root[0].fragment_count_delta = -999,
        "physical rows" => snapshot.mutations_since_root[0].total_rows_delta = -999,
        "visible rows" => snapshot.mutations_since_root[0].visible_rows_delta = -999,
        _ => unreachable!(),
    }
    let error = FragmentMetadataTree::open_snapshot(
        fixture.store,
        fixture.base,
        fixture.scheduler,
        Arc::new(LanceCache::with_capacity(0)),
        &snapshot,
        3,
        fixture.config.clone(),
        fixture.tree.next_fragment_id(),
    )
    .await
    .err()
    .expect("snapshot totals must agree with the tree");
    assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
    assert!(error.to_string().contains(count), "{error}");
}

#[rstest]
#[case::buffered(17, false)]
#[case::bulk(29, true)]
#[tokio::test]
async fn mutations_survive_routing_growth_and_historical_reads(
    #[case] seed: u64,
    #[case] bulk: bool,
) {
    let policy = SnapshotPolicy {
        inline_root_bytes: 0,
        max_suffix_bytes: 512,
    };
    let config = FragmentMetadataTreeConfig::new(512, u32::MAX).with_max_leaf_bytes(4096);
    let mut fixture = Fixture::new(48, config, policy).await;
    let initial = fixture.snapshot.clone();
    let mut expected: BTreeMap<_, _> = (0..48).map(|id| (id, make_fragment(id))).collect();
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut maximum_height = fixture.tree.height();
    for round in 0..12 {
        let mut changes = Vec::new();
        for offset in 0..4 {
            let id = 48 + round * 4 + offset;
            let fragment = make_fragment(id);
            changes.push(action::add_fragment(&fragment));
            expected.insert(id, fragment);
        }
        let id = rng.random_range(0..48);
        let mut fragment = make_fragment(id);
        fragment.files[0].path = format!("round-{round}-{id}.lance");
        changes.push(action::add_fragment(&fragment));
        expected.insert(id, fragment);
        fixture.commit(changes, policy, bulk).await;
        maximum_height = maximum_height.max(fixture.tree.height());
        assert_eq!(
            fixture.tree.materialize().await.unwrap(),
            expected.values().cloned().collect::<Vec<_>>()
        );
        fixture.tree.verify_watermarks().await.unwrap();
        let reopened = fixture.open(&fixture.snapshot, round + 2).await;
        assert_eq!(
            reopened.materialize().await.unwrap(),
            fixture.tree.materialize().await.unwrap()
        );
    }
    assert!(
        maximum_height >= 2,
        "the fixture must cross an interior level"
    );
    let remove = expected
        .keys()
        .copied()
        .filter(|id| *id != 7)
        .map(action::remove_fragment)
        .collect();
    fixture.commit(remove, policy, true).await;
    assert_eq!(fixture.tree.height(), 1);
    assert_eq!(
        fixture.tree.materialize().await.unwrap(),
        vec![expected[&7].clone()]
    );
    let historical = fixture.open(&initial, 1).await;
    assert_eq!(
        historical.materialize().await.unwrap(),
        (0..48).map(make_fragment).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn bulk_reuses_validation_leaf_reads_and_coalesces_across_parents() {
    let policy = SnapshotPolicy {
        inline_root_bytes: 0,
        max_suffix_bytes: 0,
    };
    let config = FragmentMetadataTreeConfig::new(512, u32::MAX).with_max_leaf_bytes(4096);
    let mut fixture = Fixture::new(64, config, policy).await;
    let leaves = fixture.tree.shape_report().await.unwrap().leaf_keys.len();
    assert!(fixture.tree.height() >= 2);
    fixture.io.incremental_stats();
    fixture
        .commit((1..64).map(action::remove_fragment).collect(), policy, true)
        .await;
    let measured = fixture.io.incremental_stats();
    let reads = measured
        .requests
        .iter()
        .filter(|request| {
            request.method.starts_with("get_") && request.path.as_ref().contains("_bt/leaf/")
        })
        .count();
    assert_eq!(
        reads, leaves,
        "validation and materialization must share leaf reads: {measured:?}"
    );
    assert_eq!(fixture.tree.root_child_count(), 1);
    assert_eq!(
        measured.write_iops, 2,
        "one replacement leaf and one root base"
    );
    assert_eq!(
        fixture.tree.materialize().await.unwrap(),
        vec![make_fragment(0)]
    );
}

#[rstest]
#[case::compacted(false, true)]
#[case::buffered(true, true)]
#[case::near_capacity(false, false)]
#[tokio::test]
async fn root_contracts_multiple_children_with_room_to_grow(
    #[case] buffered: bool,
    #[case] should_contract: bool,
) {
    let policy = SnapshotPolicy::default();
    let mut fixture = Fixture::new(4, FragmentMetadataTreeConfig::default(), policy).await;
    let mut leaves = Vec::new();
    for id in 0..4 {
        let written = fixture
            .tree
            .store
            .write_leaves(&[make_fragment(id)], 0, &fixture.tree.config)
            .await
            .unwrap();
        leaves.push(written.into_iter().next().unwrap().child_ref);
    }
    let mut expected: Vec<_> = (0..4).map(make_fragment).collect();
    let mut child_buffer = Vec::new();
    if buffered {
        let old_path = expected[0].files[0].path.clone();
        expected[0].files[0].path = "first-replacement.lance".into();
        child_buffer.push(pb::FragmentMetadataMutation {
            action_sequence: 1,
            action: Some(action::replace_data_file(
                0,
                &old_path,
                &expected[0].files[0],
            )),
            ..Default::default()
        });
        let old_path = expected[0].files[0].path.clone();
        expected[0].files[0].path = "second-replacement.lance".into();
        fixture.tree.buffer.push(pb::FragmentMetadataMutation {
            action_sequence: 2,
            action: Some(action::replace_data_file(
                0,
                &old_path,
                &expected[0].files[0],
            )),
            ..Default::default()
        });
        fixture.tree.next_action_sequence = 3;
        fixture.tree.store.next_action_sequence = 3;
    }
    let left = fixture
        .tree
        .store
        .write_internal(leaves[..2].to_vec(), child_buffer)
        .await
        .unwrap();
    let right = fixture
        .tree
        .store
        .write_internal(leaves[2..].to_vec(), Vec::new())
        .await
        .unwrap();
    fixture.tree.children = vec![left.child_ref, right.child_ref];
    let collapsed_bytes = fixture
        .tree
        .children
        .iter()
        .map(|child| child.object_size)
        .sum::<u64>()
        + node::internal_logical_bytes(&[], &fixture.tree.buffer);
    fixture.tree.config.max_node_bytes = if should_contract {
        collapsed_bytes * 2
    } else {
        collapsed_bytes * 10 / 9
    };
    let mut historical = fixture.snapshot.clone();
    historical.next_action_sequence = fixture.tree.next_action_sequence;
    historical.root = Some(pb::fragment_metadata_tree::Root::InlineRoot(
        fixture.tree.compacted_root(),
    ));
    assert_eq!(fixture.tree.height(), 2);
    assert_eq!(
        fixture.tree.resolve_fragment(0).await.unwrap(),
        Some(expected[0].clone())
    );

    fixture.io.incremental_stats();
    fixture.tree.maybe_shrink_root().await.unwrap();
    let io = fixture.io.incremental_stats();
    assert_eq!(fixture.tree.height(), if should_contract { 1 } else { 2 });
    assert_eq!(io.write_iops, 0, "contraction must reuse the leaves");
    assert!(
        io.requests
            .iter()
            .all(|request| !request.path.as_ref().contains("_bt/leaf/"))
    );
    assert_eq!(fixture.tree.materialize().await.unwrap(), expected);
    assert_eq!(
        fixture.tree.resolve_fragment(0).await.unwrap(),
        Some(expected[0].clone())
    );
    fixture.tree.verify_watermarks().await.unwrap();
    let mut snapshot = historical.clone();
    snapshot.root = Some(pb::fragment_metadata_tree::Root::InlineRoot(
        fixture.tree.compacted_root(),
    ));
    let reopened = fixture.open(&snapshot, 1).await;
    assert_eq!(reopened.materialize().await.unwrap(), expected);
    let old = fixture.open(&historical, 1).await;
    assert_eq!(old.height(), 2);
    assert_eq!(old.materialize().await.unwrap(), expected);
}

#[tokio::test]
async fn bootstrap_packs_compressed_batches_within_the_leaf_target() {
    let config = FragmentMetadataTreeConfig::default();
    let fixture = Fixture::new(0, config.clone(), SnapshotPolicy::default()).await;
    let fragments: Vec<_> = (5_000_000..5_000_216)
        .map(|id| make_fragment_with_files(id, 128))
        .collect();
    let encoded = fixture.tree.store.encode_leaf(&fragments).await.unwrap();
    assert!(node::leaf_logical_bytes(&fragments) > config.max_leaf_bytes * 2);
    assert!(encoded.len() as u64 <= config.max_leaf_bytes);
    fixture.io.incremental_stats();
    let (tree, stats) =
        FragmentMetadataTree::build(fixture.tree.store, config.clone(), fragments.clone())
            .await
            .unwrap();
    assert_eq!(
        stats.num_leaves, 1,
        "the complete encoding fits in one leaf"
    );
    assert_eq!(
        fixture.io.incremental_stats().write_iops,
        1,
        "do not publish candidate leaves"
    );
    assert!(
        tree.leaf_object_sizes()
            .iter()
            .all(|size| *size <= config.max_leaf_bytes)
    );
    assert_eq!(tree.materialize().await.unwrap(), fragments);
}

#[rstest]
#[case::serial(1)]
#[case::prefetched(4)]
#[tokio::test]
async fn deep_stream_honors_prefetch_and_preserves_order(#[case] prefetch: usize) {
    let policy = SnapshotPolicy::default();
    let config = FragmentMetadataTreeConfig::new(512, u32::MAX).with_max_leaf_bytes(4096);
    let mut fixture = Fixture::new(64, config, policy).await;
    let mut expected: Vec<_> = (0..64).map(make_fragment).collect();
    let mut actions = Vec::new();
    for fragment in expected.iter_mut().take(8) {
        let file = crate::fragment_metadata::support::make_backfill_data_file(fragment.id, 0);
        actions.push(action::add_data_file(fragment.id, &file));
        fragment.files.push(file);
    }
    actions.push(action::remove_fragment(63));
    actions.push(action::add_fragment(&make_fragment(64)));
    expected[63] = make_fragment(64);
    fixture.commit(actions, policy, false).await;
    assert!(fixture.tree.height() >= 2);
    let leaf_count = fixture.tree.shape_report().await.unwrap().leaf_keys.len();
    let delayed = FailpointController::default();
    delayed.set_get_latency(std::time::Duration::from_millis(2));
    let io = IOTracker::default();
    let mut store = fixture.store.as_ref().clone();
    store.apply_wrapper(&delayed);
    store.apply_wrapper(&io);
    let tree = Arc::new(fixture.tree.with_object_store(Arc::new(store)));
    let actual: Vec<_> = tree
        .fragment_stream_with_prefetch(prefetch)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actual, expected);
    let measured = io.incremental_stats();
    assert_eq!(
        measured
            .requests
            .iter()
            .filter(|request| request.path.as_ref().contains("_bt/leaf/"))
            .count(),
        leaf_count,
        "each leaf must be read once"
    );
    if prefetch == 1 {
        assert_eq!(measured.num_stages, measured.read_iops);
    } else {
        assert!(
            measured.num_stages < measured.read_iops,
            "deep leaf reads must overlap: {measured:?}"
        );
    }
}

#[tokio::test]
async fn failed_prepare_restores_frontiers_and_buffer() {
    let policy = SnapshotPolicy::default();
    let config = FragmentMetadataTreeConfig::new(1024, u32::MAX)
        .with_max_leaf_bytes(4096)
        .with_hard_capacity_bytes(8192);
    let mut fixture = Fixture::new(2, config, policy).await;
    let touched = fixture.tree.resolve_touched(&[9]).await.unwrap();
    let error = fixture
        .tree
        .prepare_snapshot(
            ValidatedCommit::fragment_actions(vec![action::add_fragment(
                &make_fragment_with_files(9, 256),
            )]),
            &touched,
            &fixture.snapshot,
            policy,
            true,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
    assert!(error.to_string().contains("hard_capacity_bytes"), "{error}");
    assert_eq!(fixture.tree.version(), 1);
    assert_eq!(fixture.tree.next_fragment_id(), 2);
    assert_eq!(fixture.tree.root_buffer_len(), 0);
    fixture
        .commit(vec![action::add_fragment(&make_fragment(2))], policy, false)
        .await;
    assert_eq!(
        fixture.tree.materialize().await.unwrap(),
        (0..3).map(make_fragment).collect::<Vec<_>>()
    );
}

#[rstest]
#[case::rewrite(false, None)]
#[case::split_and_remove(true, None)]
#[case::failed_put(false, Some(FailWhen::Before))]
#[case::lost_put_response(false, Some(FailWhen::After))]
#[tokio::test]
async fn sibling_leaf_drains_overlap_and_preserve_snapshots(
    #[case] change_shape: bool,
    #[case] failure: Option<FailWhen>,
) {
    let policy = SnapshotPolicy::default();
    let config = FragmentMetadataTreeConfig::default()
        .with_max_leaf_bytes(4096)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(128, config, policy).await;
    assert_eq!(fixture.tree.height(), 1);
    assert!(fixture.tree.children.len() > 1);
    let original = fixture.tree.materialize().await.unwrap();
    let previous = fixture.snapshot.clone();
    let mut expected = Vec::new();
    let mut actions = Vec::new();
    for fragment in &original {
        let child = node::child_index_for(&fixture.tree.children, fragment.id);
        if change_shape && child.is_multiple_of(2) {
            actions.push(action::remove_fragment(fragment.id));
        } else {
            let replacement =
                make_fragment_with_files(fragment.id, if change_shape { 16 } else { 2 });
            actions.push(action::add_fragment(&replacement));
            expected.push(replacement);
        }
    }
    let ids: Vec<_> = original.iter().map(|fragment| fragment.id).collect();
    let touched = fixture.tree.resolve_touched(&ids).await.unwrap();
    let delayed = FailpointController::default();
    delayed.set_get_latency(std::time::Duration::from_millis(2));
    if let Some(when) = failure {
        delayed.arm(Failpoint {
            on: FailOn::Put,
            when,
            path_contains: "_bt/leaf/".into(),
            nth: 2,
        });
    }
    let io = IOTracker::default();
    let mut store = fixture.store.as_ref().clone();
    store.apply_wrapper(&delayed);
    store.apply_wrapper(&io);
    fixture.tree = fixture.tree.with_object_store(Arc::new(store));
    let result = fixture
        .tree
        .prepare_snapshot(
            ValidatedCommit::fragment_actions(actions.clone()),
            &touched,
            &previous,
            policy,
            false,
        )
        .await;
    if failure.is_some() {
        let error = result.unwrap_err();
        assert!(matches!(error, Error::IO { .. }), "{error}");
        assert!(delayed.tripped());
        assert!(error.to_string().contains("failpoint"), "{error}");
        assert_eq!(fixture.tree.version(), 1);
        assert_eq!(fixture.tree.root_buffer_len(), 0);
        assert_eq!(fixture.tree.materialize().await.unwrap(), original);
        delayed.disarm();
        fixture
            .tree
            .prepare_snapshot(
                ValidatedCommit::fragment_actions(actions),
                &touched,
                &previous,
                policy,
                false,
            )
            .await
            .unwrap();
    } else {
        let (_, stats) = result.unwrap();
        assert!(stats.flushes > 1);
        if change_shape {
            assert!(stats.splits > 0);
        }
        let measured = io.incremental_stats();
        assert!(
            measured.num_stages < measured.read_iops + measured.write_iops,
            "sibling drains must overlap: {measured:?}"
        );
    }
    assert_eq!(fixture.tree.materialize().await.unwrap(), expected);
    assert_eq!(
        fixture
            .open(&previous, 1)
            .await
            .materialize()
            .await
            .unwrap(),
        original
    );
}

#[rstest]
#[case::buffered_inline(false, usize::MAX)]
#[case::buffered_external(false, 0)]
#[case::bulk_external(true, 0)]
#[tokio::test]
async fn aliased_file_actions_survive_pressure_and_checkpoint(
    #[case] bulk: bool,
    #[case] inline_root_bytes: usize,
) {
    let policy = SnapshotPolicy {
        inline_root_bytes,
        max_suffix_bytes: 256,
    };
    let config = FragmentMetadataTreeConfig::new(512, u32::MAX)
        .with_max_leaf_bytes(4096)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(32, config, policy).await;
    assert!(fixture.tree.height() >= 2);
    let mut expected: BTreeMap<_, _> = (0..32).map(|id| (id, make_fragment(id))).collect();
    let mut fragment = make_fragment_with_files(7, 2);
    fragment.files[0].path = "A".into();
    fragment.files[1].path = "B".into();
    fixture
        .commit(vec![action::add_fragment(&fragment)], policy, bulk)
        .await;
    expected.insert(7, fragment.clone());
    let historical = fixture.snapshot.clone();
    let mut materialized = 0;
    for (from, to) in [("B", "A"), ("A", "C")] {
        let mut replacement = fragment.files[0].clone();
        replacement.path = to.into();
        // Replacement mapping and version are deliberately different: the
        // action changes the first matching slot's location fields only.
        replacement.fields = Arc::from([99]);
        replacement.column_indices = Arc::from([9]);
        replacement.file_major_version = 99;
        let stats = fixture
            .commit(
                vec![action::replace_data_file(7, from, &replacement)],
                policy,
                bulk,
            )
            .await;
        materialized += stats.messages_materialized;
        let matched = fragment
            .files
            .iter_mut()
            .find(|file| file.path == from)
            .unwrap();
        matched.path = to.into();
        matched.file_size_bytes = replacement.file_size_bytes;
        matched.base_id = replacement.base_id;
        expected.insert(7, fragment.clone());
        let reopened = fixture
            .open(&fixture.snapshot, fixture.tree.version())
            .await;
        assert_eq!(
            reopened.resolve_fragment(7).await.unwrap(),
            Some(fragment.clone())
        );
        assert_eq!(
            reopened.materialize().await.unwrap(),
            expected.values().cloned().collect::<Vec<_>>()
        );
        assert_eq!(
            reopened
                .resolve_fragments(&RoaringBitmap::from_iter([7]))
                .await
                .unwrap(),
            vec![fragment.clone()]
        );
        assert_eq!(
            reopened.fragment_at_row_offset(7).await.unwrap(),
            OffsetResolution::Found(Box::new(fragment.clone()), 0)
        );
        fixture.tree.verify_watermarks().await.unwrap();
    }
    assert_eq!(
        fragment
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        ["C", "A"]
    );
    if !bulk {
        assert_eq!(
            materialized, 0,
            "a chain below the amortization gate stays buffered"
        );
    }
    fixture.commit(Vec::new(), policy, true).await;
    assert_eq!(
        fixture.tree.materialize().await.unwrap(),
        expected.values().cloned().collect::<Vec<_>>()
    );
    let old = fixture.open(&historical, 2).await;
    let old_fragment = old.resolve_fragment(7).await.unwrap().unwrap();
    assert_eq!(
        old_fragment
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        ["A", "B"]
    );
}

#[tokio::test]
async fn point_scatter_and_lower_bound_reads_use_the_same_state() {
    let mut fixture = Fixture::new(
        64,
        FragmentMetadataTreeConfig::new(1024, u32::MAX).with_max_leaf_bytes(4096),
        SnapshotPolicy::default(),
    )
    .await;
    fixture
        .commit(
            vec![
                action::remove_fragment(7),
                action::add_fragment(&make_fragment(90)),
            ],
            SnapshotPolicy::default(),
            false,
        )
        .await;
    let bitmap = RoaringBitmap::from_iter([2, 7, 8, 63, 90]);
    let selected = fixture.tree.resolve_fragments(&bitmap).await.unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|fragment| fragment.id)
            .collect::<Vec<_>>(),
        vec![2, 8, 63, 90]
    );
    let tree = Arc::new(fixture.tree);
    let from: Vec<_> = tree
        .clone()
        .fragment_stream_from(62)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        from.iter().map(|fragment| fragment.id).collect::<Vec<_>>(),
        vec![62, 63, 90]
    );
    assert_eq!(tree.resolve_fragment(7).await.unwrap(), None);
}

#[tokio::test]
async fn scattered_batches_wait_for_amortization_and_replay_in_order() {
    const ROUNDS: u32 = 24;
    let policy = SnapshotPolicy::default();
    let config = FragmentMetadataTreeConfig::default()
        .with_max_leaf_bytes(16 * 1024)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(1024, config.clone(), policy).await;
    assert_eq!(fixture.tree.height(), 1);
    let leaves = fixture.tree.root_child_count();
    assert!(leaves >= 8, "{leaves} leaves");
    let original = fixture.tree.materialize().await.unwrap();
    let historical = fixture.snapshot.clone();
    let mut expected: BTreeMap<u64, Fragment> = original
        .iter()
        .map(|fragment| (fragment.id, fragment.clone()))
        .collect();
    let mut targets = Vec::with_capacity(leaves);
    for child in 0..leaves {
        let id = original
            .iter()
            .map(|fragment| fragment.id)
            .find(|id| node::child_index_for(&fixture.tree.children, *id) == child)
            .unwrap();
        targets.push(id);
    }
    let chained = targets[leaves / 2];
    let mut flushes = 0;
    let mut deferred_rounds = 0;
    for round in 0..ROUNDS {
        let mut actions = Vec::new();
        for &id in &targets {
            let file = make_backfill_data_file(id, round);
            actions.push(action::add_data_file(id, &file));
            expected.get_mut(&id).unwrap().files.push(file);
        }
        let current = expected[&chained].files[0].path.clone();
        let replacement = make_replacement_data_file(chained, round);
        actions.push(action::replace_data_file(chained, &current, &replacement));
        let slot = &mut expected.get_mut(&chained).unwrap().files[0];
        slot.path = replacement.path.clone();
        slot.file_size_bytes = replacement.file_size_bytes;
        slot.base_id = replacement.base_id;

        let stats = fixture.commit(actions, policy, false).await;
        flushes += stats.flushes;
        deferred_rounds += u64::from(stats.flushes == 0);
        assert!(
            !node::internal_overflows(&fixture.tree.children, &fixture.tree.buffer, &config),
            "round {round}: the root must stay within its structural budget"
        );
        assert_eq!(
            fixture.tree.materialize().await.unwrap(),
            expected.values().cloned().collect::<Vec<_>>(),
            "round {round}"
        );
        fixture.tree.verify_watermarks().await.unwrap();
    }
    assert!(
        deferred_rounds > 0 && flushes >= leaves as u64,
        "every leaf drained at least once and some rounds only buffered: \
         flushes={flushes} deferred_rounds={deferred_rounds}"
    );
    assert!(
        flushes <= (leaves as u64) * u64::from(ROUNDS) / 4,
        "sub-gate batches must not rewrite a leaf every round: flushes={flushes} leaves={leaves}"
    );
    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    assert_eq!(
        reopened.materialize().await.unwrap(),
        expected.values().cloned().collect::<Vec<_>>()
    );
    let chained_fragment = reopened.resolve_fragment(chained).await.unwrap().unwrap();
    assert_eq!(
        chained_fragment.files[0].path,
        make_replacement_data_file(chained, ROUNDS - 1).path
    );
    assert_eq!(
        chained_fragment.files[0].fields,
        original[0].files[0].fields
    );
    assert_eq!(chained_fragment.files.len(), 1 + ROUNDS as usize);
    assert_eq!(
        fixture
            .open(&historical, 1)
            .await
            .materialize()
            .await
            .unwrap(),
        original
    );
}

#[rstest]
#[case::inline_root_with_suffix("inline_root_with_suffix")]
#[case::external_root_suffix_zero("external_root_suffix_zero")]
#[case::root_buffer_zero("root_buffer_zero")]
#[case::root_next_zero("root_next_zero")]
#[tokio::test]
async fn sequence_zero_is_rejected_on_open(#[case] shape: &str) {
    let fixture = Fixture::new(
        4,
        FragmentMetadataTreeConfig::default(),
        SnapshotPolicy::default(),
    )
    .await;
    let pb::fragment_metadata_tree::Root::InlineRoot(root) = fixture.snapshot.root.clone().unwrap()
    else {
        panic!("expected inline root");
    };
    assert_eq!(root.next_action_sequence, 1, "bootstrap starts at 1");
    assert_eq!(root.children[0].materialized_through_action_sequence, 0);
    let zero = pb::FragmentMetadataMutation {
        action_sequence: 0,
        action: Some(action::remove_fragment(1)),
        fragment_count_delta: -1,
        total_rows_delta: -1,
        visible_rows_delta: -1,
    };
    let mut snapshot = fixture.snapshot.clone();
    match shape {
        "inline_root_with_suffix" => {
            snapshot.mutations_since_root = vec![zero];
            snapshot.next_action_sequence = 1;
        }
        "external_root_suffix_zero" => {
            let store = NodeStore::new(
                fixture.store.clone(),
                fixture.base.clone(),
                fixture.scheduler.clone(),
                Arc::new(LanceCache::with_capacity(0)),
            );
            let (path, _) = store.write_root_base(&root).await.unwrap();
            snapshot.root = Some(pb::fragment_metadata_tree::Root::RootPath(path));
            snapshot.mutations_since_root = vec![zero];
            snapshot.next_action_sequence = 1;
        }
        "root_buffer_zero" => {
            let mut root = root;
            root.buffer = vec![zero];
            snapshot.root = Some(pb::fragment_metadata_tree::Root::InlineRoot(root));
        }
        _ => {
            let mut root = root;
            root.next_action_sequence = 0;
            snapshot.root = Some(pb::fragment_metadata_tree::Root::InlineRoot(root));
            snapshot.next_action_sequence = 0;
        }
    }
    let error = FragmentMetadataTree::open_snapshot(
        fixture.store,
        fixture.base,
        fixture.scheduler,
        Arc::new(LanceCache::with_capacity(0)),
        &snapshot,
        1,
        fixture.config.clone(),
        fixture.tree.next_fragment_id(),
    )
    .await
    .err()
    .expect("sequence 0 must not open");
    assert!(
        matches!(
            error,
            Error::CorruptFile { .. } | Error::InvalidInput { .. }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn first_fences_follow_the_parent_not_the_first_stored_id() {
    let policy = SnapshotPolicy::default();
    let config = FragmentMetadataTreeConfig::new(512, u32::MAX)
        .with_max_leaf_bytes(4096)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(64, config, policy).await;
    assert!(fixture.tree.height() >= 2);
    assert_eq!(fixture.tree.children[0].min_key, 0);
    let actions: Vec<_> = (0..6).map(action::remove_fragment).collect();
    fixture.commit(actions, policy, false).await;
    fixture.commit(Vec::new(), policy, true).await;
    assert_eq!(fixture.tree.children[0].min_key, 0);
    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    let fragments = reopened.materialize().await.unwrap();
    assert_eq!(fragments[0].id, 6);
    reopened.verify_watermarks().await.unwrap();
    let mut snapshot = fixture.snapshot.clone();
    if let Some(pb::fragment_metadata_tree::Root::InlineRoot(root)) = &mut snapshot.root {
        root.children[0].min_key = 6;
    } else {
        panic!("expected inline root");
    }
    let error = FragmentMetadataTree::open_snapshot(
        fixture.store.clone(),
        fixture.base.clone(),
        fixture.scheduler.clone(),
        Arc::new(LanceCache::with_capacity(0)),
        &snapshot,
        fixture.tree.version(),
        fixture.config.clone(),
        fixture.tree.next_fragment_id(),
    )
    .await
    .err()
    .expect("a first fence above the parent's must not open");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
}

#[rstest]
#[case::point("point")]
#[case::stream("stream")]
#[case::offset("offset")]
#[tokio::test]
async fn mutation_at_or_below_its_leaf_watermark_is_rejected(#[case] read: &str) {
    let policy = SnapshotPolicy::default();
    let config = FragmentMetadataTreeConfig::default()
        .with_max_leaf_bytes(4096)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(64, config, policy).await;
    let target = 40;
    fixture
        .commit(
            vec![action::add_fragment(&make_fragment_with_files(target, 4))],
            policy,
            true,
        )
        .await;
    let mut snapshot = fixture.snapshot.clone();
    let Some(pb::fragment_metadata_tree::Root::InlineRoot(root)) = &mut snapshot.root else {
        panic!("expected inline root");
    };
    let leaf = &root.children[node::child_index_for(&root.children, target)];
    assert!(leaf.materialized_through_action_sequence >= 1);
    root.buffer.push(pb::FragmentMetadataMutation {
        action_sequence: leaf.materialized_through_action_sequence,
        action: Some(action::clear_deletion_file(target)),
        fragment_count_delta: 0,
        total_rows_delta: 0,
        visible_rows_delta: 0,
    });
    let tree = fixture.open(&snapshot, fixture.tree.version()).await;
    let error = match read {
        "point" => tree.resolve_fragment(target).await.err(),
        "stream" => tree.iter_fragments().try_collect::<Vec<_>>().await.err(),
        _ => tree.fragment_at_row_offset(target).await.err(),
    }
    .expect("a mutation at the watermark must not replay");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
    assert!(error.to_string().contains("watermark"), "{error}");
}
