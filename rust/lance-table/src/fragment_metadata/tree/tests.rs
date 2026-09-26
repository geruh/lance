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
use object_store::ObjectStoreExt;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use rstest::rstest;

mod ownership;

/// Singleton repair must preserve ancestor-buffered actions without advancing
/// the leaf watermark past them.
#[tokio::test]
async fn singleton_repair_leaves_the_callers_pending_actions_unmaterialized() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig {
        max_node_bytes: 4096,
        max_leaf_bytes: 64 * 1024,
        semantic_buffer_bytes: 2048,
        ..FragmentTreeConfig::default()
    };
    // Draining `narrow` leaves a singleton. A pending bucket under `wide`
    // crosses the drain threshold only after the two directories merge.
    const NARROW_LEAF: u64 = 64;
    const WIDE_LEAF: u64 = 224;
    let total = 2 * NARROW_LEAF + 3 * WIDE_LEAF;
    let mut fixture = Fixture::new(total, config.clone(), policy).await;
    let mut expected: BTreeMap<u64, Fragment> = fixture
        .tree
        .materialize()
        .await
        .unwrap()
        .into_iter()
        .map(|fragment| (fragment.id, fragment))
        .collect();
    let watermark = fixture.tree.next_action_sequence - 1;
    let mut leaves = Vec::new();
    let mut start = 0;
    for count in [NARROW_LEAF, NARROW_LEAF, WIDE_LEAF, WIDE_LEAF, WIDE_LEAF] {
        let fragments: Vec<Fragment> = (start..start + count)
            .map(|id| expected[&id].clone())
            .collect();
        let written = fixture
            .tree
            .store
            .write_leaves(&fragments, watermark, &config)
            .await
            .unwrap();
        assert_eq!(written.len(), 1, "each range must encode as one leaf");
        leaves.push(written.into_iter().next().unwrap().child_ref);
        start += count;
    }
    let (narrow_leaves, wide_leaves) = leaves.split_at(2);
    for leaf in narrow_leaves {
        assert!(
            node::is_underflow(leaf, &config),
            "narrow leaves must underflow so they coalesce: {leaf:?}"
        );
    }
    let mut pending = Vec::new();
    let mut next_action_sequence = fixture.tree.next_action_sequence;
    let mut pending_files = BTreeMap::new();
    let merged_children: Vec<_> = std::iter::once(narrow_leaves[0].clone())
        .chain(wide_leaves.iter().cloned())
        .collect();
    for leaf in wide_leaves {
        let mut bucket = Vec::new();
        let mut id = leaf.min_key;
        // A small margin covers the routing bytes the coalesced leaf changes.
        while node::internal_logical_bytes(&[], &bucket)
            < flush::amortization_gate(flush::fair_share(&merged_children, &config), leaf) + 64
        {
            let file = make_backfill_data_file(id, 1);
            bucket.push(pb::FragmentTreeMutation {
                action_sequence: next_action_sequence,
                action: Some(action::add_data_file(id, &file)),
                ..Default::default()
            });
            pending_files.insert(id, file);
            next_action_sequence += 1;
            id += 1;
        }
        assert!(
            node::internal_logical_bytes(&[], &bucket)
                < flush::amortization_gate(flush::fair_share(wide_leaves, &config), leaf),
            "the bucket must sit below the gate of its own node and above the merged node's"
        );
        pending.extend(bucket);
    }
    let narrow = fixture
        .tree
        .store
        .write_internal(narrow_leaves.to_vec(), Vec::new())
        .await
        .unwrap()
        .child_ref;
    let wide = fixture
        .tree
        .store
        .write_internal(wide_leaves.to_vec(), pending.clone())
        .await
        .unwrap()
        .child_ref;
    assert!(
        !node::internal_overflows(wide_leaves, &pending, &config)
            && node::buffer_pressured(&pending, &config),
        "the wide node must be pressured yet unable to drain any child"
    );
    fixture.tree.children = vec![narrow, wide];
    fixture.tree.buffer.clear();
    fixture.tree.buffer_index.take();
    fixture.tree.next_action_sequence = next_action_sequence;
    fixture.tree.store.next_action_sequence = next_action_sequence;
    fixture.snapshot = pb::FragmentTree {
        root: Some(pb::fragment_tree::Root::InlineRoot(
            fixture.tree.compacted_root(),
        )),
        mutations_since_root: Vec::new(),
        next_action_sequence,
    };
    fixture.tree.snapshot = Some(Box::new(fixture.snapshot.clone()));
    fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;

    // One residual action under `wide` stays in the root because it is below
    // the root's gate for that child. Enough actions under `narrow` pressure
    // the root, drain into `narrow`, rewrite both of its leaves, and leave
    // them small enough to coalesce into one.
    let residual = 2 * NARROW_LEAF + 2 * WIDE_LEAF + WIDE_LEAF / 2;
    let mut commit_files = vec![(residual, make_backfill_data_file(residual, 2))];
    let mut actions = vec![action::add_data_file(residual, &commit_files[0].1)];
    while node::internal_logical_bytes(&[], &pending_for(&actions)) < config.semantic_buffer_bytes {
        let id = actions.len() as u64 - 1;
        let file = make_backfill_data_file(id, 2);
        actions.push(action::add_data_file(id, &file));
        commit_files.push((id, file));
    }
    let narrow_actions = actions.len() as u64 - 1;
    assert!(
        narrow_actions < 2 * NARROW_LEAF,
        "the pressuring actions must all route under the narrow node"
    );
    for (id, file) in pending_files
        .iter()
        .chain(commit_files.iter().map(|(id, file)| (id, file)))
    {
        expected.get_mut(id).unwrap().files.push(file.clone());
    }
    let stats = fixture.commit(actions, policy, false).await;
    assert_eq!(
        stats.messages_materialized, narrow_actions,
        "only the actions drained into the narrow node may reach a leaf"
    );
    assert!(stats.merges >= 1, "the singleton must have been repaired");

    let reader = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    reader.verify_watermarks().await.unwrap();
    let shape = reader.shape_report().await.unwrap();
    assert_eq!(
        shape.root_buffer_len + shape.node_buffer_lens.iter().sum::<u64>(),
        pending.len() as u64 + 1,
        "the wide node's pending actions and the residual must still be buffered"
    );
    let resolved = reader.resolve_fragment(residual).await.unwrap().unwrap();
    assert_eq!(resolved, expected[&residual]);
    assert_eq!(
        reader.materialize().await.unwrap(),
        expected.into_values().collect::<Vec<_>>()
    );
}

fn pending_for(actions: &[pb::FragmentAction]) -> Vec<pb::FragmentTreeMutation> {
    actions
        .iter()
        .map(|action| pb::FragmentTreeMutation {
            action: Some(action.clone()),
            ..Default::default()
        })
        .collect()
}

struct Fixture {
    tree: FragmentTree,
    config: FragmentTreeConfig,
    snapshot: pb::FragmentTree,
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
    let fixture = Fixture::new(2, FragmentTreeConfig::default(), SnapshotPolicy::default()).await;
    let mut snapshot = fixture.snapshot.clone();
    let Some(pb::fragment_tree::Root::InlineRoot(root)) = &mut snapshot.root else {
        panic!("expected inline root");
    };
    if oversized_child {
        root.children[0].object_size = 0;
    } else {
        root.next_action_sequence = 0;
    }
    let result = FragmentTree::open_snapshot(
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
    async fn new(count: u64, config: FragmentTreeConfig, policy: SnapshotPolicy) -> Self {
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
        let (tree, snapshot, _) = FragmentTree::bootstrap_snapshot(
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

    async fn open(&self, snapshot: &pb::FragmentTree, version: u64) -> FragmentTree {
        FragmentTree::open_snapshot(
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
    // Removing under 1/16 of a leaf stays buffered, which this test needs.
    let mut fixture = Fixture::new(64, FragmentTreeConfig::default(), checkpoint).await;
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
        (2..64).map(make_fragment).collect::<Vec<_>>()
    );

    let mut snapshot = fixture.snapshot.clone();
    match count {
        "Fragments" => snapshot.mutations_since_root[0].fragment_count_delta = -999,
        "physical rows" => snapshot.mutations_since_root[0].total_rows_delta = -999,
        "visible rows" => snapshot.mutations_since_root[0].visible_rows_delta = -999,
        _ => unreachable!(),
    }
    let error = FragmentTree::open_snapshot(
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
    let config = FragmentTreeConfig::new(512, u32::MAX).with_max_leaf_bytes(6 * 1024);
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
            changes.push(action::upsert_fragment(&fragment));
            expected.insert(id, fragment);
        }
        let id = rng.random_range(0..48);
        let mut fragment = make_fragment(id);
        fragment.files[0].path = format!("round-{round}-{id}.lance");
        changes.push(action::upsert_fragment(&fragment));
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
    let config = FragmentTreeConfig::new(512, u32::MAX).with_max_leaf_bytes(6 * 1024);
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
    let mut fixture = Fixture::new(4, FragmentTreeConfig::default(), policy).await;
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
        child_buffer.push(pb::FragmentTreeMutation {
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
        fixture.tree.buffer.push(pb::FragmentTreeMutation {
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
    historical.root = Some(pb::fragment_tree::Root::InlineRoot(
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
    snapshot.root = Some(pb::fragment_tree::Root::InlineRoot(
        fixture.tree.compacted_root(),
    ));
    let reopened = fixture.open(&snapshot, 1).await;
    assert_eq!(reopened.materialize().await.unwrap(), expected);
    let old = fixture.open(&historical, 1).await;
    assert_eq!(old.height(), 2);
    assert_eq!(old.materialize().await.unwrap(), expected);
}

/// A buffered commit on a tree whose root routes through interiors reads
/// nothing when the joined level could not fit the fanout limit. The child
/// references already record each interior's fanout.
#[tokio::test]
async fn small_commit_under_a_routed_root_reads_no_node() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default().with_max_leaf_bytes(1);
    let mut fixture = Fixture::new(300, config, policy).await;
    assert!(fixture.tree.height() >= 3, "{}", fixture.tree.height());
    let actions = vec![action::upsert_fragment(&make_fragment(300))];
    let touched = fixture.tree.resolve_touched(&[300]).await.unwrap();
    fixture.io.incremental_stats();
    let (snapshot, _) = fixture
        .tree
        .prepare_snapshot(
            ValidatedCommit::fragment_actions(actions),
            &touched,
            &fixture.snapshot.clone(),
            policy,
            false,
        )
        .await
        .unwrap();
    assert_eq!(fixture.io.incremental_stats().read_iops, 0);
    let reopened = fixture.open(&snapshot, fixture.tree.version()).await;
    assert_eq!(
        reopened.materialize().await.unwrap(),
        (0..301).map(make_fragment).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn bootstrap_packs_compressed_batches_within_the_leaf_target() {
    let config = FragmentTreeConfig::default();
    let fixture = Fixture::new(0, config.clone(), SnapshotPolicy::default()).await;
    let fragments: Vec<_> = (5_000_000..5_000_216)
        .map(|id| make_fragment_with_files(id, 128))
        .collect();
    let encoded = fixture.tree.store.encode_leaf(&fragments).await.unwrap();
    assert!(node::leaf_logical_bytes(&fragments) > config.max_leaf_bytes * 2);
    assert!(encoded.len() as u64 <= config.max_leaf_bytes);
    fixture.io.incremental_stats();
    let (tree, stats) = FragmentTree::build(fixture.tree.store, config.clone(), fragments.clone())
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
    let config = FragmentTreeConfig::new(512, u32::MAX).with_max_leaf_bytes(6 * 1024);
    let mut fixture = Fixture::new(64, config, policy).await;
    let mut expected: Vec<_> = (0..64).map(make_fragment).collect();
    let mut actions = Vec::new();
    for fragment in expected.iter_mut().take(8) {
        let file = crate::fragment_metadata::support::make_backfill_data_file(fragment.id, 0);
        actions.push(action::add_data_file(fragment.id, &file));
        fragment.files.push(file);
    }
    actions.push(action::remove_fragment(63));
    actions.push(action::upsert_fragment(&make_fragment(64)));
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
    let config = FragmentTreeConfig::new(1024, u32::MAX)
        .with_max_leaf_bytes(4096)
        .with_hard_capacity_bytes(8192);
    let mut fixture = Fixture::new(2, config, policy).await;
    let touched = fixture.tree.resolve_touched(&[9]).await.unwrap();
    let error = fixture
        .tree
        .prepare_snapshot(
            ValidatedCommit::fragment_actions(vec![action::upsert_fragment(
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
        .commit(
            vec![action::upsert_fragment(&make_fragment(2))],
            policy,
            false,
        )
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
    // The scenario needs every leaf directly under the root, so the fanout
    // cap is lifted; the default cap would split this wide directory.
    let config = FragmentTreeConfig {
        max_children_per_node: u32::MAX,
        ..FragmentTreeConfig::new(16 * 1024, u32::MAX)
            .with_max_leaf_bytes(6 * 1024)
            .with_semantic_buffer_bytes(1)
    };
    let mut fixture = Fixture::new(128, config, policy).await;
    assert_eq!(fixture.tree.height(), 1);
    assert!(fixture.tree.children.len() > 1);
    let original = fixture.tree.materialize().await.unwrap();
    let previous = fixture.snapshot.clone();
    let mut expected = Vec::new();
    let mut actions = Vec::new();
    for fragment in &original {
        let child = node::child_index_for(&fixture.tree.children, fragment.id);
        // Each leaf keeps its first record, so every drain reads its leaf and
        // the reads can overlap.
        if fragment.id == fixture.tree.children[child].min_key {
            expected.push(fragment.clone());
            continue;
        }
        if change_shape && child.is_multiple_of(2) {
            actions.push(action::remove_fragment(fragment.id));
        } else {
            let replacement =
                make_fragment_with_files(fragment.id, if change_shape { 16 } else { 2 });
            actions.push(action::upsert_fragment(&replacement));
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
    let config = FragmentTreeConfig::new(512, u32::MAX)
        .with_max_leaf_bytes(4096)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(32, config, policy).await;
    assert!(fixture.tree.height() >= 2);
    let mut expected: BTreeMap<_, _> = (0..32).map(|id| (id, make_fragment(id))).collect();
    let mut fragment = make_fragment_with_files(7, 2);
    fragment.files[0].path = "A".into();
    fragment.files[1].path = "B".into();
    fixture
        .commit(vec![action::upsert_fragment(&fragment)], policy, bulk)
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
        FragmentTreeConfig::new(1024, u32::MAX).with_max_leaf_bytes(4096),
        SnapshotPolicy::default(),
    )
    .await;
    fixture
        .commit(
            vec![
                action::remove_fragment(7),
                action::upsert_fragment(&make_fragment(90)),
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
    let config = FragmentTreeConfig::default()
        .with_max_leaf_bytes(16 * 1024)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(512, config.clone(), policy).await;
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
    let fixture = Fixture::new(4, FragmentTreeConfig::default(), SnapshotPolicy::default()).await;
    let pb::fragment_tree::Root::InlineRoot(root) = fixture.snapshot.root.clone().unwrap() else {
        panic!("expected inline root");
    };
    assert_eq!(root.next_action_sequence, 1, "bootstrap starts at 1");
    assert_eq!(root.children[0].materialized_through_action_sequence, 0);
    let zero = pb::FragmentTreeMutation {
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
            snapshot.root = Some(pb::fragment_tree::Root::RootUuid(path));
            snapshot.mutations_since_root = vec![zero];
            snapshot.next_action_sequence = 1;
        }
        "root_buffer_zero" => {
            let mut root = root;
            root.buffer = vec![zero];
            snapshot.root = Some(pb::fragment_tree::Root::InlineRoot(root));
        }
        _ => {
            let mut root = root;
            root.next_action_sequence = 0;
            snapshot.root = Some(pb::fragment_tree::Root::InlineRoot(root));
            snapshot.next_action_sequence = 0;
        }
    }
    let error = FragmentTree::open_snapshot(
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
    let config = FragmentTreeConfig::new(512, u32::MAX)
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
    if let Some(pb::fragment_tree::Root::InlineRoot(root)) = &mut snapshot.root {
        root.children[0].min_key = 6;
    } else {
        panic!("expected inline root");
    }
    let error = FragmentTree::open_snapshot(
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

/// Removing everything below the last routing child empties the leading
/// subtrees in one drain. The surviving interior inherits the root's lower
/// bound, so the fence stored on its own first child must move with it.
#[tokio::test]
async fn draining_leading_subtrees_moves_the_surviving_interior_fence() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::new(512, u32::MAX)
        .with_max_leaf_bytes(4096)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(512, config, policy).await;
    assert!(fixture.tree.height() >= 2);
    let survivor = fixture.tree.children.last().unwrap();
    assert!(survivor.height >= 1 && survivor.min_key > 0);
    let first_survivor = survivor.min_key;
    let actions: Vec<_> = (0..first_survivor).map(action::remove_fragment).collect();
    fixture.commit(actions, policy, false).await;

    assert_eq!(fixture.tree.children[0].min_key, 0);
    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    reopened.verify_reachable().await.unwrap();
    reopened.verify_watermarks().await.unwrap();
    assert!(reopened.buffered_action_keys().await.unwrap().is_empty());
    let ids: Vec<_> = reopened
        .materialize()
        .await
        .unwrap()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    assert_eq!(ids, (first_survivor..512).collect::<Vec<_>>());
}

/// A create buffered for a leaf that later loses every stored fragment lands
/// in that leaf's replacement, never in a sibling whose watermark already
/// covers the create.
#[tokio::test]
async fn emptied_leaf_keeps_its_buffered_creates_under_its_own_watermark() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default().with_max_leaf_bytes(16 * 1024);
    let mut fixture = Fixture::new(256, config, policy).await;
    assert_eq!(fixture.tree.height(), 1);
    assert!(fixture.tree.root_child_count() >= 2);
    let last = fixture.tree.children.last().unwrap().min_key;
    let second = fixture.tree.children[1].min_key;
    let fresh = fixture.tree.next_fragment_id();
    fixture
        .commit(
            vec![action::upsert_fragment(&make_fragment(fresh))],
            policy,
            false,
        )
        .await;
    assert_eq!(fixture.tree.root_buffer_len(), 1);
    // Removing a quarter of the first leaf drains only that leaf.
    let removed = second / 4;
    fixture
        .commit(
            (0..removed).map(action::remove_fragment).collect(),
            policy,
            false,
        )
        .await;
    assert_eq!(fixture.tree.root_buffer_len(), 1);
    fixture
        .commit(
            (last..fresh).map(action::remove_fragment).collect(),
            policy,
            false,
        )
        .await;

    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    reopened.verify_watermarks().await.unwrap();
    reopened.verify_reachable().await.unwrap();
    let ids: Vec<_> = reopened
        .materialize()
        .await
        .unwrap()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    let expected: Vec<_> = (removed..last).chain([fresh]).collect();
    assert_eq!(ids, expected);
}

/// One commit shrinks the first routing child to a single leaf while appends
/// split the last child past the fanout, so the root must split too. The
/// shrunken child is repaired before the root splits, or a split piece would
/// persist a single-child interior that no reader accepts.
#[tokio::test]
async fn contraction_and_root_split_in_one_commit_keep_nodes_readable() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::new(64 * 1024, 4)
        .with_max_leaf_bytes(16 * 1024)
        .with_semantic_buffer_bytes(1);
    let mut full_root = None;
    for count in (64..4096).step_by(32) {
        let fixture = Fixture::new(count, config.clone(), policy).await;
        if fixture.tree.height() == 2 && fixture.tree.root_child_count() == 4 {
            full_root = Some(fixture);
            break;
        }
    }
    let mut fixture = full_root.expect("some table size gives a two-level tree with a full root");
    let first = fixture
        .tree
        .store
        .read_internal(&fixture.tree.children[0])
        .await
        .unwrap();
    assert!(
        first.children.len() >= 2,
        "the first child needs several leaves"
    );
    let last_leaf_start = first.children.last().unwrap().min_key;
    let fresh = fixture.tree.next_fragment_id();
    let appended = fresh;
    let mut actions: Vec<_> = (0..last_leaf_start).map(action::remove_fragment).collect();
    actions.extend((fresh..fresh + appended).map(|id| action::upsert_fragment(&make_fragment(id))));
    fixture.commit(actions, policy, false).await;

    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    reopened.verify_reachable().await.unwrap();
    reopened.verify_watermarks().await.unwrap();
    let ids: Vec<_> = reopened
        .materialize()
        .await
        .unwrap()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    let expected: Vec<_> = (last_leaf_start..fresh + appended).collect();
    assert_eq!(ids, expected);
}

/// A point lookup decodes one row of its leaf, yet returns exactly what a
/// whole-leaf read holds, for every id and for an id past the last one.
#[rstest]
#[case::several_leaves(16 * 1024)]
#[case::one_leaf(4 * 1024 * 1024)]
#[tokio::test]
async fn point_reads_match_whole_leaf_reads(#[case] leaf_bytes: u64) {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default().with_max_leaf_bytes(leaf_bytes);
    let fixture = Fixture::new(300, config, policy).await;
    let whole = fixture.tree.materialize().await.unwrap();
    for fragment in &whole {
        assert_eq!(
            fixture
                .tree
                .resolve_fragment(fragment.id)
                .await
                .unwrap()
                .as_ref(),
            Some(fragment)
        );
    }
    assert!(fixture.tree.resolve_fragment(300).await.unwrap().is_none());
}

/// Trees that share a leaf cache stop reading a leaf from storage once it has
/// been decoded twice. The cache never stands in for storage when a tree
/// proves its objects exist.
#[tokio::test]
async fn shared_leaf_cache_never_hides_a_missing_leaf() {
    let config = FragmentTreeConfig::default().with_max_leaf_bytes(16 * 1024);
    let fixture = Fixture::new(300, config, SnapshotPolicy::default()).await;
    let leaves = LanceCache::with_capacity(64 * 1024 * 1024);
    let mut first = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    first.set_leaf_cache(leaves.clone());
    let mut second = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    second.set_leaf_cache(leaves);
    let expected = first.materialize().await.unwrap();
    assert_eq!(second.materialize().await.unwrap(), expected);
    assert!(first.root_child_count() >= 2);

    fixture.io.incremental_stats();
    assert_eq!(second.materialize().await.unwrap(), expected);
    let leaf_reads = fixture
        .io
        .incremental_stats()
        .requests
        .into_iter()
        .filter(|request| request.path.as_ref().contains("_bt/leaf/"))
        .count();
    assert_eq!(leaf_reads, 0);

    let leaf = fixture
        .tree
        .node_paths()
        .await
        .unwrap()
        .into_iter()
        .find(|path| path.starts_with("_bt/leaf/"))
        .unwrap();
    let path: Path = fixture
        .base
        .parts()
        .chain(Path::from(leaf).parts())
        .collect();
    fixture.store.inner.delete(&path).await.unwrap();
    assert_eq!(second.materialize().await.unwrap(), expected);
    assert!(second.verify_reachable().await.is_err());
}

/// A batch that overwrites every record of every leaf determines the new
/// leaves, so the drain reads each old leaf once, for the record headers that
/// check the stored deltas, and never decodes its files.
#[tokio::test]
async fn wholly_replaced_leaves_rebuild_from_record_headers() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default()
        .with_max_leaf_bytes(16 * 1024)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(256, config, policy).await;
    let leaves = fixture.tree.root_child_count();
    assert!(leaves >= 2);
    assert_eq!(fixture.tree.height(), 1);
    let expected: Vec<_> = (0..256).map(|id| make_fragment_with_files(id, 2)).collect();
    let actions: Vec<_> = expected.iter().map(action::upsert_fragment).collect();
    let ids: Vec<_> = (0..256).collect();
    let touched = fixture.tree.resolve_touched(&ids).await.unwrap();
    let previous = fixture.snapshot.clone();
    let io = IOTracker::default();
    let mut store = fixture.store.as_ref().clone();
    store.apply_wrapper(&io);
    fixture.tree = fixture.tree.with_object_store(Arc::new(store));
    let (snapshot, _) = fixture
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

    let leaf_reads: Vec<_> = io
        .incremental_stats()
        .requests
        .into_iter()
        .filter(|request| {
            request.method.starts_with("get") && request.path.as_ref().contains("_bt/leaf/")
        })
        .collect();
    assert_eq!(leaf_reads.len(), leaves, "{leaf_reads:?}");
    fixture.snapshot = snapshot;
    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    reopened.verify_watermarks().await.unwrap();
    assert_eq!(reopened.materialize().await.unwrap(), expected);
}

/// Check removals against the stored record headers before rebuilding or
/// retiring a leaf.
#[rstest]
#[case::rebuilt_leaf(true)]
#[case::dropped_leaf(false)]
#[tokio::test]
async fn drain_rejects_a_removal_of_a_fragment_the_leaf_lacks(#[case] creates: bool) {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default().with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(8, config, policy).await;
    fixture
        .commit(vec![action::remove_fragment(7)], policy, true)
        .await;
    let mut snapshot = fixture.snapshot.clone();
    let Some(pb::fragment_tree::Root::InlineRoot(root)) = &mut snapshot.root else {
        panic!("expected inline root");
    };
    assert_eq!(root.children.len(), 1);
    assert_eq!(root.children[0].num_keys, 7);
    // Fragments 0 to 5 are removed, and so is fragment 7, which the leaf no
    // longer holds. Fragment 6 is never named.
    let mut actions: Vec<_> = (0..6)
        .chain([7])
        .map(|id| (action::remove_fragment(id), -1))
        .collect();
    if creates {
        actions.push((action::upsert_fragment(&make_fragment(8)), 1));
    }
    for (action, delta) in actions {
        root.buffer.push(pb::FragmentTreeMutation {
            action_sequence: root.next_action_sequence,
            action: Some(action),
            fragment_count_delta: delta,
            total_rows_delta: delta,
            visible_rows_delta: delta,
        });
        root.next_action_sequence += 1;
    }
    snapshot.next_action_sequence = root.next_action_sequence;
    let mut tree = FragmentTree::open_snapshot(
        fixture.store.clone(),
        fixture.base.clone(),
        fixture.scheduler.clone(),
        Arc::new(LanceCache::with_capacity(0)),
        &snapshot,
        fixture.tree.version(),
        fixture.config.clone(),
        9,
    )
    .await
    .unwrap();
    let touched = tree.resolve_touched(&[]).await.unwrap();
    let error = tree
        .prepare_snapshot(
            ValidatedCommit::fragment_actions(Vec::new()),
            &touched,
            &snapshot,
            policy,
            false,
        )
        .await
        .expect_err("a removal of an absent fragment must not retire the leaf");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
}

/// Check removals against the stored records before retiring an entire subtree.
#[tokio::test]
async fn drain_rejects_a_removal_that_would_drop_a_subtree_it_does_not_cover() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::new(512, u32::MAX)
        .with_max_leaf_bytes(6 * 1024)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(64, config, policy).await;
    fixture
        .commit(vec![action::remove_fragment(0)], policy, true)
        .await;
    let mut snapshot = fixture.snapshot.clone();
    let Some(pb::fragment_tree::Root::InlineRoot(root)) = &mut snapshot.root else {
        panic!("expected inline root");
    };
    assert!(root.children[0].height > 0);
    let end = root.children[1].min_key;
    assert_eq!(root.children[0].num_keys, end - 1);
    // Every stored fragment of the first subtree is removed except the last,
    // and fragment 0, which the subtree no longer holds, is removed too.
    for id in (0..end - 1).collect::<Vec<_>>() {
        root.buffer.push(pb::FragmentTreeMutation {
            action_sequence: root.next_action_sequence,
            action: Some(action::remove_fragment(id)),
            fragment_count_delta: -1,
            total_rows_delta: -1,
            visible_rows_delta: -1,
        });
        root.next_action_sequence += 1;
    }
    snapshot.next_action_sequence = root.next_action_sequence;
    let mut tree = fixture.open(&snapshot, fixture.tree.version()).await;
    let touched = tree.resolve_touched(&[]).await.unwrap();
    let error = tree
        .prepare_snapshot(
            ValidatedCommit::fragment_actions(Vec::new()),
            &touched,
            &snapshot,
            policy,
            false,
        )
        .await
        .expect_err("a removal of an absent fragment must not retire the subtree");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
}

/// A drain whose batch claims to replace every stored record still checks
/// each stored delta against the record headers. A buffer that counts an
/// insert as a replacement would otherwise drop a fragment nothing targets.
#[tokio::test]
async fn drain_rejects_a_replacement_that_misstates_its_delta() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default().with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(8, config, policy).await;
    fixture
        .commit(vec![action::remove_fragment(7)], policy, true)
        .await;
    let mut snapshot = fixture.snapshot.clone();
    let Some(pb::fragment_tree::Root::InlineRoot(root)) = &mut snapshot.root else {
        panic!("expected inline root");
    };
    assert_eq!(root.children.len(), 1);
    assert_eq!(root.children[0].num_keys, 7);
    // Six stored fragments are replaced, and fragment 7, which the leaf no
    // longer holds, is inserted with the zero delta of a replacement.
    for id in (0..6).chain([7]) {
        root.buffer.push(pb::FragmentTreeMutation {
            action_sequence: root.next_action_sequence,
            action: Some(action::upsert_fragment(&make_fragment_with_files(id, 2))),
            fragment_count_delta: 0,
            total_rows_delta: 0,
            visible_rows_delta: 0,
        });
        root.next_action_sequence += 1;
    }
    snapshot.next_action_sequence = root.next_action_sequence;
    let mut tree = fixture.open(&snapshot, fixture.tree.version()).await;
    let touched = tree.resolve_touched(&[]).await.unwrap();
    let error = tree
        .prepare_snapshot(
            ValidatedCommit::fragment_actions(Vec::new()),
            &touched,
            &snapshot,
            policy,
            false,
        )
        .await
        .expect_err("a misstated replacement must not rebuild the leaf");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
}

/// Removing all but one fragment from a tree three or more levels deep leaves
/// single-child interiors that no sibling can absorb. The commit rebuilds in
/// bulk rather than persisting that chain.
#[tokio::test]
async fn collapsing_to_one_leaf_rebuilds_instead_of_persisting_a_chain() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::new(512, u32::MAX)
        .with_max_leaf_bytes(1024)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(512, config, policy).await;
    assert!(
        fixture.tree.height() >= 3,
        "height {}",
        fixture.tree.height()
    );
    let survivor = 300;
    let actions: Vec<_> = (0..512)
        .filter(|id| *id != survivor)
        .map(action::remove_fragment)
        .collect();
    fixture.commit(actions, policy, false).await;

    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    reopened.verify_reachable().await.unwrap();
    reopened.verify_watermarks().await.unwrap();
    assert_eq!(reopened.height(), 1);
    let ids: Vec<_> = reopened
        .materialize()
        .await
        .unwrap()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    assert_eq!(ids, vec![survivor]);
}

/// Regrowth from a childless root lifts fresh leaves into interior nodes once
/// they exceed the fanout. An interior node stores its children's fences and a
/// reader checks the first stored fence against the parent's entry, so the
/// first leaf must carry the root's lower bound before the lift. A first live
/// id above zero is what exposes the difference.
#[tokio::test]
async fn childless_root_regrowth_that_splits_the_root_reopens() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default()
        .with_max_leaf_bytes(4096)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(0, config, policy).await;
    assert!(fixture.tree.children.is_empty());
    let ids: Vec<u64> = (100..120).collect();
    let actions: Vec<_> = ids
        .iter()
        .map(|id| action::upsert_fragment(&make_fragment(*id)))
        .collect();
    fixture.commit(actions, policy, false).await;
    assert!(
        fixture.tree.height() >= 2,
        "twenty singleton leaves must split the root at fanout {}",
        fixture.config.max_children_per_node
    );
    assert_eq!(fixture.tree.children[0].min_key, 0);
    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    reopened.verify_reachable().await.unwrap();
    let regrown: Vec<u64> = reopened
        .materialize()
        .await
        .unwrap()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    assert_eq!(regrown, ids);
}

/// Every read rejects a buffered action at or below its leaf's watermark,
/// including the leaf-prefetching streams that eager loads and cleanup use,
/// the set reads commits resolve through, and a point read whose newest
/// action replaces the whole record.
#[rstest]
#[case::point("point")]
#[case::point_after_reset("point_after_reset")]
#[case::stream("stream")]
#[case::offset("offset")]
#[case::prefetch_stream("prefetch_stream")]
#[case::fragment_stream("fragment_stream")]
#[case::resolve_fragments("resolve_fragments")]
#[case::resolve_touched("resolve_touched")]
#[case::materialize("materialize")]
#[tokio::test]
async fn mutation_at_or_below_its_leaf_watermark_is_rejected(#[case] read: &str) {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default()
        .with_max_leaf_bytes(4096)
        .with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(8, config, policy).await;
    let target = 4;
    fixture
        .commit(
            vec![action::upsert_fragment(&make_fragment_with_files(
                target, 4,
            ))],
            policy,
            true,
        )
        .await;
    let mut snapshot = fixture.snapshot.clone();
    let Some(pb::fragment_tree::Root::InlineRoot(root)) = &mut snapshot.root else {
        panic!("expected inline root");
    };
    let leaf = &root.children[node::child_index_for(&root.children, target)];
    assert!(leaf.materialized_through_action_sequence >= 1);
    let stale = if read == "point_after_reset" {
        action::upsert_fragment(&make_fragment_with_files(target, 4))
    } else {
        action::clear_deletion_file(target)
    };
    root.buffer.push(pb::FragmentTreeMutation {
        action_sequence: leaf.materialized_through_action_sequence,
        action: Some(stale),
        fragment_count_delta: 0,
        total_rows_delta: 0,
        visible_rows_delta: 0,
    });
    let tree = fixture.open(&snapshot, fixture.tree.version()).await;
    let error = match read {
        "point" | "point_after_reset" => tree.resolve_fragment(target).await.err(),
        "resolve_fragments" => tree
            .resolve_fragments(&RoaringBitmap::from_iter([target as u32]))
            .await
            .err(),
        "resolve_touched" => tree.resolve_touched(&[target]).await.err(),
        "materialize" => tree.materialize().await.err(),
        "stream" => tree.iter_fragments().try_collect::<Vec<_>>().await.err(),
        "prefetch_stream" => Arc::new(tree)
            .fragment_stream_with_prefetch(4)
            .try_collect::<Vec<_>>()
            .await
            .err(),
        "fragment_stream" => Arc::new(tree)
            .fragment_stream()
            .try_collect::<Vec<_>>()
            .await
            .err(),
        _ => tree.fragment_at_row_offset(target).await.err(),
    }
    .expect("a mutation at the watermark must not replay");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
    assert!(error.to_string().contains("watermark"), "{error}");
}

/// With a node budget that fits exactly two child references per split piece,
/// greedy packing leaves a one-child piece at the right edge. No split may
/// persist it, since the format rejects single-child interiors on read.
#[rstest]
#[case::bootstrap("bootstrap")]
#[case::bulk_rebuild("bulk")]
#[case::two_level_lift("buffered")]
#[case::interior_drain("interior")]
#[tokio::test]
async fn splits_never_persist_a_single_child_interior(#[case] path: &str) {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::new(300, 16).with_max_leaf_bytes(1);
    let (count, added) = match path {
        "bootstrap" => (7, 0),
        "bulk" => (4, 1),
        "interior" => (8, 7),
        _ => (4, 7),
    };
    let mut fixture = Fixture::new(count, config, policy).await;
    if path == "interior" {
        assert!(fixture.tree.height() >= 2);
    }
    if added > 0 {
        let actions = (count..count + added)
            .map(|id| action::upsert_fragment(&make_fragment(id)))
            .collect();
        let stats = fixture.commit(actions, policy, path == "bulk").await;
        if path == "interior" {
            assert!(stats.max_flush_depth > 0);
            assert!(stats.splits > 0);
        }
    }
    let reopened = fixture
        .open(&fixture.snapshot, fixture.tree.version())
        .await;
    assert_eq!(
        reopened.materialize().await.unwrap(),
        (0..count + added).map(make_fragment).collect::<Vec<_>>()
    );
    reopened.verify_reachable().await.unwrap();
    reopened.verify_watermarks().await.unwrap();
    assert!(
        reopened
            .shape_report()
            .await
            .unwrap()
            .node_fanouts
            .iter()
            .all(|fanout| (2..=16).contains(fanout))
    );
}

/// A node budget whose split piece holds one child reference cannot build
/// routing. Bootstrap and a root lift refuse it instead of publishing
/// single-child interiors that no reader can open.
#[rstest]
#[case::bootstrap("bootstrap")]
#[case::root_lift("lift")]
#[tokio::test]
async fn budget_that_cannot_pair_child_references_is_refused(#[case] path: &str) {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::new(200, 16).with_max_leaf_bytes(1);
    let error = if path == "bootstrap" {
        let (store, base) = ObjectStore::from_uri("memory://").await.unwrap();
        let scheduler = ScanScheduler::new(store.clone(), SchedulerConfig::default_for_testing());
        FragmentTree::bootstrap_snapshot(
            store,
            base,
            scheduler,
            Arc::new(LanceCache::with_capacity(0)),
            config,
            (0..4).map(make_fragment).collect(),
            1,
            policy,
        )
        .await
        .err()
    } else {
        let mut fixture = Fixture::new(2, config, policy).await;
        let actions: Vec<_> = (2..4)
            .map(|id| action::upsert_fragment(&make_fragment(id)))
            .collect();
        let touched = fixture.tree.resolve_touched(&[2, 3]).await.unwrap();
        fixture
            .tree
            .prepare_snapshot(
                ValidatedCommit::fragment_actions(actions),
                &touched,
                &fixture.snapshot.clone(),
                policy,
                false,
            )
            .await
            .err()
    }
    .expect("a budget that cannot pair child references must be refused");
    assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
}

/// A drain rejects a leaf that stores a fragment at or above the end of the
/// range its parent assigned. Rewriting it would otherwise publish a piece
/// fenced past its right sibling, or keep a fragment point reads never find.
#[tokio::test]
async fn drain_rejects_a_leaf_storing_keys_past_its_range_end() {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default().with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(1, config, policy).await;
    let mut leaves = Vec::new();
    for ids in [vec![0], vec![60, 150], vec![100, 120]] {
        let fragments: Vec<_> = ids.into_iter().map(make_fragment).collect();
        let written = fixture
            .tree
            .store
            .write_leaves(&fragments, 0, &fixture.tree.config)
            .await
            .unwrap();
        leaves.push(written.into_iter().next().unwrap().child_ref);
    }
    // The middle leaf owns [50, 100) but stores 150, which its sibling's
    // range covers.
    leaves[1].min_key = 50;
    let tree = &mut fixture.tree;
    tree.children = leaves;
    // Enough inserts into the middle leaf's range that draining it pays.
    tree.buffer = (61..91)
        .zip(1..)
        .map(|(id, action_sequence)| pb::FragmentTreeMutation {
            action_sequence,
            action: Some(action::upsert_fragment(&make_fragment(id))),
            fragment_count_delta: 1,
            total_rows_delta: 1,
            visible_rows_delta: 1,
        })
        .collect();
    tree.buffer_index.take();
    tree.total_fragments = 35;
    tree.total_rows = 35;
    tree.visible_rows = 35;
    tree.next_action_sequence = 31;
    tree.store.next_action_sequence = 31;
    let error = tree
        .rewrite_tree()
        .await
        .expect_err("a leaf storing keys past its range must not be rewritten");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
}

/// Reads and drains reject an interior action targeting a sibling's range.
#[rstest]
#[case::full_read("materialize")]
#[case::stream("stream")]
#[case::prefetch_stream("prefetch_stream")]
#[case::row_offset("row_offset")]
#[case::drain("drain")]
#[case::bulk_drain("bulk")]
#[tokio::test]
async fn interior_action_outside_its_range_is_rejected(#[case] read: &str) {
    let policy = SnapshotPolicy::default();
    let config = FragmentTreeConfig::default().with_semantic_buffer_bytes(1);
    let mut fixture = Fixture::new(5, config, policy).await;
    let mut leaves = Vec::new();
    for ids in [vec![0], vec![1, 2], vec![3], vec![4]] {
        let fragments: Vec<_> = ids.into_iter().map(make_fragment).collect();
        let written = fixture
            .tree
            .store
            .write_leaves(&fragments, 0, &fixture.tree.config)
            .await
            .unwrap();
        leaves.push(written.into_iter().next().unwrap().child_ref);
    }
    // The left interior owns [0, 3) but buffers an insert of fragment 3,
    // which the right interior holds.
    let mut duplicate = make_fragment(3);
    duplicate.files[0].path = "duplicate.lance".into();
    let misrouted = pb::FragmentTreeMutation {
        action_sequence: 1,
        action: Some(action::upsert_fragment(&duplicate)),
        fragment_count_delta: 1,
        total_rows_delta: 1,
        visible_rows_delta: 1,
    };
    let left = fixture
        .tree
        .store
        .write_internal(leaves[..2].to_vec(), vec![misrouted])
        .await
        .unwrap();
    let right = fixture
        .tree
        .store
        .write_internal(leaves[2..].to_vec(), Vec::new())
        .await
        .unwrap();
    let tree = &mut fixture.tree;
    tree.children = vec![left.child_ref, right.child_ref];
    tree.buffer.clear();
    tree.buffer_index.take();
    tree.total_fragments = 6;
    tree.total_rows = 6;
    tree.visible_rows = 6;
    tree.next_action_sequence = 2;
    tree.store.next_action_sequence = 2;
    if matches!(read, "drain" | "bulk") {
        tree.buffer.push(pb::FragmentTreeMutation {
            action_sequence: 2,
            action: Some(action::remove_fragment(2)),
            fragment_count_delta: -1,
            total_rows_delta: -1,
            visible_rows_delta: -1,
        });
        tree.next_action_sequence = 3;
        tree.store.next_action_sequence = 3;
        tree.force_flush = read == "bulk";
    }
    let error = match read {
        "stream" => tree.iter_fragments().try_collect::<Vec<_>>().await.err(),
        "prefetch_stream" => Arc::new(fixture.tree)
            .fragment_stream_with_prefetch(4)
            .try_collect::<Vec<_>>()
            .await
            .err(),
        "materialize" => tree.materialize().await.err(),
        "row_offset" => tree.fragment_at_row_offset(0).await.err(),
        _ => tree.rewrite_tree().await.err(),
    }
    .expect("an interior action outside its range must be rejected");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
}

/// A point read checks a buffered replacement's deltas against the stored record.
#[rstest]
#[case::point("point")]
#[case::stream("stream")]
#[tokio::test]
async fn buffered_reset_with_a_misstated_delta_is_rejected(#[case] read: &str) {
    let policy = SnapshotPolicy::default();
    let fixture = Fixture::new(8, FragmentTreeConfig::default(), policy).await;
    let target = 3;
    let mut snapshot = fixture.snapshot.clone();
    let Some(pb::fragment_tree::Root::InlineRoot(root)) = &mut snapshot.root else {
        panic!("expected inline root");
    };
    assert!(!root.children.is_empty());
    // The stored fragment is replaced by an identical record, so the true
    // deltas are zero. The buffer counts it as an insert.
    root.buffer.push(pb::FragmentTreeMutation {
        action_sequence: root.next_action_sequence,
        action: Some(action::upsert_fragment(&make_fragment(target))),
        fragment_count_delta: 1,
        total_rows_delta: 0,
        visible_rows_delta: 0,
    });
    root.next_action_sequence += 1;
    snapshot.next_action_sequence = root.next_action_sequence;
    let tree = fixture.open(&snapshot, fixture.tree.version()).await;
    let error = match read {
        "point" => tree.resolve_fragment(target).await.err(),
        _ => tree.iter_fragments().try_collect::<Vec<_>>().await.err(),
    }
    .expect("a misstated delta must be rejected");
    assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
}
