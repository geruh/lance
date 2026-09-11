// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Immutable routing with buffered fragment changes.
//!
//! Pressure drains the children whose pending bytes amortize a rewrite.
//! Splits and coalesces transfer messages with their ranges. The Version
//! Manifest publishes the prepared root after every dependency has been written.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, OnceLock};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::format::Fragment;
use crate::format::pb;
use crate::fragment_metadata::commit::{self, TouchedFragments, ValidatedCommit};
use crate::fragment_metadata::node::{
    self, FragmentMetadataTreeConfig, InternalNode, apply_aggregate_delta, sum_aggregate_deltas,
};
use crate::fragment_metadata::store::NodeStore;
use lance_core::cache::LanceCache;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::ScanScheduler;
use object_store::path::Path;
use prost::Message;
use roaring::RoaringBitmap;

mod flush;
mod publication;
pub use publication::SnapshotPolicy;

/// Accumulated write work over one commit (for stats / benchmark accounting).
#[derive(Debug, Default, Clone, Copy)]
struct WriteAcc {
    io_bytes: u64,
    flushes: u64,
    splits: u64,
    merges: u64,
    /// Actions applied at leaves this commit.
    materialized: u64,
    /// Actions removed by same-key squashing this commit.
    squashed: u64,
    /// Deepest tree level at which a flush occurred this commit (0 = root only).
    /// Values above 0 mean an interior ε-buffer drained.
    max_flush_depth: u32,
}

impl WriteAcc {
    fn add(&mut self, o: Self) {
        self.io_bytes += o.io_bytes;
        self.flushes += o.flushes;
        self.splits += o.splits;
        self.merges += o.merges;
        self.materialized += o.materialized;
        self.squashed += o.squashed;
        self.max_flush_depth = self.max_flush_depth.max(o.max_flush_depth);
    }
}

/// Result of flushing an internal node: (possibly split) children, residual
/// buffer, and accumulated write work.
type FlushResult = (
    Vec<pb::FragmentMetadataChild>,
    Vec<pb::FragmentMetadataMutation>,
    WriteAcc,
);
/// Result of ingesting into a subtree: the child ref(s) that now represent it
/// (>1 if it split), and accumulated write work.
type IngestResult = (Vec<pb::FragmentMetadataChild>, WriteAcc);
type BufferedChild = (pb::FragmentMetadataChild, Vec<pb::FragmentMetadataMutation>);

/// Bytes/structure written while bootstrapping.
#[derive(Debug, Default, Clone, Copy)]
pub struct BootstrapStats {
    pub io_write_bytes: u64,
    pub num_leaves: u64,
    /// Root-to-leaf edge count; see [`FragmentMetadataTree::height`].
    pub height: u32,
}

/// Result of one commit.
#[derive(Debug, Default, Clone, Copy)]
pub struct CommitStats {
    /// Compacted root, internal-node, and leaf bytes written by this commit.
    pub tree_write_bytes: u64,
    /// Inline or external roots prepared by this commit.
    pub checkpoints: u64,
    pub flushes: u64,
    pub splits: u64,
    pub merges: u64,
    /// Root-to-leaf edge count; see [`FragmentMetadataTree::height`].
    pub height: u32,
    /// Deepest level flushed this commit (0 = root buffer only; ≥1 = cascaded
    /// into internal nodes — the deep-flush regime).
    pub max_flush_depth: u32,
    /// Actions this commit staged into the root buffer.
    pub messages_in: u64,
    /// Actions that reached a leaf this commit (any commit's actions).
    pub messages_materialized: u64,
    /// Actions removed by same-key squashing this commit.
    pub messages_squashed: u64,
    /// Root buffer occupancy after the commit.
    pub root_buffer_len: u64,
}

/// Tree size and occupancy statistics.
#[derive(Debug, Default, Clone)]
pub struct ShapeReport {
    /// Root-to-leaf edge count; see [`FragmentMetadataTree::height`].
    pub height: u32,
    pub root_bytes: u64,
    pub root_buffer_len: u64,
    pub root_buffer_bytes: u64,
    pub root_fanout: u32,
    /// Encoded leaf object sizes from child references.
    pub leaf_bytes: Vec<u64>,
    /// Actual encoded Lance bytes used for the leaf-size policy.
    pub leaf_object_bytes: Vec<u64>,
    pub leaf_keys: Vec<u64>,
    pub node_bytes: Vec<u64>,
    pub node_fanouts: Vec<u32>,
    pub node_buffer_lens: Vec<u64>,
    pub node_buffer_bytes: Vec<u64>,
}

/// State of an in-order fragment walk: subtrees still to visit, each with
/// the buffered actions routed to it, and fragments ready to yield.
#[derive(Default)]
struct FragmentWalk {
    lower_bound: u64,
    stack: Vec<BufferedChild>,
    ready: VecDeque<Fragment>,
    pending_error: Option<Error>,
}

/// Where the table's `offset`-th visible row lives.
#[derive(Debug, Clone, PartialEq)]
pub enum OffsetResolution {
    /// The fragment holding the row and the visible-row offset within it.
    Found(Box<Fragment>, u64),
    /// The offset is at or past the table's visible row count.
    BeyondEnd,
}

/// A writer session over a fragment metadata tree. Holds the root (child
/// references, ε-buffer, and aggregates). Interior and leaf nodes are read on
/// demand. A scan may prefetch several leaves.
#[derive(Clone)]
pub struct FragmentMetadataTree {
    store: NodeStore,
    config: FragmentMetadataTreeConfig,
    version: u64,
    children: Vec<pb::FragmentMetadataChild>,
    buffer: Vec<pb::FragmentMetadataMutation>,
    buffer_index: OnceLock<node::BufferIndex>,
    next_action_sequence: u64,
    total_fragments: u64,
    total_rows: u64,
    /// The next fragment id an append allocates; see `next_fragment_id`.
    next_fragment_id: u64,
    visible_rows: u64,
    /// Transient selection of ordered bulk materialization for this commit.
    force_flush: bool,
    /// Exact descriptor this writer opened/prepared; derived, never persisted.
    snapshot: Option<Box<pb::FragmentMetadataTree>>,
}

#[derive(Clone)]
struct MutableState {
    version: u64,
    children: Vec<pb::FragmentMetadataChild>,
    buffer: Vec<pb::FragmentMetadataMutation>,
    next_action_sequence: u64,
    total_fragments: u64,
    total_rows: u64,

    next_fragment_id: u64,
    visible_rows: u64,
}

impl FragmentMetadataTree {
    /// Bind an immutable snapshot to another handle for the same object namespace.
    pub fn with_object_store(mut self, store: Arc<ObjectStore>) -> Self {
        self.store.rebind(store);
        self
    }

    pub fn set_foreign_bases(&mut self, foreign_bases: HashMap<u32, (Arc<ObjectStore>, Path)>) {
        self.store.set_foreign_bases(foreign_bases);
    }

    async fn build(
        mut store: NodeStore,
        config: FragmentMetadataTreeConfig,
        fragments: Vec<Fragment>,
    ) -> Result<(Self, BootstrapStats)> {
        config.validate()?;
        store.hard_capacity_bytes = config.hard_capacity_bytes;
        let mut io = 0u64;
        let mut layer: Vec<pb::FragmentMetadataChild> = Vec::new();
        let mut total_rows = 0u64;
        let mut visible_rows = 0u64;
        let mut next_fragment_id = 0u64;
        let num_fragments = fragments.len() as u64;
        for f in &fragments {
            next_fragment_id = next_fragment_id.max(f.id.checked_add(1).ok_or_else(|| {
                Error::invalid_input(format!("Fragment ID {} leaves no next ID", f.id))
            })?);
            commit::require_known_counts(f)?;
            visible_rows = visible_rows
                .checked_add(f.num_rows().unwrap_or(0) as u64)
                .ok_or_else(|| {
                    Error::invalid_input(format!("Visible row count overflow at fragment {}", f.id))
                })?;
            total_rows = total_rows
                .checked_add(f.physical_rows.unwrap_or(0) as u64)
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "fragment metadata tree total_rows overflow while bootstrapping fragment id={}",
                        f.id
                    ))
                })?;
        }
        for w in store.write_initial_leaves(&fragments, &config).await? {
            io += w.io_bytes;
            layer.push(w.child_ref);
        }
        let num_leaves = layer.len() as u64;
        node::fence(&mut layer, 0);

        // Byte pressure matters even below the configured fanout ceiling: a
        // directory of long paths can overflow with very few children.
        while node::internal_overflows(&layer, &[], &config) {
            let previous_width = layer.len();
            let mut next: Vec<pb::FragmentMetadataChild> = Vec::new();
            let groups = node::split_internal(
                layer,
                Vec::new(),
                config.split_piece_bytes(),
                config.max_children_per_node,
            );
            if groups.len() >= previous_width {
                return Err(Error::invalid_input(format!(
                    "node budget {} cannot group two child references; widen the directory budget",
                    config.max_node_bytes
                )));
            }
            for (group, buffer) in groups {
                let w = store.write_internal(group, buffer).await?;
                io += w.io_bytes;
                next.push(w.child_ref);
            }
            layer = next;
        }
        let height = layer.iter().map(|c| c.height).max().unwrap_or(0) + 1;

        let tree = Self {
            store,
            config,
            version: 1,
            children: layer,
            buffer: Vec::new(),
            buffer_index: OnceLock::new(),
            next_action_sequence: 1,
            total_fragments: num_fragments,
            total_rows,

            next_fragment_id,
            visible_rows,
            force_flush: false,
            snapshot: None,
        };
        Ok((
            tree,
            BootstrapStats {
                io_write_bytes: io,
                num_leaves,
                height,
            },
        ))
    }

    /// Root-to-leaf edge count: one for a root over leaves, two with an
    /// intervening routing level. An empty tree also reports one.
    pub fn height(&self) -> u32 {
        self.children.iter().map(|c| c.height).max().unwrap_or(0) + 1
    }

    /// Latest atomically published version held by this session.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Number of logical fragments, including buffered adds and removes.
    ///
    /// This reads root metadata only and performs no object-store IO.
    pub fn count_fragments(&self) -> u64 {
        self.total_fragments
    }

    /// Sum of physical rows across logical fragments.
    ///
    /// Admitted fragments have known counts. This reads root metadata only
    /// and performs no object-store IO.
    pub fn count_rows(&self) -> u64 {
        self.total_rows
    }

    /// Visible rows (physical minus deleted) across the table. Exact by the
    /// writer invariant on known counts. Root metadata only, no IO.
    pub fn count_visible_rows(&self) -> u64 {
        self.visible_rows
    }

    /// The next fragment id an append may allocate: zero on an empty table,
    /// otherwise one above the largest id ever assigned, buffered adds
    /// included. This is Lance's manifest allocation rule.
    pub fn next_fragment_id(&self) -> u64 {
        self.next_fragment_id
    }

    /// Number of actions currently buffered directly in the in-memory root.
    pub fn root_buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Number of direct child references currently held by the in-memory root.
    pub fn root_child_count(&self) -> usize {
        self.children.len()
    }

    /// Encoded leaf sizes when the root points directly to leaves. Returns an
    /// empty list when there is an intervening routing level.
    pub fn leaf_object_sizes(&self) -> Vec<u64> {
        self.children
            .iter()
            .filter(|child| child.height == 0)
            .map(|child| child.object_size)
            .collect()
    }

    /// Resolve one fragment by loading only the root-to-leaf path: at most
    /// `height` reads on a compacted root.
    ///
    /// Routing follows sibling `min_key` fences (see [`node::child_index_for`]),
    /// and buffered actions from every node on the path are applied with the
    /// same action_sequence ordering as [`Self::materialize`].
    pub async fn resolve_fragment(&self, frag_id: u64) -> Result<Option<Fragment>> {
        let mut actions: Vec<pb::FragmentMetadataMutation> = self
            .buffer_index
            .get_or_init(|| node::BufferIndex::new(&self.buffer))
            .for_fragment(frag_id)
            .map(|offset| self.buffer[offset].clone())
            .collect();
        actions.sort_by_key(|t| t.action_sequence);
        if let Some(reset) = actions.iter().rposition(|t| {
            matches!(
                t.action.as_ref().and_then(|a| a.action.as_ref()),
                Some(pb::fragment_action::Action::AddFragment(_))
                    | Some(pb::fragment_action::Action::RemoveFragment(_))
            )
        }) {
            // Root messages for this key are newer than messages below them.
            // A whole-state reset makes the older leaf state unnecessary. The
            // prior record is unknown here, so deltas cannot be verified.
            let mut fragments = BTreeMap::new();
            node::apply_actions(&mut fragments, actions.split_off(reset))?;
            self.store.share_data_file_lists(fragments.values_mut());
            return Ok(fragments.remove(&frag_id));
        }
        let mut children = Cow::Borrowed(self.children.as_slice());
        let mut fragment = None;

        while !children.is_empty() {
            let child = children[node::child_index_for(&children, frag_id)].clone();
            if child.height == 0 {
                node::validate_routed(
                    std::slice::from_ref(&child),
                    &[actions.clone()],
                    node::ROOT_EXCLUSIVE_END,
                )?;
                fragment = self
                    .store
                    .read_leaf(&child)
                    .await?
                    .into_iter()
                    .find(|candidate| candidate.id == frag_id);
                break;
            }

            let internal = self.store.read_internal(&child).await?;
            actions.extend(
                internal
                    .buffer
                    .into_iter()
                    .filter(|tagged| node::action_key(tagged) == frag_id),
            );
            children = Cow::Owned(internal.children);
        }

        let mut fragments = BTreeMap::new();
        if let Some(fragment) = fragment {
            fragments.insert(fragment.id, fragment);
        }
        self.replay_verified(&mut fragments, actions)?;
        Ok(fragments.remove(&frag_id))
    }

    /// Resolve the current state of exactly `fragment_ids`, sharing path
    /// reads across ids in the same subtree. This is the only read a commit
    /// performs before publication: work is proportional to the touched
    /// fragments and the tree height, never to the table.
    pub async fn resolve_touched(&self, fragment_ids: &[u64]) -> Result<TouchedFragments> {
        let ids: BTreeSet<u64> = fragment_ids.iter().copied().collect();
        if ids.is_empty() {
            return Ok(TouchedFragments::default());
        }
        let bitmap: Option<RoaringBitmap> = ids
            .iter()
            .map(|id| u32::try_from(*id).ok())
            .collect::<Option<Vec<u32>>>()
            .map(RoaringBitmap::from_iter);
        let fragments = match bitmap {
            Some(bitmap) => self
                .resolve_fragments_concurrent(&bitmap, 8)
                .await?
                .into_iter()
                .map(|fragment| (fragment.id, fragment))
                .collect(),
            None => {
                let mut fragments = BTreeMap::new();
                for id in &ids {
                    if let Some(fragment) = self.resolve_fragment(*id).await? {
                        fragments.insert(*id, fragment);
                    }
                }
                fragments
            }
        };
        Ok(TouchedFragments { ids, fragments })
    }

    /// Resolve a set of fragment ids while loading only the subtrees whose
    /// routing range intersects the set.
    ///
    /// Pruning uses the routing fences, never a child's live `max_key`: a
    /// buffered insert may sit above the child's live range and still belong
    /// to it. Each covering node or leaf is loaded at most once. Leaf GETs
    /// run with concurrency 1; use [`Self::resolve_fragments_concurrent`]
    /// when the caller can overlap them.
    pub async fn resolve_fragments(&self, fragment_ids: &RoaringBitmap) -> Result<Vec<Fragment>> {
        self.resolve_fragments_concurrent(fragment_ids, 1).await
    }

    /// Same as [`Self::resolve_fragments`], overlapping covering leaf GETs.
    pub async fn resolve_fragments_concurrent(
        &self,
        fragment_ids: &RoaringBitmap,
        concurrency: usize,
    ) -> Result<Vec<Fragment>> {
        let mut leaves = Vec::new();
        let mut actions = self
            .buffer
            .iter()
            .filter(|tagged| bitmap_contains(fragment_ids, node::action_key(tagged)))
            .cloned()
            .collect();
        self.collect_covering_leaves(
            self.children.clone(),
            0,
            Some(node::ROOT_EXCLUSIVE_END),
            fragment_ids,
            &mut leaves,
            &mut actions,
        )
        .await?;
        let store = self.store.clone();
        let mut fragments = futures::stream::iter(leaves)
            .map(move |child| {
                let store = store.clone();
                async move { store.read_leaf(&child).await }
            })
            .buffered(concurrency.max(1))
            .try_fold(BTreeMap::new(), |mut fragments, loaded| async move {
                fragments.extend(
                    loaded
                        .into_iter()
                        .filter(|fragment| bitmap_contains(fragment_ids, fragment.id))
                        .map(|fragment| (fragment.id, fragment)),
                );
                Ok(fragments)
            })
            .await?;
        self.replay_verified(&mut fragments, actions)?;
        fragments.retain(|fragment_id, _| bitmap_contains(fragment_ids, *fragment_id));
        Ok(fragments.into_values().collect())
    }

    /// Resolve which fragment holds the table's `offset`-th visible row and
    /// the offset within that fragment, by descending subtree visible-row
    /// totals: O(height) reads, never a stream from the first fragment.
    ///
    /// Totals are exact by the writer invariant on known counts. Buffered
    /// actions above a child adjust its total and are applied when its leaf
    /// is read, so the descent and a full stream agree.
    pub async fn fragment_at_row_offset(&self, offset: u64) -> Result<OffsetResolution> {
        if offset >= self.visible_rows {
            return Ok(OffsetResolution::BeyondEnd);
        }
        let mut remaining = offset;
        let mut children = self.children.clone();
        let mut actions: Vec<pb::FragmentMetadataMutation> = self.buffer.clone();
        loop {
            if children.is_empty() {
                // The whole table lives in buffered actions.
                let mut fragments = BTreeMap::new();
                self.replay_verified(&mut fragments, actions)?;
                return Ok(walk_fragments(fragments.into_values(), remaining));
            }
            let buckets = node::partition_buffer_by_child(&children, actions);
            node::validate_routed(&children, &buckets, node::ROOT_EXCLUSIVE_END)?;
            let mut chosen = None;
            for (child, bucket) in children.iter().zip(buckets) {
                let delta = sum_aggregate_deltas(
                    bucket.iter().map(|tagged| tagged.visible_rows_delta),
                    "visible_rows_delta",
                )?;
                let adjusted = apply_aggregate_delta(child.visible_rows, delta, "visible_rows")?;
                if remaining < adjusted {
                    chosen = Some((child.clone(), bucket));
                    break;
                }
                remaining -= adjusted;
            }
            let Some((child, bucket)) = chosen else {
                // Totals said the offset is inside, but the walk fell off:
                // an accounting bug, not a caller error.
                return Err(Error::internal(format!(
                    "row-offset descent exhausted children with {remaining} rows remaining"
                )));
            };
            if child.height == 0 {
                let mut fragments: BTreeMap<u64, Fragment> = self
                    .store
                    .read_leaf(&child)
                    .await?
                    .into_iter()
                    .map(|fragment| (fragment.id, fragment))
                    .collect();
                self.replay_verified(&mut fragments, bucket)?;
                return Ok(walk_fragments(fragments.into_values(), remaining));
            }
            let internal = self.store.read_internal(&child).await?;
            actions = bucket;
            actions.extend(internal.buffer);
            children = internal.children;
        }
    }

    /// Collect all leaf fragments and replay buffered actions in action sequence number order.
    pub async fn materialize(&self) -> Result<Vec<Fragment>> {
        let mut map: BTreeMap<u64, Fragment> = BTreeMap::new();
        let mut actions: Vec<pb::FragmentMetadataMutation> = self.buffer.clone();
        self.collect_subtree(self.children.clone(), &mut map, &mut actions)
            .await?;
        self.replay_verified(&mut map, actions)?;
        Ok(map.into_values().collect())
    }

    /// Stream fragments in id order. This walk materializes one leaf at a
    /// time. [`Self::fragment_stream_with_prefetch`] may keep several leaf
    /// reads in flight.
    pub fn iter_fragments(&self) -> impl Stream<Item = Result<Fragment>> + '_ {
        futures::stream::try_unfold(self.start_walk_from(0), move |mut walk| async move {
            let next = self.walk_next(&mut walk).await?;
            Ok(next.map(|fragment| (fragment, walk)))
        })
    }

    /// The same walk as [`Self::iter_fragments`], owning the tree so the
    /// stream can outlive a borrow. This is the source a lazy scanner
    /// consumes.
    pub fn fragment_stream(self: Arc<Self>) -> BoxStream<'static, Result<Fragment>> {
        self.fragment_stream_with_prefetch(1)
    }

    /// Stream fragments in increasing ID order, starting at `fragment_id`
    /// inclusively. Routing skips earlier subtrees; holes and removed IDs are
    /// omitted. The stream remains pinned to this tree's version.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use lance_table::fragment_metadata::FragmentMetadataTree;
    /// # use futures::TryStreamExt;
    /// # async fn example(tree: Arc<FragmentMetadataTree>) -> lance_core::Result<()> {
    /// let mut stream = tree.fragment_stream_from(100);
    /// while let Some(fragment) = stream.try_next().await? {
    ///     assert!(fragment.id >= 100);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn fragment_stream_from(
        self: Arc<Self>,
        fragment_id: u64,
    ) -> BoxStream<'static, Result<Fragment>> {
        let walk = self.start_walk_from(fragment_id);
        futures::stream::try_unfold((self, walk), |(tree, mut walk)| async move {
            let next = tree.walk_next(&mut walk).await?;
            Ok(next.map(|fragment| (fragment, (tree, walk))))
        })
        .boxed()
    }

    /// Stream in ID order with at most `prefetch` leaf reads in flight, including
    /// across routing-node boundaries. Zero selects one read at a time.
    pub fn fragment_stream_with_prefetch(
        self: Arc<Self>,
        prefetch: usize,
    ) -> BoxStream<'static, Result<Fragment>> {
        if self.children.is_empty() {
            return self.fragment_stream_from(0);
        }
        let walk = self.start_walk_from(0);
        let store = self.store.clone();
        futures::stream::try_unfold((self, walk), |(tree, mut walk)| async move {
            let leaf = tree.walk_next_leaf(&mut walk).await?;
            Ok(leaf.map(|leaf| (leaf, (tree, walk))))
        })
        .map_ok(move |(child, actions)| {
            let store = store.clone();
            async move {
                let mut fragments: BTreeMap<u64, Fragment> = store
                    .read_leaf(&child)
                    .await?
                    .into_iter()
                    .map(|fragment| (fragment.id, fragment))
                    .collect();
                store.apply_verified(&mut fragments, actions)?;
                Ok::<_, Error>(fragments)
            }
        })
        .try_buffered(prefetch.max(1))
        .map_ok(|state| futures::stream::iter(state.into_values().map(Ok)))
        .try_flatten()
        .boxed()
    }

    /// Fragment ids that still have a buffered action somewhere in the tree,
    /// root included. Empty means every committed action has reached a leaf.
    pub async fn buffered_action_keys(&self) -> Result<HashSet<u64>> {
        let mut keys: HashSet<u64> = self.buffer.iter().map(node::action_key).collect();
        let mut pending = self.children.clone();
        while let Some(child) = pending.pop() {
            if child.height == 0 {
                continue;
            }
            let internal = self.store.read_internal(&child).await?;
            keys.extend(internal.buffer.iter().map(node::action_key));
            pending.extend(internal.children);
        }
        Ok(keys)
    }

    /// Walk the tree and collect `(height, logical_bytes)` for every internal node
    /// (root included). Used to measure how full internal ε-buffers are — a node
    /// near `B` is holding a big buffer, a node near its ref-only size is "cold".
    pub async fn internal_node_sizes(&self) -> Result<Vec<(u32, u64)>> {
        let mut out = vec![(
            self.height(),
            node::internal_logical_bytes(&self.children, &self.buffer),
        )];
        self.collect_internal_sizes(self.children.clone(), &mut out)
            .await?;
        Ok(out)
    }

    /// Structural health of the current tree: every internal node's fanout,
    /// encoded bytes and buffer occupancy, every leaf's logical bytes and
    /// key count, read from the object store (internal nodes only; leaf
    /// sizes come from their refs).
    pub async fn shape_report(&self) -> Result<ShapeReport> {
        let root = self.compacted_root();
        let mut report = ShapeReport {
            height: self.height(),
            root_bytes: root.encoded_len() as u64,
            root_buffer_len: self.buffer.len() as u64,
            root_buffer_bytes: node::buffer_bytes(&self.buffer),
            root_fanout: self.children.len() as u32,
            ..Default::default()
        };
        let mut stack: Vec<pb::FragmentMetadataChild> = self.children.clone();
        while let Some(child) = stack.pop() {
            if child.height == 0 {
                report.leaf_bytes.push(child.object_size);
                report.leaf_object_bytes.push(child.object_size);
                report.leaf_keys.push(child.num_keys);
                continue;
            }
            let node = self.store.read_internal(&child).await?;
            report.node_bytes.push(child.object_size);
            report.node_fanouts.push(node.children.len() as u32);
            report.node_buffer_lens.push(node.buffer.len() as u64);
            report
                .node_buffer_bytes
                .push(node::buffer_bytes(&node.buffer));
            stack.extend(node.children);
        }
        Ok(report)
    }

    /// The leaf-watermark invariant: every action still buffered anywhere in
    /// the tree must be newer than the leaf watermark it routes to. A
    /// violation would mean a leaf claims to hold a change it never applied.
    pub async fn verify_watermarks(&self) -> Result<()> {
        // (action sequence number, routing key), gathered root first.
        let mut pending: Vec<(u64, u64)> = self
            .buffer
            .iter()
            .map(|tagged| (tagged.action_sequence, node::action_key(tagged)))
            .collect();
        let mut stack: Vec<pb::FragmentMetadataChild> = self.children.clone();
        let mut leaves: Vec<pb::FragmentMetadataChild> = Vec::new();
        while let Some(child) = stack.pop() {
            if child.height == 0 {
                leaves.push(child);
                continue;
            }
            let internal = self.store.read_internal(&child).await?;
            pending.extend(
                internal
                    .buffer
                    .iter()
                    .map(|tagged| (tagged.action_sequence, node::action_key(tagged))),
            );
            stack.extend(internal.children);
        }
        leaves.sort_by_key(|leaf| leaf.min_key);
        for (action_sequence, key) in pending {
            if leaves.is_empty() {
                continue;
            }
            let leaf = &leaves[node::child_index_for(&leaves, key)];
            if action_sequence <= leaf.materialized_through_action_sequence {
                return Err(Error::invalid_input(format!(
                    "buffered action action_sequence={action_sequence} for fragment {key} is at or below its \
                     leaf's watermark {} (leaf {}): the leaf claims a change it cannot hold",
                    leaf.materialized_through_action_sequence, leaf.path
                )));
            }
        }
        Ok(())
    }

    /// Read every node and leaf reachable from this root, proving the
    /// published tree references no missing object. Returns the object count.
    pub async fn verify_reachable(&self) -> Result<u64> {
        let mut pending = self.children.clone();
        let mut objects = 0u64;
        while let Some(child) = pending.pop() {
            objects += 1;
            if child.height == 0 {
                self.store.read_leaf(&child).await?;
            } else {
                pending.extend(self.store.read_internal(&child).await?.children);
            }
        }
        Ok(objects)
    }

    fn mutable_state(&self) -> MutableState {
        MutableState {
            version: self.version,
            children: self.children.clone(),
            buffer: self.buffer.clone(),
            next_action_sequence: self.next_action_sequence,
            total_fragments: self.total_fragments,
            total_rows: self.total_rows,

            next_fragment_id: self.next_fragment_id,
            visible_rows: self.visible_rows,
        }
    }

    fn restore_mutable_state(&mut self, state: MutableState) {
        self.buffer_index.take();
        self.version = state.version;
        self.children = state.children;
        self.buffer = state.buffer;
        self.next_action_sequence = state.next_action_sequence;
        self.store.next_action_sequence = state.next_action_sequence;
        self.total_fragments = state.total_fragments;

        self.total_rows = state.total_rows;
        self.next_fragment_id = state.next_fragment_id;
        self.visible_rows = state.visible_rows;
    }

    /// Assign fresh action sequence numbers and move the in-memory root to
    /// the next version: buffer, aggregates, id allocation, and table state.
    fn stage_commit(
        &mut self,
        commit: &ValidatedCommit,
        aggregate_deltas: Vec<commit::ActionDeltas>,
    ) -> Result<Vec<pb::FragmentMetadataMutation>> {
        let actions = &commit.fragment_actions;
        let action_count = u64::try_from(actions.len()).map_err(|_| {
            Error::invalid_input(format!(
                "fragment metadata tree action count does not fit u64: {}",
                actions.len()
            ))
        })?;
        let next_action_sequence = self.next_action_sequence.checked_add(action_count).ok_or_else(|| {
            Error::invalid_input(format!(
                "fragment metadata tree action_sequence overflow: next_action_sequence={}, action_count={action_count}",
                self.next_action_sequence
            ))
        })?;
        let fragment_count_delta = sum_aggregate_deltas(
            aggregate_deltas.iter().map(|deltas| deltas.fragment_count),
            "fragment_count_delta",
        )?;
        let total_rows_delta = sum_aggregate_deltas(
            aggregate_deltas.iter().map(|deltas| deltas.physical_rows),
            "total_rows_delta",
        )?;
        let visible_rows_delta = sum_aggregate_deltas(
            aggregate_deltas.iter().map(|deltas| deltas.visible_rows),
            "visible_rows_delta",
        )?;

        let total_fragments = apply_aggregate_delta(
            self.total_fragments,
            fragment_count_delta,
            "total_fragments",
        )?;
        let total_rows = apply_aggregate_delta(self.total_rows, total_rows_delta, "total_rows")?;
        let visible_rows =
            apply_aggregate_delta(self.visible_rows, visible_rows_delta, "visible_rows")?;

        let mut tagged = Vec::with_capacity(actions.len());
        for (offset, (action, deltas)) in actions.iter().zip(aggregate_deltas).enumerate() {
            tagged.push(pb::FragmentMetadataMutation {
                action_sequence: self.next_action_sequence + offset as u64,
                action: Some(action.clone()),
                fragment_count_delta: deltas.fragment_count,
                total_rows_delta: deltas.physical_rows,
                visible_rows_delta: deltas.visible_rows,
            });
        }
        self.buffer_index.take();
        self.buffer.extend(tagged.iter().cloned());
        self.next_action_sequence = next_action_sequence;
        self.store.next_action_sequence = next_action_sequence;
        self.total_fragments = total_fragments;
        self.total_rows = total_rows;
        self.visible_rows = visible_rows;
        self.version = self.version.checked_add(1).ok_or_else(|| {
            Error::invalid_input("fragment metadata tree version counter exhausted")
        })?;
        for action in actions {
            if let Some(pb::fragment_action::Action::AddFragment(fragment)) = &action.action {
                self.next_fragment_id = self.next_fragment_id.max(fragment.id + 1);
            }
        }
        if let Some(next) = commit.next_fragment_id {
            if next < self.next_fragment_id || next > u64::from(u32::MAX) + 1 {
                return Err(Error::invalid_input(format!(
                    "Fragment ID frontier {next} must be between {} and 2^32",
                    self.next_fragment_id
                )));
            }
            self.next_fragment_id = next;
        }
        Ok(tagged)
    }

    /// The compacted root for the current in-memory state, publishing
    /// `committed` as the version's history.
    fn compacted_root(&self) -> pb::FragmentMetadataRoot {
        pb::FragmentMetadataRoot {
            children: self.children.clone(),
            buffer: self.buffer.clone(),
            next_action_sequence: self.next_action_sequence,
        }
    }

    /// Flush the root buffer as far as it goes, then split, coalesce, and
    /// shrink the root. Every touched node is copy-on-write, so nothing here
    /// is visible until the root is published.
    async fn rewrite_tree(&mut self) -> Result<WriteAcc> {
        let mut acc = WriteAcc::default();

        // Flush the root buffer down as far as it will go (root is depth 0).
        let children = std::mem::take(&mut self.children);
        let buffer = std::mem::take(&mut self.buffer);
        let (children, buffer, a) = if self.force_flush {
            let result = super::bulk::materialize(
                &self.store,
                &self.config,
                children,
                buffer,
                self.next_action_sequence.saturating_sub(1),
            )
            .await?;
            (
                result.children,
                Vec::new(),
                WriteAcc {
                    io_bytes: result.io_bytes,
                    flushes: result.flushes,
                    splits: result.splits,
                    merges: result.merges,
                    materialized: result.materialized,
                    max_flush_depth: result.max_flush_depth,
                    ..Default::default()
                },
            )
        } else {
            self.flush_internal(children, buffer, 0, 0).await?
        };
        acc.add(a);
        self.children = children;
        self.buffer = buffer;
        node::fence(&mut self.children, 0);

        // A childless root (an empty table grown by appends) cannot flush
        // anywhere: materialize its buffer into the first leaves instead of
        // splitting into a childless internal node.
        if self.children.is_empty()
            && (node::internal_overflows(&self.children, &self.buffer, &self.config)
                || node::buffer_pressured(&self.buffer, &self.config))
        {
            let mut fragments = BTreeMap::new();
            let actions = std::mem::take(&mut self.buffer);
            self.replay_verified(&mut fragments, actions)?;
            let fragments: Vec<Fragment> = fragments.into_values().collect();
            let watermark = self.next_action_sequence.saturating_sub(1);
            for w in self
                .store
                .write_leaves(&fragments, watermark, &self.config)
                .await?
            {
                acc.io_bytes += w.io_bytes;
                self.children.push(w.child_ref);
            }
        }

        // A broad transaction can cross several heights at once. Keep lifting
        // routing until the new root itself fits, not just its first children.
        while node::internal_overflows(&self.children, &self.buffer, &self.config) {
            let previous_width = self.children.len();
            let pieces = node::split_internal(
                std::mem::take(&mut self.children),
                std::mem::take(&mut self.buffer),
                self.config.split_piece_bytes(),
                self.config.max_children_per_node,
            );
            if pieces.len() >= previous_width {
                return Err(Error::invalid_input(format!(
                    "node budget {} cannot reduce a root with {} children",
                    self.config.max_node_bytes, previous_width
                )));
            }
            let mut new_children = Vec::with_capacity(pieces.len());
            for (ch, buf) in pieces {
                let w = self.store.write_internal(ch, buf).await?;
                acc.io_bytes += w.io_bytes;
                new_children.push(w.child_ref);
            }
            self.children = new_children;
            acc.splits += 1;
        }

        // Coalesce underflowing children (self-balancing on deletes).
        if !self.force_flush {
            let children = std::mem::take(&mut self.children);
            let (children, a) = self.merge_small_children(children).await?;
            acc.add(a);
            self.children = children;
        }

        // Shrink: a root with a single internal child pulls that child up.
        self.maybe_shrink_root().await?;
        node::fence(&mut self.children, 0);
        Ok(acc)
    }

    /// Flush an internal node's buffer to its children while it is pressured.
    /// Only children whose pending batch amortizes a rewrite drain. Under
    /// semantic pressure the rest keep buffering. Under structural pressure
    /// the caller splits whatever routing still overflows. `depth` is this
    /// node's level below the root, 0 for the root itself, and `lower_bound`
    /// the range start its parent assigned. Returns the children, split if
    /// needed, and the residual buffer.
    fn flush_internal(
        &self,
        mut children: Vec<pb::FragmentMetadataChild>,
        mut buffer: Vec<pb::FragmentMetadataMutation>,
        depth: u32,
        lower_bound: u64,
    ) -> BoxFuture<'_, Result<FlushResult>> {
        Box::pin(async move {
            let mut acc = WriteAcc::default();
            loop {
                // A childless node has nowhere to flush; the caller turns the
                // buffer into leaves instead.
                if children.is_empty() {
                    break;
                }
                let structural = node::internal_overflows(&children, &buffer, &self.config);
                if !structural && !node::buffer_pressured(&buffer, &self.config) {
                    break;
                }
                let pending = buffer.len();
                let mut buckets = node::partition_buffer_by_child(&children, buffer);
                node::validate_routed(&children, &buckets, node::ROOT_EXCLUSIVE_END)?;
                let indices = flush::select_children(
                    &children,
                    &buckets,
                    &self.config,
                    self.store.object_store.io_parallelism(),
                );
                if indices.is_empty() {
                    // Under semantic pressure no batch pays for its object yet,
                    // so keep buffering. Under structural pressure a byte
                    // overflow always leaves a bucket at or above the fair
                    // share, so only routing or fanout remains and the caller
                    // splits it.
                    debug_assert!(
                        !structural
                            || pending == 0
                            || children.len() as u32 > self.config.max_children_per_node
                    );
                    buffer = buckets.into_iter().flatten().collect();
                    break;
                }
                let concurrency = indices.len();
                let drains: Vec<_> = indices
                    .into_iter()
                    .map(|idx| {
                        (
                            idx,
                            children[idx].clone(),
                            std::mem::take(&mut buckets[idx]),
                        )
                    })
                    .collect();
                buffer = buckets.into_iter().flatten().collect();
                let mut results: Vec<_> = futures::stream::iter(drains)
                    .map(|(idx, child, actions)| async move {
                        self.ingest(child, actions, depth)
                            .await
                            .map(|result| (idx, result))
                    })
                    .buffered(concurrency)
                    .try_collect()
                    .await?;
                // Apply from the right so splits and removals keep earlier indices valid.
                results.sort_unstable_by_key(|(idx, _)| std::cmp::Reverse(*idx));
                for (idx, (new_refs, a)) in results {
                    acc.add(a);
                    acc.flushes += 1;
                    acc.max_flush_depth = acc.max_flush_depth.max(depth);
                    children.splice(idx..idx + 1, new_refs);
                }
            }
            node::fence(&mut children, lower_bound);
            Ok((children, buffer, acc))
        })
    }

    /// Push `incoming` messages into the subtree rooted at `child` (at `depth`
    /// below the root); apply at a leaf, recurse+buffer at an internal node;
    /// split on overflow. Returns the child ref(s) that now represent the subtree.
    fn ingest(
        &self,
        child: pb::FragmentMetadataChild,
        incoming: Vec<pb::FragmentMetadataMutation>,
        depth: u32,
    ) -> BoxFuture<'_, Result<IngestResult>> {
        Box::pin(async move {
            let mut acc = WriteAcc::default();
            if child.height == 0 {
                // Leaf: apply messages, then split if it overflows.
                let fragments = self.store.read_leaf(&child).await?;
                let mut map: BTreeMap<u64, Fragment> =
                    fragments.into_iter().map(|f| (f.id, f)).collect();
                acc.materialized += incoming.len() as u64;
                self.replay_verified(&mut map, incoming)?;
                let new_frags: Vec<Fragment> = map.into_values().collect();

                // A fully-emptied leaf is dropped from its parent. Keeping it would
                // create a phantom child with min_key=0 that corrupts the
                // sorted-by-min_key invariant `child_index_for` relies on.
                if new_frags.is_empty() {
                    return Ok((vec![], acc));
                }
                // The flush that reaches a leaf drained every buffer on its
                // path for this range first, so the leaf now holds the
                // result of every applicable action up to the action_sequence ceiling.
                let watermark = self.next_action_sequence.saturating_sub(1);
                let written = self
                    .store
                    .write_leaves(&new_frags, watermark, &self.config)
                    .await?;
                acc.splits += written.len().saturating_sub(1) as u64;
                let mut refs: Vec<_> = written
                    .into_iter()
                    .map(|w| {
                        acc.io_bytes += w.io_bytes;
                        w.child_ref
                    })
                    .collect();
                node::fence(&mut refs, child.min_key);
                Ok((refs, acc))
            } else {
                // Internal: buffer, recurse-flush, split if it overflows.
                let InternalNode {
                    children,
                    mut buffer,
                } = self.store.read_internal(&child).await?;
                buffer.extend(incoming);
                let before = buffer.len();
                buffer = node::squash_buffer(buffer);
                acc.squashed += (before - buffer.len()) as u64;
                // This node is one level deeper than the parent that flushed to it.
                let (children, buffer, a) = self
                    .flush_internal(children, buffer, depth + 1, child.min_key)
                    .await?;
                acc.add(a);
                // Rebalance: coalesce any underflowing children before checking split.
                let (children, a) = self.merge_small_children(children).await?;
                acc.add(a);

                // An internal node whose children all vanished is dropped too (same
                // phantom-min_key=0 hazard as an empty leaf).
                if children.is_empty() {
                    return Ok((vec![], acc));
                }
                if node::internal_overflows(&children, &buffer, &self.config) {
                    let mut refs = Vec::new();
                    for (ch, buf) in node::split_internal(
                        children,
                        buffer,
                        self.config.split_piece_bytes(),
                        self.config.max_children_per_node,
                    ) {
                        let w = self.store.write_internal(ch, buf).await?;
                        acc.io_bytes += w.io_bytes;
                        refs.push(w.child_ref);
                    }
                    acc.splits += 1;
                    node::fence(&mut refs, child.min_key);
                    Ok((refs, acc))
                } else {
                    let w = self.store.write_internal(children, buffer).await?;
                    acc.io_bytes += w.io_bytes;
                    let mut refs = vec![w.child_ref];
                    node::fence(&mut refs, child.min_key);
                    Ok((refs, acc))
                }
            }
        })
    }

    /// Coalesce runs of adjacent children when one underflows (leaf ≤ 0.25 B;
    /// internal < max_children_per_node/4 children), bounded so the merged node stays valid
    /// (leaves ≤ 0.6 B; internal ≤ max_children_per_node children). Reads/writes the merged
    /// node(s). Leaves concat fragments; internal nodes concat children + buffers.
    async fn merge_small_children(
        &self,
        children: Vec<pb::FragmentMetadataChild>,
    ) -> Result<(Vec<pb::FragmentMetadataChild>, WriteAcc)> {
        let mut acc = WriteAcc::default();
        let mut out: Vec<pb::FragmentMetadataChild> = Vec::with_capacity(children.len());
        let mut i = 0;
        while i < children.len() {
            if !node::is_underflow(&children[i], &self.config) {
                out.push(children[i].clone());
                i += 1;
                continue;
            }
            // Grow a coalesce group with adjacent siblings, bounded by node kind.
            let is_leaf = children[i].height == 0;
            let mut group = vec![children[i].clone()];
            let mut bytes = children[i].object_size;
            let mut fan = children[i].num_children;
            let mut j = i + 1;
            while j < children.len() {
                let c = &children[j];
                let fits = if is_leaf {
                    bytes + c.object_size <= self.config.leaf_coalesce_ceiling()
                } else {
                    fan + c.num_children <= self.config.max_children_per_node
                        && bytes + c.object_size <= self.config.coalesce_ceiling()
                };
                if !fits {
                    break;
                }
                bytes += c.object_size;
                fan += c.num_children;
                group.push(c.clone());
                j += 1;
            }
            if group.len() == 1 {
                out.extend(group);
            } else {
                let lower_bound = group[0].min_key;
                let (mut merged, a) = self.coalesce(group).await?;
                node::fence(&mut merged, lower_bound);
                acc.add(a);
                acc.merges += 1;
                out.extend(merged);
            }
            i = j;
        }
        // A singleton at either edge needs its neighbor even when the normal
        // soft coalesce ceiling would leave it alone. Rebalance the combined
        // range under the hard node budget, keeping messages with their fences.
        let mut repaired: Vec<pb::FragmentMetadataChild> = Vec::with_capacity(out.len());
        let mut pending: VecDeque<_> = out.into();
        while let Some(child) = pending.pop_front() {
            if child.height == 0 || child.num_children > 1 {
                repaired.push(child);
                continue;
            }
            let pair = if let Some(right) = pending.pop_front() {
                vec![child, right]
            } else if let Some(left) = repaired.pop() {
                vec![left, child]
            } else {
                // Its parent must provide a sibling or shrink this root.
                repaired.push(child);
                break;
            };
            let lower_bound = pair[0].min_key;
            let mut children = Vec::new();
            let mut buffer = Vec::new();
            for sibling in pair {
                let node = self.store.read_internal(&sibling).await?;
                children.extend(node.children);
                buffer.extend(node.buffer);
            }
            let (children, buffer, flushed) = self
                .flush_internal(children, buffer, 0, lower_bound)
                .await?;
            acc.add(flushed);
            let (children, coalesced) = Box::pin(self.merge_small_children(children)).await?;
            acc.add(coalesced);
            if children.is_empty() && buffer.is_empty() {
                continue;
            }
            if node::internal_overflows(&children, &buffer, &self.config) {
                // Occupancy repair may exceed the preferred half-full target.
                // After draining hard pressure, split against the hard budget
                // so a hot but valid range does not become a singleton again.
                let pieces = node::split_internal(
                    children,
                    buffer,
                    self.config.max_node_bytes - 1,
                    self.config.max_children_per_node,
                );
                for (children, buffer) in pieces {
                    if children.len() < 2 {
                        return Err(Error::invalid_input(format!(
                            "node budget {} cannot repair a singleton interior range",
                            self.config.max_node_bytes
                        )));
                    }
                    let written = self.store.write_internal(children, buffer).await?;
                    acc.io_bytes += written.io_bytes;
                    repaired.push(written.child_ref);
                }
            } else {
                let written = self.store.write_internal(children, buffer).await?;
                acc.io_bytes += written.io_bytes;
                repaired.push(written.child_ref);
                acc.merges += 1;
            }
        }
        Ok((repaired, acc))
    }

    /// Combine an adjacent group of same-height children into one node.
    async fn coalesce(
        &self,
        group: Vec<pb::FragmentMetadataChild>,
    ) -> Result<(Vec<pb::FragmentMetadataChild>, WriteAcc)> {
        let mut acc = WriteAcc::default();
        if group[0].height == 0 {
            let mut fragments: Vec<Fragment> = Vec::new();
            for c in &group {
                fragments.extend(self.store.read_leaf(c).await?);
            }
            fragments.sort_by_key(|f| f.id);
            let watermark = group
                .iter()
                .map(|child| child.materialized_through_action_sequence)
                .min()
                .unwrap_or(0);
            let written = self
                .store
                .write_leaves(&fragments, watermark, &self.config)
                .await?;
            let refs = written
                .into_iter()
                .map(|w| {
                    acc.io_bytes += w.io_bytes;
                    w.child_ref
                })
                .collect();
            Ok((refs, acc))
        } else {
            let mut children: Vec<pb::FragmentMetadataChild> = Vec::new();
            let mut buffer: Vec<pb::FragmentMetadataMutation> = Vec::new();
            for c in &group {
                let node = self.store.read_internal(c).await?;
                children.extend(node.children);
                buffer.extend(node.buffer);
            }
            // Joining parents exposes siblings that previously belonged to
            // separate ranges; repair their occupancy before persisting them.
            let (children, repaired) = Box::pin(self.merge_small_children(children)).await?;
            acc.add(repaired);
            let w = self.store.write_internal(children, buffer).await?;
            acc.io_bytes += w.io_bytes;
            Ok((vec![w.child_ref], acc))
        }
    }

    /// Remove a routing level when its contents fit in the root. Multiple
    /// children use the coalesce ceiling so growth and shrinkage have hysteresis.
    async fn maybe_shrink_root(&mut self) -> Result<()> {
        while !self.children.is_empty() && self.children[0].height > 0 {
            if self.children.len() > 1 {
                // Interior byte_size is the exact encoded children-plus-buffer
                // body. Rule out a collapse without fetching every child.
                let bytes = self.children.iter().try_fold(
                    node::internal_logical_bytes(&[], &self.buffer),
                    |bytes, child| bytes.checked_add(child.object_size),
                );
                if bytes.is_none_or(|bytes| bytes > self.config.coalesce_ceiling()) {
                    break;
                }
            }
            let mut children = Vec::new();
            let mut buffer = self.buffer.clone();
            for child in &self.children {
                let node = self.store.read_internal(child).await?;
                children.extend(node.children);
                buffer.extend(node.buffer);
            }
            if node::internal_overflows(&children, &buffer, &self.config) {
                break;
            }
            self.children = children;
            self.buffer = buffer;
            self.buffer_index.take();
            node::fence(&mut self.children, 0);
        }
        Ok(())
    }

    /// Apply mutations and share repeated field mappings.
    fn replay_verified(
        &self,
        fragments: &mut BTreeMap<u64, Fragment>,
        actions: Vec<pb::FragmentMetadataMutation>,
    ) -> Result<()> {
        self.store.apply_verified(fragments, actions)
    }

    fn start_walk_from(&self, lower_bound: u64) -> FragmentWalk {
        let mut walk = FragmentWalk {
            lower_bound,
            ..Default::default()
        };
        if self.children.is_empty() {
            let mut fragments = BTreeMap::new();
            match self.replay_verified(&mut fragments, self.buffer.clone()) {
                Ok(()) => walk.ready.extend(fragments.into_values()),
                Err(error) => walk.pending_error = Some(error),
            }
        } else {
            let action_buckets =
                node::partition_buffer_by_child(&self.children, self.buffer.clone());
            if let Err(error) =
                node::validate_routed(&self.children, &action_buckets, node::ROOT_EXCLUSIVE_END)
            {
                walk.pending_error = Some(error);
                return walk;
            }
            let first = node::child_index_for(&self.children, lower_bound);
            for (child, actions) in self
                .children
                .iter()
                .cloned()
                .zip(action_buckets)
                .skip(first)
                .rev()
            {
                walk.stack.push((child, actions));
            }
        }
        walk
    }

    /// Advance an in-order walk by one fragment, reading one leaf at a time.
    async fn walk_next(&self, walk: &mut FragmentWalk) -> Result<Option<Fragment>> {
        if let Some(error) = walk.pending_error.take() {
            return Err(error);
        }
        loop {
            if let Some(fragment) = walk.ready.pop_front() {
                if fragment.id < walk.lower_bound {
                    continue;
                }
                return Ok(Some(fragment));
            }
            let Some((child, actions)) = self.walk_next_leaf(walk).await? else {
                return Ok(None);
            };
            let mut fragments: BTreeMap<u64, Fragment> = self
                .store
                .read_leaf(&child)
                .await?
                .into_iter()
                .map(|fragment| (fragment.id, fragment))
                .collect();
            self.replay_verified(&mut fragments, actions)?;
            walk.ready.extend(fragments.into_values());
        }
    }

    /// Traverse routing once, yielding leaves and their inherited actions in
    /// order. Leaf I/O can then be prefetched without buffering the full table.
    async fn walk_next_leaf(&self, walk: &mut FragmentWalk) -> Result<Option<BufferedChild>> {
        while let Some((child, mut actions)) = walk.stack.pop() {
            if child.height == 0 {
                return Ok(Some((child, actions)));
            }
            let internal = self.store.read_internal(&child).await?;
            actions.extend(internal.buffer);
            if internal.children.is_empty() {
                return Err(super::validation::corrupt(format!(
                    "Interior {} has no children",
                    child.path
                )));
            }
            let action_buckets = node::partition_buffer_by_child(&internal.children, actions);
            node::validate_routed(
                &internal.children,
                &action_buckets,
                node::ROOT_EXCLUSIVE_END,
            )?;
            let first = node::child_index_for(&internal.children, walk.lower_bound);
            for (child, actions) in internal
                .children
                .into_iter()
                .zip(action_buckets)
                .skip(first)
                .rev()
            {
                walk.stack.push((child, actions));
            }
        }
        Ok(None)
    }

    /// Read every leaf and every internal buffer below `children` into the
    /// full-materialization accumulators. Table sized by definition; only
    /// [`Self::materialize`] and the oracle tests use it.
    fn collect_subtree<'a>(
        &'a self,
        children: Vec<pb::FragmentMetadataChild>,
        frags: &'a mut BTreeMap<u64, Fragment>,
        actions: &'a mut Vec<pb::FragmentMetadataMutation>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for child in &children {
                if child.height == 0 {
                    for f in self.store.read_leaf(child).await? {
                        frags.insert(f.id, f);
                    }
                } else {
                    let node = self.store.read_internal(child).await?;
                    actions.extend(node.buffer);
                    self.collect_subtree(node.children, frags, actions).await?;
                }
            }
            Ok(())
        })
    }

    fn collect_covering_leaves<'a>(
        &'a self,
        children: Vec<pb::FragmentMetadataChild>,
        lower_bound: u64,
        upper_bound: Option<u64>,
        fragment_ids: &'a RoaringBitmap,
        leaves: &'a mut Vec<pb::FragmentMetadataChild>,
        actions: &'a mut Vec<pb::FragmentMetadataMutation>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for (index, child) in children.iter().enumerate() {
                let child_lower_bound = if index == 0 {
                    lower_bound
                } else {
                    child.min_key
                };
                let child_upper_bound = children
                    .get(index + 1)
                    .map(|next| next.min_key)
                    .or(upper_bound);
                if !bitmap_intersects_range(fragment_ids, child_lower_bound, child_upper_bound) {
                    continue;
                }
                if child.height == 0 {
                    leaves.push(child.clone());
                } else {
                    let internal = self.store.read_internal(child).await?;
                    actions.extend(
                        internal.buffer.into_iter().filter(|tagged| {
                            bitmap_contains(fragment_ids, node::action_key(tagged))
                        }),
                    );
                    self.collect_covering_leaves(
                        internal.children,
                        child_lower_bound,
                        child_upper_bound,
                        fragment_ids,
                        leaves,
                        actions,
                    )
                    .await?;
                }
            }
            Ok(())
        })
    }

    fn collect_internal_sizes<'a>(
        &'a self,
        children: Vec<pb::FragmentMetadataChild>,
        out: &'a mut Vec<(u32, u64)>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for c in &children {
                if c.height > 0 {
                    out.push((c.height, c.object_size));
                    let node = self.store.read_internal(c).await?;
                    self.collect_internal_sizes(node.children, out).await?;
                }
            }
            Ok(())
        })
    }
}

fn walk_fragments(
    fragments: impl IntoIterator<Item = Fragment>,
    mut remaining: u64,
) -> OffsetResolution {
    for fragment in fragments {
        // Counts are known by the writer invariant; a violation would have
        // been rejected at commit.
        let visible = fragment.num_rows().unwrap_or(0) as u64;
        if remaining < visible {
            return OffsetResolution::Found(Box::new(fragment), remaining);
        }
        remaining -= visible;
    }
    OffsetResolution::BeyondEnd
}

fn bitmap_contains(fragment_ids: &RoaringBitmap, fragment_id: u64) -> bool {
    u32::try_from(fragment_id)
        .map(|fragment_id| fragment_ids.contains(fragment_id))
        .unwrap_or(false)
}

fn bitmap_intersects_range(
    fragment_ids: &RoaringBitmap,
    lower_bound: u64,
    upper_bound: Option<u64>,
) -> bool {
    if upper_bound.is_some_and(|upper_bound| upper_bound <= lower_bound) {
        return false;
    }
    let Ok(lower_bound) = u32::try_from(lower_bound) else {
        return false;
    };
    let upper_bound = upper_bound
        .and_then(|upper_bound| u32::try_from(upper_bound).ok())
        .map(|upper_bound| upper_bound.saturating_sub(1))
        .unwrap_or(u32::MAX);
    lower_bound <= upper_bound && fragment_ids.range_cardinality(lower_bound..=upper_bound) > 0
}

#[cfg(test)]
mod tests;
