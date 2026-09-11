// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Prepare immutable tree objects for publication by an authoritative manifest.
//! This path never writes a version-numbered root or races a second commit CAS.

use super::*;
use crate::format::pb::fragment_metadata_tree::Root;

/// Publication byte budgets, independent of leaf and semantic-buffer budgets.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotPolicy {
    /// Embed root and descriptor only when their complete encoded contribution
    /// to the Version Manifest fits this size. Zero disables embedding.
    pub inline_root_bytes: usize,
    /// Maximum encoded cumulative suffix. Zero writes an external root every commit.
    pub max_suffix_bytes: usize,
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self {
            inline_root_bytes: 64 * 1024,
            max_suffix_bytes: 32 * 1024,
        }
    }
}

impl FragmentMetadataTree {
    /// Resolve validation state while retaining complete leaf reads on local
    /// scratch storage for a subsequent bulk materialization. Scratch is owned
    /// by this writer and never referenced by a published snapshot.
    pub async fn resolve_touched_for_bulk(&mut self, ids: &[u64]) -> Result<TouchedFragments> {
        self.store.retain_validation_reads()?;
        self.resolve_touched(ids).await
    }

    /// Immutable node paths required by this snapshot. Root bases and
    /// mutations_since_root belong to the manifest descriptor.
    pub async fn node_paths(&self) -> Result<Vec<String>> {
        let mut pending = self.children.clone();
        let mut paths = std::collections::BTreeSet::new();
        while let Some(child) = pending.pop() {
            if !paths.insert(child.path.clone()) {
                return Err(Error::invalid_input(format!(
                    "fragment metadata tree version {} repeats node {}",
                    self.version, child.path
                )));
            }
            if child.height > 0 {
                pending.extend(self.store.read_internal(&child).await?.children);
            }
        }
        Ok(paths.into_iter().collect())
    }

    /// Build immutable leaves and routing without publishing a version.
    /// Publish the returned snapshot through the dataset's Version Manifest.
    /// A failed publication leaves only unreachable immutable objects.
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap_snapshot(
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
        config: FragmentMetadataTreeConfig,
        mut fragments: Vec<Fragment>,
        version: u64,
        policy: SnapshotPolicy,
    ) -> Result<(Self, pb::FragmentMetadataTree, BootstrapStats)> {
        fragments.sort_by_key(|fragment| fragment.id);
        if fragments.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return Err(Error::invalid_input(
                "fragment metadata tree bootstrap contains duplicate fragment IDs",
            ));
        }
        let store = NodeStore::new(object_store, base, scheduler, cache);
        let (mut tree, mut stats) = Self::build(store, config, fragments).await?;
        tree.version = version;
        let (snapshot, bytes) = tree.checkpoint_snapshot(policy).await?;
        tree.snapshot = Some(Box::new(snapshot.clone()));
        stats.io_write_bytes += bytes;
        Ok((tree, snapshot, stats))
    }

    /// Open fragment state using only a manifest descriptor and its named base.
    /// `version` comes from that same authoritative manifest.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_snapshot(
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
        snapshot: &pb::FragmentMetadataTree,
        version: u64,
        config: FragmentMetadataTreeConfig,
        next_fragment_id: u64,
    ) -> Result<Self> {
        let mut store = NodeStore::new(object_store, base, scheduler, cache);
        store.next_action_sequence = snapshot.next_action_sequence;
        let root = match &snapshot.root {
            Some(Root::InlineRoot(root)) if snapshot.mutations_since_root.is_empty() => {
                root.clone()
            }
            Some(Root::InlineRoot(_)) => {
                return Err(Error::invalid_input(format!(
                    "fragment metadata tree version {version} has a suffix after an inline root"
                )));
            }
            Some(Root::RootPath(path)) => store.read_root_base(path).await?,
            None => {
                return Err(Error::invalid_input(format!(
                    "fragment metadata tree version {version} has no root base"
                )));
            }
        };
        let (base_fragments, base_rows, base_visible_rows) =
            super::super::validation::root(&root, next_fragment_id)?;
        store.hard_capacity_bytes = config.hard_capacity_bytes;
        super::super::validation::buffer(
            &snapshot.mutations_since_root,
            root.next_action_sequence,
            snapshot.next_action_sequence,
        )?;
        if next_fragment_id > u64::from(u32::MAX) + 1
            || snapshot
                .mutations_since_root
                .iter()
                .any(|tagged| node::action_key(tagged) >= next_fragment_id)
        {
            return Err(super::super::validation::corrupt(format!(
                "Invalid snapshot allocation frontier at version {version}"
            )));
        }
        if snapshot.next_action_sequence < root.next_action_sequence {
            return Err(Error::invalid_input(format!(
                "fragment metadata tree version {version} has next_action_sequence {} below the root's {}",
                snapshot.next_action_sequence, root.next_action_sequence
            )));
        }
        if snapshot.mutations_since_root.iter().any(|action| {
            action.action_sequence < root.next_action_sequence
                || action.action_sequence >= snapshot.next_action_sequence
        }) {
            return Err(Error::invalid_input(format!(
                "fragment metadata tree version {version} mutations_since_root crosses its root action sequence frontier"
            )));
        }
        let total_fragments = snapshot
            .mutations_since_root
            .iter()
            .try_fold(base_fragments, |value, action| {
                apply_aggregate_delta(value, action.fragment_count_delta, "Fragments")
            })?;
        let total_rows = snapshot
            .mutations_since_root
            .iter()
            .try_fold(base_rows, |value, action| {
                apply_aggregate_delta(value, action.total_rows_delta, "physical rows")
            })?;
        let visible_rows = snapshot
            .mutations_since_root
            .iter()
            .try_fold(base_visible_rows, |value, action| {
                apply_aggregate_delta(value, action.visible_rows_delta, "visible rows")
            })?;
        if visible_rows > total_rows {
            return Err(super::super::validation::corrupt(format!(
                "Derived visible rows {visible_rows} exceed physical rows {total_rows} at version {version}"
            )));
        }
        let mut buffer = root.buffer;
        buffer.extend(snapshot.mutations_since_root.iter().cloned());
        config.validate()?;
        Ok(Self {
            store,
            config,
            version,
            children: root.children,
            buffer: node::squash_buffer(buffer),
            buffer_index: OnceLock::new(),
            next_action_sequence: snapshot.next_action_sequence,
            total_fragments,
            total_rows,
            visible_rows,
            next_fragment_id,
            force_flush: false,
            snapshot: Some(Box::new(snapshot.clone())),
        })
    }

    /// Prepare a validated mutation for manifest publication. No version becomes
    /// visible here. On failure, the in-memory tree is restored; objects already
    /// written remain unreachable until normal retention GC removes them.
    pub async fn prepare_snapshot(
        &mut self,
        commit: ValidatedCommit,
        touched: &TouchedFragments,
        previous_snapshot: &pb::FragmentMetadataTree,
        policy: SnapshotPolicy,
        bulk: bool,
    ) -> Result<(pb::FragmentMetadataTree, CommitStats)> {
        if self.snapshot.as_deref() != Some(previous_snapshot) {
            return Err(Error::invalid_input(format!(
                "fragment metadata tree version {} received a descriptor from another validation state",
                self.version
            )));
        }
        let deltas =
            commit::aggregate_deltas(&commit.fragment_actions, touched, self.next_fragment_id)?;
        let previous = self.mutable_state();
        let result = self
            .prepare_snapshot_inner(commit, deltas, previous_snapshot, policy, bulk)
            .await;
        self.force_flush = false;
        self.store.clear_validation_reads();
        if result.is_err() {
            self.restore_mutable_state(previous);
        }
        if let Ok((snapshot, _)) = &result {
            self.snapshot = Some(Box::new(snapshot.clone()));
        }
        result
    }

    async fn prepare_snapshot_inner(
        &mut self,
        commit: ValidatedCommit,
        deltas: Vec<commit::ActionDeltas>,
        previous: &pb::FragmentMetadataTree,
        policy: SnapshotPolicy,
        bulk: bool,
    ) -> Result<(pb::FragmentMetadataTree, CommitStats)> {
        let tagged = self.stage_commit(&commit, deltas)?;
        let messages_in = tagged.len() as u64;
        let before = self.buffer.len();
        self.buffer = node::squash_buffer(std::mem::take(&mut self.buffer));
        let squashed = before - self.buffer.len();
        let mut suffix = previous.mutations_since_root.clone();
        suffix.extend(tagged);
        let suffix = node::squash_buffer(suffix);
        if !bulk
            && matches!(&previous.root, Some(Root::RootPath(_)))
            && !node::internal_overflows(&self.children, &self.buffer, &self.config)
            && node::internal_logical_bytes(&[], &suffix) <= policy.max_suffix_bytes as u64
        {
            return Ok((
                self.snapshot_descriptor(previous.root.clone(), suffix),
                CommitStats {
                    messages_in,
                    messages_squashed: squashed as u64,
                    height: self.height(),
                    root_buffer_len: self.buffer.len() as u64,
                    ..Default::default()
                },
            ));
        }
        self.force_flush = bulk;
        let acc = self.rewrite_tree().await?;
        let (snapshot, bytes) = self.checkpoint_snapshot(policy).await?;
        Ok((
            snapshot,
            CommitStats {
                tree_write_bytes: acc.io_bytes + bytes,
                messages_in,
                messages_materialized: acc.materialized,
                messages_squashed: squashed as u64 + acc.squashed,
                flushes: acc.flushes,
                splits: acc.splits,
                merges: acc.merges,
                max_flush_depth: acc.max_flush_depth,
                height: self.height(),
                root_buffer_len: self.buffer.len() as u64,
                checkpoints: 1,
            },
        ))
    }

    fn snapshot_descriptor(
        &self,
        root: Option<Root>,
        mutations_since_root: Vec<pb::FragmentMetadataMutation>,
    ) -> pb::FragmentMetadataTree {
        pb::FragmentMetadataTree {
            root,
            mutations_since_root,
            next_action_sequence: self.next_action_sequence,
        }
    }

    async fn checkpoint_snapshot(
        &self,
        policy: SnapshotPolicy,
    ) -> Result<(pb::FragmentMetadataTree, u64)> {
        let root = self.compacted_root();
        if root.encoded_len() as u64 > self.config.hard_capacity_bytes {
            return Err(Error::invalid_input(format!(
                "fragment metadata root envelope requires {} encoded bytes, exceeding hard_capacity_bytes={}",
                root.encoded_len(),
                self.config.hard_capacity_bytes
            )));
        }
        let mut snapshot = self.snapshot_descriptor(Some(Root::InlineRoot(root)), Vec::new());
        if snapshot.encoded_len() <= policy.inline_root_bytes {
            return Ok((snapshot, 0));
        }
        let Some(Root::InlineRoot(root)) = &snapshot.root else {
            return Err(Error::internal(
                "prepared fragment metadata snapshot has no inline root",
            ));
        };
        let (path, bytes) = self.store.write_root_base(root).await?;
        snapshot.root = Some(Root::RootPath(path));
        Ok((snapshot, bytes))
    }
}
