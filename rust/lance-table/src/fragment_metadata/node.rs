// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Routing, accounting, and reduction of validated fragment actions.
//!
//! Node policies use encoded bytes because fragments and actions vary in size.
//! Leaf encoding and object I/O live in `store`; recursive mutation lives in `tree`.

use std::collections::BTreeMap;

use prost::Message;

use crate::format::Fragment;
use crate::format::pb::{self, fragment_action::Action};
use crate::fragment_metadata::action;
use lance_core::{Error, Result};

/// Default routing and leaf targets. These are independent writer policies.
pub const DEFAULT_MAX_NODE_BYTES: u64 = 1024 * 1024;
pub const DEFAULT_MAX_LEAF_BYTES: u64 = 1024 * 1024;

/// Encoded-byte policy for immutable nodes and semantic buffers.
#[derive(Debug, Clone)]
pub struct FragmentMetadataTreeConfig {
    /// Target encoded routing-plus-buffer body size. Routing grows after flushing.
    pub max_node_bytes: u64,
    /// Target encoded Lance leaf size. A single fragment may exceed this target.
    pub max_leaf_bytes: u64,
    /// Pending bytes at which a flush drains children whose batches amortize a
    /// rewrite. Smaller batches stay buffered until `max_node_bytes` forces them.
    pub semantic_buffer_bytes: u64,
    /// Complete object limit, including root envelopes and singleton leaves.
    pub hard_capacity_bytes: u64,
    // A bounded fanout is useful for structural tests. Dataset writers grow by bytes.
    pub(crate) max_children_per_node: u32,
}

impl Default for FragmentMetadataTreeConfig {
    fn default() -> Self {
        Self {
            max_node_bytes: DEFAULT_MAX_NODE_BYTES,
            max_leaf_bytes: DEFAULT_MAX_LEAF_BYTES,
            semantic_buffer_bytes: 256 * 1024,
            hard_capacity_bytes: 64 * 1024 * 1024,
            max_children_per_node: u32::MAX,
        }
    }
}

impl FragmentMetadataTreeConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_node_bytes == 0
            || self.max_leaf_bytes == 0
            || self.semantic_buffer_bytes == 0
            || self.hard_capacity_bytes == 0
            || self.max_leaf_bytes > self.hard_capacity_bytes
            || self.max_node_bytes > self.hard_capacity_bytes
            || self.semantic_buffer_bytes > self.hard_capacity_bytes
            || self.max_node_bytes > u64::MAX / 3
            || self.max_leaf_bytes > u64::MAX / 3
            || self.max_children_per_node < 2
        {
            return Err(Error::invalid_input(format!(
                "fragment metadata tree config is invalid: max_node_bytes={}, \
                 max_leaf_bytes={}, semantic_buffer_bytes={}, hard_capacity_bytes={}, \
                 max_children_per_node={}",
                self.max_node_bytes,
                self.max_leaf_bytes,
                self.semantic_buffer_bytes,
                self.hard_capacity_bytes,
                self.max_children_per_node
            )));
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new(max_node_bytes: u64, max_children_per_node: u32) -> Self {
        Self {
            max_node_bytes,
            max_leaf_bytes: max_node_bytes,
            semantic_buffer_bytes: max_node_bytes,
            max_children_per_node,
            ..Self::default()
        }
    }

    pub fn with_max_leaf_bytes(mut self, bytes: u64) -> Self {
        self.max_leaf_bytes = bytes;
        self
    }
    pub fn with_semantic_buffer_bytes(mut self, bytes: u64) -> Self {
        self.semantic_buffer_bytes = bytes;
        self
    }
    pub fn with_hard_capacity_bytes(mut self, bytes: u64) -> Self {
        self.hard_capacity_bytes = bytes;
        self
    }
    pub fn split_ceiling(&self) -> u64 {
        self.max_node_bytes
    }
    pub fn split_piece_bytes(&self) -> u64 {
        self.max_node_bytes / 2
    }
    pub fn merge_floor(&self) -> u64 {
        self.max_node_bytes / 4
    }
    pub fn coalesce_ceiling(&self) -> u64 {
        self.max_node_bytes * 3 / 5
    }
    pub fn leaf_split_ceiling(&self) -> u64 {
        self.max_leaf_bytes
    }
    pub fn leaf_merge_floor(&self) -> u64 {
        self.max_leaf_bytes / 4
    }
    pub fn leaf_coalesce_ceiling(&self) -> u64 {
        self.max_leaf_bytes * 3 / 5
    }
}

/// An internal node: child pivots (sorted by `min_key`, contiguous) + the
/// ε-buffer. The buffer is kept in action_sequence (insertion) order in memory and sorted by
/// `(key, action_sequence)` only when grouping for a flush.
#[derive(Debug, Clone, Default)]
pub struct InternalNode {
    pub children: Vec<pb::FragmentMetadataChild>,
    pub buffer: Vec<pb::FragmentMetadataMutation>,
}

/// Derived offsets into an immutable buffer. The wire representation stays a
/// plain action sequence; a sparse lookup need not examine unrelated actions.
#[derive(Debug, Clone)]
pub(crate) struct BufferIndex {
    offsets: Vec<(u64, usize)>,
}

impl BufferIndex {
    pub(crate) fn new(buffer: &[pb::FragmentMetadataMutation]) -> Self {
        let mut offsets: Vec<_> = buffer
            .iter()
            .enumerate()
            .map(|(offset, action)| (action_key(action), offset))
            .collect();
        offsets.sort_unstable();
        Self { offsets }
    }

    pub(crate) fn for_fragment(&self, id: u64) -> impl Iterator<Item = usize> + '_ {
        let start = self.offsets.partition_point(|(key, _)| *key < id);
        self.offsets[start..]
            .iter()
            .take_while(move |(key, _)| *key == id)
            .map(|(_, offset)| *offset)
    }
}

/// Fragment protobuf bytes, used to bound candidate encoding work and report amplification.
pub fn fragment_logical_bytes(fragment: &Fragment) -> u64 {
    pb::DataFragment::from(fragment).encoded_len() as u64
}

/// Fragment protobuf bytes before Lance encoding; this is not the leaf capacity metric.
pub fn leaf_logical_bytes(fragments: &[Fragment]) -> u64 {
    fragments.iter().map(fragment_logical_bytes).sum()
}

fn varint_bytes(mut value: u64) -> u64 {
    let mut bytes = 1;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn repeated_message_bytes(message: &impl Message) -> u64 {
    let payload_bytes = message.encoded_len() as u64;
    1 + varint_bytes(payload_bytes) + payload_bytes
}

/// Logical byte size of an internal node = its exact encoded protobuf size.
pub fn internal_logical_bytes(
    children: &[pb::FragmentMetadataChild],
    buffer: &[pb::FragmentMetadataMutation],
) -> u64 {
    children.iter().map(repeated_message_bytes).sum::<u64>()
        + buffer.iter().map(repeated_message_bytes).sum::<u64>()
}

/// Whether an internal node violates its encoded-byte or fanout limit. This is
/// the structural bound. A node over it must drain or split.
pub fn internal_overflows(
    children: &[pb::FragmentMetadataChild],
    buffer: &[pb::FragmentMetadataMutation],
    config: &FragmentMetadataTreeConfig,
) -> bool {
    children.len() as u32 > config.max_children_per_node
        || internal_logical_bytes(children, buffer) >= config.split_ceiling()
}

/// Whether pending bytes exceed the semantic buffer. This is the soft bound.
/// The writer drains children whose batches amortize a rewrite and keeps the
/// rest buffered until structural pressure forces progress.
pub fn buffer_pressured(
    buffer: &[pb::FragmentMetadataMutation],
    config: &FragmentMetadataTreeConfig,
) -> bool {
    internal_logical_bytes(&[], buffer) >= config.semantic_buffer_bytes
}

/// The target key of a buffered action (the fragment id it mutates).
pub fn action_key(t: &pb::FragmentMetadataMutation) -> u64 {
    t.action
        .as_ref()
        .and_then(action::target_frag_id)
        .unwrap_or(0)
}

/// Index of the child owning `key`: the rightmost child whose `min_key` is at
/// most `key`, and the first child for keys below every fence.
///
/// Routing invariant: sibling `min_key` values are the only fences. The last
/// child's exclusive end is inherited from its parent (`2^32` at the root).
/// Keys below the first fence belong to the first child, so an emptied
/// node must be dropped rather than kept with `min_key = 0`.
pub fn child_index_for(children: &[pb::FragmentMetadataChild], key: u64) -> usize {
    match children.binary_search_by(|c| c.min_key.cmp(&key)) {
        Ok(i) => i,
        Err(0) => 0,
        Err(i) => i - 1,
    }
}

/// Pin the first child's fence to the node's lower bound. A rebuilt child list
/// starts at its first stored key, but the range it owns starts where the
/// parent says it does. That is 0 at the root and the parent's entry below it.
pub fn fence(children: &mut [pb::FragmentMetadataChild], lower_bound: u64) {
    if let Some(first) = children.first_mut() {
        debug_assert!(lower_bound <= first.min_key);
        first.min_key = lower_bound;
    }
}

/// Exclusive end of the last child of a root. Fragment IDs are `u32`.
pub const ROOT_EXCLUSIVE_END: u64 = 1u64 << 32;

/// Exclusive end of `children[index]` inside a parent whose last child ends
/// at `parent_end`.
pub fn exclusive_end(children: &[pb::FragmentMetadataChild], index: usize, parent_end: u64) -> u64 {
    children
        .get(index + 1)
        .map(|child| child.min_key)
        .unwrap_or(parent_end)
}

/// Build a child reference for a leaf that has just been written.
pub fn leaf_ref(
    path: String,
    fragments: &[Fragment],
    object_size: u64,
    materialized_through_action_sequence: u64,
) -> Result<pb::FragmentMetadataChild> {
    let total_rows = sum_aggregate_values(
        fragments
            .iter()
            .map(|fragment| fragment.physical_rows.unwrap_or(0) as u64),
        "total_rows",
    )?;
    // Counts are known by the writer invariant
    // (`commit::require_known_counts`); a violation here means state
    // bypassed validation.
    let visible_rows = sum_aggregate_values(
        fragments
            .iter()
            .map(|fragment| fragment.num_rows().unwrap_or(0) as u64),
        "visible_rows",
    )?;
    Ok(pb::FragmentMetadataChild {
        path,
        base_id: None,
        min_key: fragments.first().map(|f| f.id).unwrap_or(0),
        num_keys: fragments.len() as u64,
        height: 0,
        num_children: 0,
        total_rows,
        object_size,
        materialized_through_action_sequence,
        visible_rows,
    })
}

/// Build a child reference for an internal node that has just been written.
pub fn internal_ref(
    path: String,
    children: &[pb::FragmentMetadataChild],
    buffer: &[pb::FragmentMetadataMutation],
    object_size: u64,
) -> Result<pb::FragmentMetadataChild> {
    let fragment_count_delta = sum_aggregate_deltas(
        buffer.iter().map(|action| action.fragment_count_delta),
        "fragment_count_delta",
    )?;
    let total_rows_delta = sum_aggregate_deltas(
        buffer.iter().map(|action| action.total_rows_delta),
        "total_rows_delta",
    )?;
    let num_keys = apply_aggregate_delta(
        sum_aggregate_values(children.iter().map(|child| child.num_keys), "num_keys")?,
        fragment_count_delta,
        "num_keys",
    )?;
    let total_rows = apply_aggregate_delta(
        sum_aggregate_values(children.iter().map(|child| child.total_rows), "total_rows")?,
        total_rows_delta,
        "total_rows",
    )?;
    let visible_rows_delta = sum_aggregate_deltas(
        buffer.iter().map(|action| action.visible_rows_delta),
        "visible_rows_delta",
    )?;
    let visible_rows = apply_aggregate_delta(
        sum_aggregate_values(
            children.iter().map(|child| child.visible_rows),
            "visible_rows",
        )?,
        visible_rows_delta,
        "visible_rows",
    )?;
    let height = children
        .iter()
        .map(|c| c.height)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| super::validation::corrupt("Internal node height exceeds u32"))?;
    Ok(pb::FragmentMetadataChild {
        path,
        base_id: None,
        min_key: children.first().map(|c| c.min_key).unwrap_or(0),
        num_keys,
        height,
        num_children: children.len() as u32,
        total_rows,
        object_size,
        materialized_through_action_sequence: 0,
        visible_rows,
    })
}

pub(super) fn apply_aggregate_delta(base: u64, delta: i64, name: &str) -> Result<u64> {
    if delta >= 0 {
        base.checked_add(delta as u64).ok_or_else(|| {
            Error::invalid_input(format!(
                "fragment metadata tree {name} aggregate overflow: base={base}, delta={delta}"
            ))
        })
    } else {
        base.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
            Error::invalid_input(format!(
                "fragment metadata tree {name} aggregate underflow: base={base}, delta={delta}"
            ))
        })
    }
}

pub(super) fn sum_aggregate_deltas(
    deltas: impl IntoIterator<Item = i64>,
    name: &str,
) -> Result<i64> {
    deltas.into_iter().try_fold(0i64, |sum, delta| {
        sum.checked_add(delta).ok_or_else(|| {
            Error::invalid_input(format!(
                "fragment metadata tree {name} overflow while summing: sum={sum}, delta={delta}"
            ))
        })
    })
}

fn sum_aggregate_values(values: impl IntoIterator<Item = u64>, name: &str) -> Result<u64> {
    values.into_iter().try_fold(0u64, |sum, value| {
        sum.checked_add(value).ok_or_else(|| {
            Error::invalid_input(format!(
                "fragment metadata tree {name} overflow while summing: sum={sum}, value={value}"
            ))
        })
    })
}

/// Is this child underflowing (a merge candidate)? Leaves underflow by bytes.
/// Internal nodes must be sparse by both direct-child count and their exact
/// encoded size so a hot ε-buffer is never merged as "small."
pub fn is_underflow(
    child: &pb::FragmentMetadataChild,
    config: &FragmentMetadataTreeConfig,
) -> bool {
    if child.height == 0 {
        child.object_size <= config.leaf_merge_floor()
    } else {
        child.num_children < (config.max_children_per_node / 4).max(1)
            && child.object_size <= config.merge_floor()
    }
}

/// Whether replay checks each mutation's count deltas against the record it
/// changes. Storage paths verify. The reducer oracle and lowering compare
/// records only, with deltas left unset.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeltaCheck {
    Trust,
    Verify,
}

/// Apply buffered mutations to an id-keyed fragment map in `(key, action_sequence)` order.
/// `action_sequence` is assigned at commit time and only ever grows, so this
/// order is the commit order per fragment wherever actions meet.
pub fn apply_actions(
    frags: &mut BTreeMap<u64, Fragment>,
    actions: Vec<pb::FragmentMetadataMutation>,
) -> Result<()> {
    apply_mutations(frags, actions, DeltaCheck::Trust)
}

/// Apply mutations and reject any whose stored deltas differ from the change
/// they make. The record before and after is in hand here, so this is where
/// the format's exact-delta rule is enforced.
pub fn apply_verified(
    frags: &mut BTreeMap<u64, Fragment>,
    actions: Vec<pb::FragmentMetadataMutation>,
) -> Result<()> {
    apply_mutations(frags, actions, DeltaCheck::Verify)
}

fn record_counts(fragment: Option<&Fragment>) -> (i64, i64, i64) {
    fragment.map_or((0, 0, 0), |fragment| {
        (
            1,
            fragment.physical_rows.unwrap_or(0) as i64,
            fragment.num_rows().unwrap_or(0) as i64,
        )
    })
}

fn apply_mutations(
    frags: &mut BTreeMap<u64, Fragment>,
    mut actions: Vec<pb::FragmentMetadataMutation>,
    check: DeltaCheck,
) -> Result<()> {
    actions.sort_unstable_by_key(|tagged| tagged.action_sequence);
    if actions
        .windows(2)
        .any(|pair| pair[0].action_sequence == pair[1].action_sequence)
    {
        return Err(super::validation::corrupt(
            "Duplicate action sequence number on a fragment replay path",
        ));
    }
    for tagged in &actions {
        super::validation::action(tagged.action.as_ref().ok_or_else(|| {
            super::validation::corrupt(format!(
                "Buffered action sequence number {} has no action",
                tagged.action_sequence
            ))
        })?)?;
    }
    actions.sort_by_key(|t| (action_key(t), t.action_sequence));
    for tagged in actions {
        let key = action_key(&tagged);
        let before = record_counts(frags.get(&key));
        if let Some(action) = tagged.action {
            apply_one(frags, action)?;
        }
        if check == DeltaCheck::Verify {
            let after = record_counts(frags.get(&key));
            let actual = (after.0 - before.0, after.1 - before.1, after.2 - before.2);
            let stored = (
                tagged.fragment_count_delta,
                tagged.total_rows_delta,
                tagged.visible_rows_delta,
            );
            if actual != stored {
                return Err(super::validation::corrupt(format!(
                    "Mutation action_sequence={} for fragment {key} stores fragment, row, and visible deltas {} {} {} but the record changed by {} {} {}",
                    tagged.action_sequence,
                    stored.0,
                    stored.1,
                    stored.2,
                    actual.0,
                    actual.1,
                    actual.2
                )));
            }
        }
    }
    Ok(())
}

fn apply_one(frags: &mut BTreeMap<u64, Fragment>, action: pb::FragmentAction) -> Result<()> {
    let Some(action) = action.action else {
        return Err(super::validation::corrupt(
            "Buffered action has no recognized variant",
        ));
    };
    match action {
        Action::AddFragment(f) => {
            let fragment = super::validation::fragment(f)?;
            frags.insert(fragment.id, fragment);
        }
        Action::RemoveFragment(id) => {
            frags.remove(&id);
        }
        Action::AddDataFile(a) => {
            let file = crate::format::DataFile::try_from(
                a.file
                    .ok_or_else(|| Error::invalid_input("AddDataFile action missing file"))?,
            )?;
            let fragment = frags.get_mut(&a.frag_id).ok_or_else(|| {
                Error::invalid_input(format!(
                    "AddDataFile action targets missing frag_id={}",
                    a.frag_id
                ))
            })?;
            fragment.files.push(file);
        }
        Action::RemoveDataFile(a) => {
            let fragment = frags.get_mut(&a.frag_id).ok_or_else(|| {
                Error::invalid_input(format!(
                    "RemoveDataFile action targets missing frag_id={}",
                    a.frag_id
                ))
            })?;
            fragment.files.retain(|f| f.path != a.path);
        }
        Action::ReplaceDataFile(a) => {
            if a.expected_path.is_empty() || a.path.is_empty() {
                return Err(Error::invalid_input(format!(
                    "ReplaceDataFile for frag_id={} is missing expected_path or path",
                    a.frag_id
                )));
            }
            let fragment = frags.get_mut(&a.frag_id).ok_or_else(|| {
                Error::invalid_input(format!(
                    "ReplaceDataFile action targets missing frag_id={}",
                    a.frag_id
                ))
            })?;
            let matched = fragment
                .files
                .iter_mut()
                .find(|file| file.path == a.expected_path)
                .ok_or_else(|| {
                    Error::internal(format!(
                        "ReplaceDataFile for frag_id={} names data file {} which the fragment \
                         no longer holds; commit-time validation must have seen a different \
                         state",
                        a.frag_id, a.expected_path
                    ))
                })?;
            matched.path = a.path.clone();
            matched.file_size_bytes = lance_io::utils::CachedFileSize::new(a.file_size_bytes);
            matched.base_id = a.base_id;
        }
        Action::AddDeletionFile(a) => {
            let deletion_file = a.deletion_file.ok_or_else(|| {
                Error::invalid_input(format!(
                    "AddDeletionFile action for frag_id={} is missing deletion_file",
                    a.frag_id
                ))
            })?;
            let fragment = frags.get_mut(&a.frag_id).ok_or_else(|| {
                Error::invalid_input(format!(
                    "AddDeletionFile action targets missing frag_id={}",
                    a.frag_id
                ))
            })?;
            let count = usize::try_from(deletion_file.num_deleted_rows)
                .map_err(|_| super::validation::corrupt("Deleted row count exceeds usize"))?;
            let mut file = crate::format::DeletionFile::try_from(deletion_file)?;
            file.num_deleted_rows = Some(count);
            fragment.deletion_file = Some(file);
        }
        Action::ClearDeletionFile(a) => {
            let fragment = frags.get_mut(&a.frag_id).ok_or_else(|| {
                Error::invalid_input(format!(
                    "ClearDeletionFile action targets missing frag_id={}",
                    a.frag_id
                ))
            })?;
            fragment.deletion_file = None;
        }
    }
    Ok(())
}

/// Normalize validated current-state actions, preserving each fragment's effect.
///
/// This is not a transaction validator: superseding a prefix is legal only
/// after that prefix has been validated. Committed transaction history must
/// retain the original actions. A run must also be a contiguous per-fragment
/// segment of the action sequence number history; never normalize across an unapplied message
/// held by another node.
///
/// The last whole-fragment reset establishes a known state, so its suffix can
/// be evaluated once. Otherwise, deletion-file assignments form an independent
/// last-writer-wins register, while file edits retain their order and first-path
/// matching semantics. In particular, a path-changing replacement chain cannot
/// be combined blindly: the second edit might match a different file slot.
///
/// Combined actions retain the last contributing action sequence number and the sum of aggregate
/// deltas. If a sum cannot be represented, leave that run unchanged.
pub fn squash_buffer(
    buffer: Vec<pb::FragmentMetadataMutation>,
) -> Vec<pb::FragmentMetadataMutation> {
    let mut runs: BTreeMap<u64, Vec<pb::FragmentMetadataMutation>> = BTreeMap::new();
    for tagged in buffer {
        runs.entry(action_key(&tagged)).or_default().push(tagged);
    }
    let mut squashed = Vec::new();
    for (key, mut run) in runs {
        run.sort_by_key(|tagged| tagged.action_sequence);
        if run.len() > 1 {
            run = squash_run(key, run);
        }
        squashed.extend(run);
    }
    squashed.sort_by_key(|tagged| tagged.action_sequence);
    squashed
}

fn combined(
    run: &[pb::FragmentMetadataMutation],
    action: Action,
) -> Option<pb::FragmentMetadataMutation> {
    Some(pb::FragmentMetadataMutation {
        action_sequence: run.last()?.action_sequence,
        action: Some(pb::FragmentAction {
            action: Some(action),
        }),
        fragment_count_delta: run
            .iter()
            .try_fold(0i64, |sum, t| sum.checked_add(t.fragment_count_delta))?,
        total_rows_delta: run
            .iter()
            .try_fold(0i64, |sum, t| sum.checked_add(t.total_rows_delta))?,
        visible_rows_delta: run
            .iter()
            .try_fold(0i64, |sum, t| sum.checked_add(t.visible_rows_delta))?,
    })
}

fn squash_run(
    key: u64,
    run: Vec<pb::FragmentMetadataMutation>,
) -> Vec<pb::FragmentMetadataMutation> {
    if run
        .iter()
        .any(|t| t.action.as_ref().and_then(|a| a.action.as_ref()).is_none())
    {
        return run;
    }
    if let Some(reset) = run.iter().rposition(|t| {
        matches!(
            t.action.as_ref().and_then(|a| a.action.as_ref()),
            Some(Action::AddFragment(_)) | Some(Action::RemoveFragment(_))
        )
    }) {
        let mut state = BTreeMap::new();
        if apply_actions(&mut state, run[reset..].to_vec()).is_err() {
            return run;
        }
        let action = match state.remove(&key) {
            Some(fragment) => Action::AddFragment(pb::DataFragment::from(&fragment)),
            None => Action::RemoveFragment(key),
        };
        return combined(&run, action).map_or(run, |t| vec![t]);
    }

    // File edits leave the deletion register and fragment existence unchanged.
    // Its assignments therefore commute across those edits on validated input.
    let mut deletion: Vec<pb::FragmentMetadataMutation> = Vec::new();
    let mut files: Vec<pb::FragmentMetadataMutation> = Vec::with_capacity(run.len());
    for tagged in &run {
        let Some(next) = tagged.action.as_ref().and_then(|a| a.action.as_ref()) else {
            return run;
        };
        if matches!(
            next,
            Action::AddDeletionFile(_) | Action::ClearDeletionFile(_)
        ) {
            deletion.push(tagged.clone());
            continue;
        }
        let mut current = tagged.clone();
        while let Some(action) = files.last().and_then(|prev| {
            combine_file_pair(
                prev.action.as_ref()?.action.as_ref()?,
                current.action.as_ref()?.action.as_ref()?,
            )
        }) {
            let start = files.len() - 1;
            let pair = [files[start].clone(), current];
            let Some(merged) = combined(&pair, action) else {
                return run;
            };
            files.pop();
            current = merged;
        }
        files.push(current);
    }
    if let Some(last) = deletion.last() {
        let Some(action) = last.action.as_ref().and_then(|a| a.action.clone()) else {
            return run;
        };
        let Some(merged) = combined(&deletion, action) else {
            return run;
        };
        files.push(merged);
    }
    files.sort_by_key(|t| t.action_sequence);
    files
}

/// Only identities valid for ordered lists with arbitrary path aliases belong
/// here. Path uniqueness is not part of the storage action contract.
fn combine_file_pair(first: &Action, second: &Action) -> Option<Action> {
    match (first, second) {
        (Action::RemoveDataFile(a), Action::RemoveDataFile(b)) if a.path == b.path => {
            Some(Action::RemoveDataFile(b.clone()))
        }
        (Action::AddDataFile(a), Action::RemoveDataFile(b)) if a.file.as_ref()?.path == b.path => {
            // The remove still has to delete any same-path files in the base.
            Some(Action::RemoveDataFile(b.clone()))
        }
        (Action::ReplaceDataFile(a), Action::ReplaceDataFile(b))
            if a.expected_path == a.path && a.expected_path == b.expected_path =>
        {
            // The first edit did not rename the slot, so both find the same
            // first matching path, including when the base has duplicate paths.
            Some(Action::ReplaceDataFile(pb::ReplaceDataFile {
                frag_id: a.frag_id,
                expected_path: a.expected_path.clone(),
                path: b.path.clone(),
                file_size_bytes: b.file_size_bytes,
                base_id: b.base_id,
            }))
        }
        _ => None,
    }
}

/// Partition a buffer into one bucket per child by the routing fences. The
/// returned vec has `children.len()` buckets; the buffer is consumed. Callers
/// must not pass an empty `children` slice.
pub fn partition_buffer_by_child(
    children: &[pb::FragmentMetadataChild],
    buffer: Vec<pb::FragmentMetadataMutation>,
) -> Vec<Vec<pb::FragmentMetadataMutation>> {
    let mut buckets: Vec<Vec<pb::FragmentMetadataMutation>> = vec![Vec::new(); children.len()];
    for tagged in buffer {
        let idx = child_index_for(children, action_key(&tagged));
        buckets[idx].push(tagged);
    }
    buckets
}

/// Reject buckets that routing could only have produced from a corrupt node.
/// A target outside the child's inherited range, or at or below its leaf
/// watermark, is already applied or was never owned by this child.
pub fn validate_routed(
    children: &[pb::FragmentMetadataChild],
    buckets: &[Vec<pb::FragmentMetadataMutation>],
    parent_end: u64,
) -> Result<()> {
    for (index, (child, bucket)) in children.iter().zip(buckets).enumerate() {
        let exclusive_end = exclusive_end(children, index, parent_end);
        for tagged in bucket {
            let target = action_key(tagged);
            if target < child.min_key || target >= exclusive_end {
                return Err(super::validation::corrupt(format!(
                    "Mutation action_sequence={} targets fragment {target} outside [{}, {exclusive_end})",
                    tagged.action_sequence, child.min_key
                )));
            }
            if child.height == 0
                && tagged.action_sequence <= child.materialized_through_action_sequence
            {
                return Err(super::validation::corrupt(format!(
                    "Mutation action_sequence={} for fragment {target} is at or below the watermark {} of leaf {}",
                    tagged.action_sequence, child.materialized_through_action_sequence, child.path
                )));
            }
        }
    }
    Ok(())
}

/// Sum of encoded bytes of a set of buffered actions.
pub fn buffer_bytes(buffer: &[pb::FragmentMetadataMutation]) -> u64 {
    buffer.iter().map(|t| t.encoded_len() as u64).sum()
}

/// Split an internal node's (children, buffer) into contiguous pieces targeting
/// `piece_bytes` and at most `max_children_per_node` children each. The buffer
/// follows its child by key range. A single indivisible child-plus-message unit
/// may exceed the byte target, but never the original node's split ceiling.
pub fn split_internal(
    children: Vec<pb::FragmentMetadataChild>,
    buffer: Vec<pb::FragmentMetadataMutation>,
    piece_bytes: u64,
    max_children_per_node: u32,
) -> Vec<(
    Vec<pb::FragmentMetadataChild>,
    Vec<pb::FragmentMetadataMutation>,
)> {
    if children.is_empty() {
        return vec![(children, buffer)];
    }
    let action_buckets = partition_buffer_by_child(&children, buffer);
    let piece_bytes = piece_bytes.max(1);
    let max_children_per_node = max_children_per_node.max(1) as usize;
    let mut pieces = Vec::new();
    let mut piece_children = Vec::new();
    let mut piece_buffer = Vec::new();
    let mut encoded_bytes = 0u64;

    for (child, actions) in children.into_iter().zip(action_buckets) {
        let unit_bytes = repeated_message_bytes(&child)
            + actions.iter().map(repeated_message_bytes).sum::<u64>();
        let exceeds_piece = !piece_children.is_empty()
            && (piece_children.len() >= max_children_per_node
                || encoded_bytes + unit_bytes > piece_bytes);
        if exceeds_piece {
            pieces.push((
                std::mem::take(&mut piece_children),
                std::mem::take(&mut piece_buffer),
            ));
            encoded_bytes = 0;
        }
        encoded_bytes += unit_bytes;
        piece_children.push(child);
        piece_buffer.extend(actions);
    }
    pieces.push((piece_children, piece_buffer));
    // Greedy byte packing can leave one child at the right edge. Borrow the
    // preceding range when both resulting nodes still fit the target; its
    // messages must move with the routing fence, including inserts in gaps.
    if pieces.len() > 1
        && pieces
            .last()
            .is_some_and(|(children, _)| children.len() == 1)
    {
        let last = pieces.len() - 1;
        let (before, after) = pieces.split_at_mut(last);
        let (left_children, left_buffer) = &mut before[last - 1];
        let (right_children, right_buffer) = &mut after[0];
        if left_children.len() > 2 && max_children_per_node >= 2 {
            let borrowed = &left_children[left_children.len() - 1];
            let extra = repeated_message_bytes(borrowed)
                + left_buffer
                    .iter()
                    .filter(|action| action_key(action) >= borrowed.min_key)
                    .map(repeated_message_bytes)
                    .sum::<u64>();
            if internal_logical_bytes(right_children, right_buffer) + extra <= piece_bytes {
                let fence = borrowed.min_key;
                let (kept, moved): (Vec<_>, Vec<_>) = std::mem::take(left_buffer)
                    .into_iter()
                    .partition(|action| action_key(action) < fence);
                *left_buffer = kept;
                right_buffer.extend(moved);
                right_children.insert(0, left_children.remove(left_children.len() - 1));
            }
        }
    }
    pieces
}

#[cfg(test)]
mod tests {
    mod reduction;
    use super::*;
    use crate::format::{DeletionFile, DeletionFileType};
    use crate::fragment_metadata::support::{make_backfill_data_file, make_fragment};

    fn tagged(action: pb::FragmentAction) -> pb::FragmentMetadataMutation {
        pb::FragmentMetadataMutation {
            action_sequence: 1,
            action: Some(action),
            fragment_count_delta: 0,
            total_rows_delta: 0,
            visible_rows_delta: 0,
        }
    }

    fn child_ref(index: u64, object_size: u64, height: u32) -> pb::FragmentMetadataChild {
        pb::FragmentMetadataChild {
            path: format!("_bt/node/{index}.node"),
            base_id: None,
            min_key: index * 10,
            num_keys: 10,
            height,
            num_children: u32::from(height > 0),
            total_rows: 10,
            object_size,
            materialized_through_action_sequence: 0,
            visible_rows: 10,
        }
    }

    #[test]
    fn internal_logical_bytes_matches_protobuf_encoding() {
        let children = vec![child_ref(0, 1_000, 0), child_ref(1, 2_000, 0)];
        let buffer = vec![
            tagged(action::remove_fragment(3)),
            tagged(action::add_data_file(7, &make_backfill_data_file(7, 0))),
        ];
        let encoded = pb::FragmentMetadataNode {
            children: children.clone(),
            buffer: buffer.clone(),
        }
        .encoded_len() as u64;

        assert_eq!(internal_logical_bytes(&children, &buffer), encoded);
    }

    #[test]
    fn split_internal_uses_parent_encoding_not_child_payload_sizes() {
        let children = (0..8)
            .map(|index| child_ref(index, 1024 * 1024, 0))
            .collect();
        let pieces = split_internal(children, Vec::new(), 1024, 4);

        assert_eq!(pieces.len(), 2);
        assert!(pieces.iter().all(|(children, _)| children.len() == 4));
        assert!(
            pieces
                .iter()
                .all(|(children, buffer)| internal_logical_bytes(children, buffer) <= 1024)
        );
    }

    #[test]
    fn split_internal_repairs_singleton_tail_with_its_gap_messages() {
        let children = (0..7).map(|index| child_ref(index, 1, 0)).collect();
        let buffer = vec![tagged(action::remove_fragment(55))];
        let pieces = split_internal(children, buffer, 1024, 3);
        assert_eq!(
            pieces
                .iter()
                .map(|(children, _)| children.len())
                .collect::<Vec<_>>(),
            vec![3, 2, 2]
        );
        assert!(pieces[1].1.is_empty());
        assert_eq!(action_key(&pieces[2].1[0]), 55);
        assert_eq!(pieces[2].0[0].min_key, 50);
        assert!(
            pieces
                .iter()
                .all(|(children, buffer)| internal_logical_bytes(children, buffer) <= 1024)
        );
    }

    #[test]
    fn hot_internal_node_is_not_an_underflow_merge_candidate() {
        let config = FragmentMetadataTreeConfig::new(1024, 16);
        let mut child = child_ref(0, 512, 1);
        child.num_children = 1;
        assert!(!is_underflow(&child, &config));

        child.object_size = 128;
        assert!(is_underflow(&child, &config));
    }

    #[test]
    fn internal_overflow_checks_encoded_bytes_and_fanout() {
        let config = FragmentMetadataTreeConfig::new(256, 4);
        let four_children: Vec<_> = (0..4).map(|index| child_ref(index, 1_000_000, 0)).collect();
        assert!(!internal_overflows(&four_children, &[], &config));

        let five_children: Vec<_> = (0..5).map(|index| child_ref(index, 1, 0)).collect();
        assert!(internal_overflows(&five_children, &[], &config));

        let hot_buffer: Vec<_> = (0..20)
            .map(|fragment_id| tagged(action::remove_fragment(fragment_id)))
            .collect();
        assert!(internal_overflows(&four_children, &hot_buffer, &config));
    }

    #[test]
    fn semantic_pressure_is_not_structural_overflow() {
        let config = FragmentMetadataTreeConfig::new(4096, 4).with_semantic_buffer_bytes(64);
        let children: Vec<_> = (0..4).map(|index| child_ref(index, 1024, 0)).collect();
        let pending: Vec<_> = (0..20)
            .map(|fragment_id| tagged(action::remove_fragment(fragment_id)))
            .collect();
        assert!(buffer_pressured(&pending, &config));
        assert!(!internal_overflows(&children, &pending, &config));
    }

    #[test]
    fn clear_deletion_file_action() {
        let mut fragment = make_fragment(7);
        fragment.deletion_file = Some(DeletionFile {
            read_version: 3,
            id: 11,
            file_type: DeletionFileType::Bitmap,
            num_deleted_rows: Some(1),
            base_id: None,
        });
        let mut fragments = BTreeMap::from([(fragment.id, fragment)]);

        apply_actions(&mut fragments, vec![tagged(action::clear_deletion_file(7))]).unwrap();

        assert_eq!(fragments[&7].deletion_file, None);
    }

    #[test]
    fn replace_data_file_swaps_the_named_file_only() {
        use crate::fragment_metadata::support::make_replacement_data_file;

        let fragment = make_fragment(7);
        let expected_path = fragment.files[0].path.clone();
        let mut fragments = BTreeMap::from([(fragment.id, fragment)]);
        let replacement = make_replacement_data_file(7, 0);
        apply_actions(
            &mut fragments,
            vec![tagged(action::replace_data_file(
                7,
                &expected_path,
                &replacement,
            ))],
        )
        .unwrap();
        assert_eq!(fragments[&7].files.len(), 1);
        assert_eq!(fragments[&7].files[0].path, replacement.path);

        // The named file is gone: a storage invariant violation, not a user error.
        let error = apply_actions(
            &mut fragments,
            vec![tagged(action::replace_data_file(
                7,
                &expected_path,
                &replacement,
            ))],
        )
        .unwrap_err();
        assert!(matches!(error, Error::Internal { .. }), "{error}");
    }

    #[test]
    fn mutation_actions_reject_missing_fragment() {
        let deletion_file = pb::DeletionFile {
            read_version: 3,
            id: 11,
            file_type: pb::deletion_file::DeletionFileType::Bitmap.into(),
            num_deleted_rows: 1,
            base_id: None,
        };
        let cases = [
            (
                "AddDataFile",
                action::add_data_file(7, &make_backfill_data_file(7, 0)),
            ),
            (
                "RemoveDataFile",
                action::remove_data_file(7, "missing.lance"),
            ),
            (
                "AddDeletionFile",
                pb::FragmentAction {
                    action: Some(Action::AddDeletionFile(pb::AddDeletionFile {
                        frag_id: 7,
                        deletion_file: Some(deletion_file),
                    })),
                },
            ),
        ];

        for (action_name, action) in cases {
            let error = apply_actions(&mut BTreeMap::new(), vec![tagged(action)]).unwrap_err();
            assert!(matches!(error, Error::InvalidInput { .. }));
            let message = error.to_string();
            assert!(message.contains(action_name), "{message}");
            assert!(message.contains("frag_id=7"), "{message}");
        }
    }
}
