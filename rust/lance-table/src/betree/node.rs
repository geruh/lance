// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! PROTOTYPE (discussion #7499): in-memory Bε-tree nodes and pure node logic.
//!
//! Two node kinds: [`InternalNode`] (child pivots + a message ε-buffer) and
//! [`LeafNode`] (the fragment table). All sizing/split/merge thresholds are in
//! **serialized bytes** because actions and fragments are variable-sized. No IO
//! here — see `store.rs` (node files) and `tree.rs` (algorithms).

use std::collections::BTreeMap;

use prost::Message;

use crate::betree::action;
use crate::format::Fragment;
use crate::format::pb::{self, fragment_action::Action};
use lance_core::{Error, Result};

pub const DEFAULT_MAX_NODE_BYTES: u64 = 10 * 1024 * 1024;
pub const DEFAULT_MAX_CHILDREN_PER_NODE: u32 = 16;

/// Bε-tree tuning knobs. There are exactly two independent knobs:
///
/// * `max_node_bytes` — the node-size limit (call it `B`): a node splits when its
///   logical content exceeds this many bytes.
/// * `max_children_per_node` — the branching factor (`B^ε` in the literature): an
///   internal node splits when it points to more children than this, and merges
///   below a quarter of it.
///
/// Everything else (the flush gate, split-piece size, merge floor) derives from
/// those two. Defaults follow the literature: ε=1/2, ~10 MiB nodes,
/// `max_children_per_node` = 16, split at `B` into ~0.5·B pieces, merge ≤ 0.25·B.
#[derive(Debug, Clone)]
pub struct BeTreeConfig {
    /// Node-size limit `B`: a node splits when its logical bytes exceed this.
    pub max_node_bytes: u64,
    /// Branching factor: the max child pointers an internal node may hold before
    /// it splits (it merges below `max_children_per_node / 4`).
    pub max_children_per_node: u32,
    /// Optional override of the flush gate. `None` (the norm) derives it as
    /// `max_node_bytes / max_children_per_node` — see [`Self::min_flush_bytes`].
    pub min_flush_override: Option<u64>,
}

impl Default for BeTreeConfig {
    fn default() -> Self {
        Self {
            max_node_bytes: DEFAULT_MAX_NODE_BYTES,
            max_children_per_node: DEFAULT_MAX_CHILDREN_PER_NODE,
            min_flush_override: None,
        }
    }
}

impl BeTreeConfig {
    /// Build a config from the two primary knobs; the flush gate derives as
    /// `max_node_bytes / max_children_per_node`.
    pub fn new(max_node_bytes: u64, max_children_per_node: u32) -> Self {
        Self {
            max_node_bytes,
            max_children_per_node,
            min_flush_override: None,
        }
    }

    /// A node splits when its logical bytes reach this (= `max_node_bytes`).
    pub fn split_ceiling(&self) -> u64 {
        self.max_node_bytes
    }
    /// Split output pieces target ~0.5·`max_node_bytes` (so a just-split node isn't near-full).
    pub fn split_piece_bytes(&self) -> u64 {
        self.max_node_bytes / 2
    }
    /// A node underflows (is a merge candidate) at ≤ 0.25·`max_node_bytes`.
    pub fn merge_floor(&self) -> u64 {
        self.max_node_bytes / 4
    }
    /// Adjacent children are coalesced while their combined bytes stay ≤ 0.6·`max_node_bytes`.
    pub fn coalesce_ceiling(&self) -> u64 {
        self.max_node_bytes * 3 / 5
    }
    /// The amortization gate: never flush a child slice smaller than this.
    ///
    /// Derived as `max_node_bytes / max_children_per_node` — a full ε-buffer split
    /// across all children leaves that much in the fullest, so this is the natural
    /// "fair share" threshold and it scales correctly with the branching factor.
    /// (A hardcoded `B/16` silently assumed 16 children: at a higher branching
    /// factor the buffer spreads thinner than the gate, so flushes never fire and
    /// the tree degrades to split-only.)
    pub fn min_flush_bytes(&self) -> u64 {
        self.min_flush_override
            .unwrap_or_else(|| self.max_node_bytes / self.max_children_per_node.max(1) as u64)
    }
}

/// A leaf: the fragment table, sorted by fragment id.
#[derive(Debug, Clone)]
pub struct LeafNode {
    pub fragments: Vec<Fragment>,
}

/// An internal node: child pivots (sorted by `min_key`, contiguous) + the
/// ε-buffer. The buffer is kept in msn (insertion) order in memory and sorted by
/// `(key, msn)` only when grouping for a flush.
#[derive(Debug, Clone, Default)]
pub struct InternalNode {
    pub children: Vec<pb::ChildRef>,
    pub buffer: Vec<pb::TaggedAction>,
}

/// Logical (uncompressed) byte size of a single fragment — the split/merge unit.
pub fn fragment_logical_bytes(fragment: &Fragment) -> u64 {
    pb::DataFragment::from(fragment).encoded_len() as u64
}

/// Logical (uncompressed) byte size of a leaf — the split/merge metric.
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
pub fn internal_logical_bytes(children: &[pb::ChildRef], buffer: &[pb::TaggedAction]) -> u64 {
    children.iter().map(repeated_message_bytes).sum::<u64>()
        + buffer.iter().map(repeated_message_bytes).sum::<u64>()
}

/// Whether an internal node violates its encoded-byte or fanout limit.
pub fn internal_overflows(
    children: &[pb::ChildRef],
    buffer: &[pb::TaggedAction],
    config: &BeTreeConfig,
) -> bool {
    children.len() as u32 > config.max_children_per_node
        || internal_logical_bytes(children, buffer) >= config.split_ceiling()
}

/// The target key of a buffered action (the fragment id it mutates).
pub fn action_key(t: &pb::TaggedAction) -> u64 {
    t.action
        .as_ref()
        .and_then(action::target_frag_id)
        .unwrap_or(0)
}

/// Index of the child owning `key`. `children` are sorted by `min_key` with
/// contiguous ranges, so this is the rightmost child with `min_key <= key`.
pub fn child_index_for(children: &[pb::ChildRef], key: u64) -> usize {
    match children.binary_search_by(|c| c.min_key.cmp(&key)) {
        Ok(i) => i,
        Err(0) => 0,
        Err(i) => i - 1,
    }
}

/// Build a `ChildRef` for a leaf that has just been written.
pub fn leaf_ref(
    node_path: String,
    fragments: &[Fragment],
    byte_size: u64,
    object_size: u64,
) -> Result<pb::ChildRef> {
    let total_rows = sum_aggregate_values(
        fragments
            .iter()
            .map(|fragment| fragment.physical_rows.unwrap_or(0) as u64),
        "total_rows",
    )?;
    Ok(pb::ChildRef {
        node_path,
        min_key: fragments.first().map(|f| f.id).unwrap_or(0),
        max_key: fragments.last().map(|f| f.id).unwrap_or(0),
        num_keys: fragments.len() as u64,
        byte_size,
        height: 0,
        num_children: 0,
        total_rows,
        object_size,
    })
}

/// Build a `ChildRef` for an internal node that has just been written.
pub fn internal_ref(
    node_path: String,
    children: &[pb::ChildRef],
    buffer: &[pb::TaggedAction],
    byte_size: u64,
    object_size: u64,
) -> Result<pb::ChildRef> {
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
    let height = children.iter().map(|c| c.height).max().unwrap_or(0) + 1;
    Ok(pb::ChildRef {
        node_path,
        min_key: children.first().map(|c| c.min_key).unwrap_or(0),
        max_key: children.last().map(|c| c.max_key).unwrap_or(0),
        num_keys,
        byte_size,
        height,
        num_children: children.len() as u32,
        total_rows,
        object_size,
    })
}

fn apply_aggregate_delta(base: u64, delta: i64, name: &str) -> Result<u64> {
    if delta >= 0 {
        base.checked_add(delta as u64).ok_or_else(|| {
            Error::invalid_input(format!(
                "Bε-tree {name} aggregate overflow: base={base}, delta={delta}"
            ))
        })
    } else {
        base.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
            Error::invalid_input(format!(
                "Bε-tree {name} aggregate underflow: base={base}, delta={delta}"
            ))
        })
    }
}

fn sum_aggregate_deltas(deltas: impl IntoIterator<Item = i64>, name: &str) -> Result<i64> {
    deltas.into_iter().try_fold(0i64, |sum, delta| {
        sum.checked_add(delta).ok_or_else(|| {
            Error::invalid_input(format!(
                "Bε-tree {name} overflow while summing: sum={sum}, delta={delta}"
            ))
        })
    })
}

fn sum_aggregate_values(values: impl IntoIterator<Item = u64>, name: &str) -> Result<u64> {
    values.into_iter().try_fold(0u64, |sum, value| {
        sum.checked_add(value).ok_or_else(|| {
            Error::invalid_input(format!(
                "Bε-tree {name} overflow while summing: sum={sum}, value={value}"
            ))
        })
    })
}

/// Is this child underflowing (a merge candidate)? Leaves underflow by bytes.
/// Internal nodes must be sparse by both direct-child count and their exact
/// encoded size so a hot ε-buffer is never merged as "small."
pub fn is_underflow(child: &pb::ChildRef, config: &BeTreeConfig) -> bool {
    if child.height == 0 {
        child.byte_size <= config.merge_floor()
    } else {
        child.num_children < (config.max_children_per_node / 4).max(1)
            && child.byte_size <= config.merge_floor()
    }
}

/// Apply buffered actions to an id-keyed fragment map, in `(key, msn)` order
/// (newest-wins). Used at leaves and when materializing.
pub fn apply_actions(
    frags: &mut BTreeMap<u64, Fragment>,
    mut actions: Vec<pb::TaggedAction>,
) -> Result<()> {
    actions.sort_by_key(|t| (action_key(t), t.msn));
    for tagged in actions {
        if let Some(action) = tagged.action {
            apply_one(frags, action)?;
        }
    }
    Ok(())
}

fn apply_one(frags: &mut BTreeMap<u64, Fragment>, action: pb::FragmentAction) -> Result<()> {
    let Some(action) = action.action else {
        return Ok(());
    };
    match action {
        Action::AddFragment(f) => {
            let fragment = Fragment::try_from(f)?;
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
            fragment.deletion_file = Some(crate::format::DeletionFile::try_from(deletion_file)?);
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

/// Partition a buffer into one bucket per child (by owning child index). The
/// returned vec has `children.len()` buckets; the buffer is consumed.
pub fn partition_buffer_by_child(
    children: &[pb::ChildRef],
    buffer: Vec<pb::TaggedAction>,
) -> Vec<Vec<pb::TaggedAction>> {
    let mut buckets: Vec<Vec<pb::TaggedAction>> = vec![Vec::new(); children.len()];
    for tagged in buffer {
        let idx = child_index_for(children, action_key(&tagged));
        buckets[idx].push(tagged);
    }
    buckets
}

/// Sum of encoded bytes of a set of buffered actions.
pub fn buffer_bytes(buffer: &[pb::TaggedAction]) -> u64 {
    buffer.iter().map(|t| t.encoded_len() as u64).sum()
}

/// Split a sorted fragment list into `⌈total/piece_bytes⌉` contiguous pieces of
/// roughly equal bytes (~0.5x-max_node_bytes each — no tiny tail, so no split-then-merge churn).
pub fn split_leaf_fragments(fragments: Vec<Fragment>, piece_bytes: u64) -> Vec<Vec<Fragment>> {
    let total = leaf_logical_bytes(&fragments);
    let num_pieces = total.div_ceil(piece_bytes.max(1)).max(1) as usize;
    if num_pieces <= 1 || fragments.len() <= 1 {
        return vec![fragments];
    }
    let target = total / num_pieces as u64;
    let mut pieces = Vec::with_capacity(num_pieces);
    let mut cur: Vec<Fragment> = Vec::new();
    let mut cur_bytes = 0u64;
    for f in fragments {
        let fb = pb::DataFragment::from(&f).encoded_len() as u64;
        cur.push(f);
        cur_bytes += fb;
        if cur_bytes >= target && pieces.len() + 1 < num_pieces {
            pieces.push(std::mem::take(&mut cur));
            cur_bytes = 0;
        }
    }
    if !cur.is_empty() {
        pieces.push(cur);
    }
    pieces
}

/// Split an internal node's (children, buffer) into contiguous pieces targeting
/// `piece_bytes` and at most `max_children_per_node` children each. The buffer
/// follows its child by key range. A single indivisible child-plus-message unit
/// may exceed the byte target, but never the original node's split ceiling.
pub fn split_internal(
    children: Vec<pb::ChildRef>,
    buffer: Vec<pb::TaggedAction>,
    piece_bytes: u64,
    max_children_per_node: u32,
) -> Vec<(Vec<pb::ChildRef>, Vec<pb::TaggedAction>)> {
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
    pieces
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::betree::support::{make_backfill_data_file, make_fragment};
    use crate::format::{DeletionFile, DeletionFileType};

    fn tagged(action: pb::FragmentAction) -> pb::TaggedAction {
        pb::TaggedAction {
            msn: 1,
            action: Some(action),
            fragment_count_delta: 0,
            total_rows_delta: 0,
        }
    }

    fn child_ref(index: u64, byte_size: u64, height: u32) -> pb::ChildRef {
        pb::ChildRef {
            node_path: format!("node-{index}"),
            min_key: index * 10,
            max_key: index * 10 + 9,
            num_keys: 10,
            byte_size,
            height,
            num_children: u32::from(height > 0),
            total_rows: 10,
            object_size: byte_size,
        }
    }

    #[test]
    fn internal_logical_bytes_matches_protobuf_encoding() {
        let children = vec![child_ref(0, 1_000, 0), child_ref(1, 2_000, 0)];
        let buffer = vec![
            tagged(action::remove_fragment(3)),
            tagged(action::add_data_file(7, &make_backfill_data_file(7, 0))),
        ];
        let encoded = pb::InternalNode {
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
    fn hot_internal_node_is_not_an_underflow_merge_candidate() {
        let config = BeTreeConfig::new(1024, 16);
        let mut child = child_ref(0, 512, 1);
        child.num_children = 1;
        assert!(!is_underflow(&child, &config));

        child.byte_size = 128;
        assert!(is_underflow(&child, &config));
    }

    #[test]
    fn internal_overflow_checks_encoded_bytes_and_fanout() {
        let config = BeTreeConfig::new(256, 4);
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
