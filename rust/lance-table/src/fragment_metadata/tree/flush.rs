// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Bε drain policy: which children a pressured buffer drains, and how many at once.
//!
//! Qualified for root-to-leaf scatter. An interior child's drain recurses, so
//! its object size understates the work below it. Deep tree amortization is a
//! separate qualification behind the deep-writer gate.

use crate::format::pb;
use crate::fragment_metadata::node::{self, FragmentMetadataTreeConfig};

// Drain at most eight children and 32 MiB of encoded child-object bytes
// per batch. Decoded fragments and replacement buffers can use more memory.
const MAX_CONCURRENT_LEAF_DRAINS: usize = 8;
const MAX_BATCH_OBJECT_BYTES: u64 = 32 * 1024 * 1024;

/// Soft-pressure target. Once pending edits reach this fraction of the
/// child object's encoded size, further batching has diminishing value against
/// the read and write of that object.
const REWRITE_AMORTIZATION_DIVISOR: u64 = 16;

/// Pending bytes a child must hold before draining it under semantic pressure.
///
/// The fair share of the node's remaining routing room guarantees progress by
/// pigeonhole no later than structural pressure. The object fraction keeps a
/// narrow directory from waiting on a share larger than a rewrite deserves.
pub(super) fn amortization_gate(fair_share: u64, child: &pb::FragmentMetadataChild) -> u64 {
    fair_share
        .min(child.object_size / REWRITE_AMORTIZATION_DIVISOR)
        .max(1)
}

pub(super) fn fair_share(
    children: &[pb::FragmentMetadataChild],
    config: &FragmentMetadataTreeConfig,
) -> u64 {
    let routing = node::internal_logical_bytes(children, &[]);
    config.split_ceiling().saturating_sub(routing) / children.len().max(1) as u64
}

/// Children worth draining now, fullest first, bounded for one batch.
///
/// A byte overflow always leaves one bucket at or above the fair share by
/// pigeonhole, so structural pressure needs no separate path here. When the
/// result is empty only routing or fanout overflows, which the caller splits.
pub(super) fn select_children(
    children: &[pb::FragmentMetadataChild],
    buckets: &[Vec<pb::FragmentMetadataMutation>],
    config: &FragmentMetadataTreeConfig,
    io_parallelism: usize,
) -> Vec<usize> {
    let fair_share = fair_share(children, config);
    let mut ranked: Vec<_> = buckets
        .iter()
        .enumerate()
        .map(|(idx, actions)| (node::internal_logical_bytes(&[], actions), idx))
        .filter(|(bytes, _)| *bytes > 0)
        .collect();
    ranked.sort_unstable_by(|a, b| b.cmp(a));
    let worthwhile = ranked
        .iter()
        .filter(|(bytes, idx)| *bytes >= amortization_gate(fair_share, &children[*idx]))
        .map(|(_, idx)| *idx);
    let limit = io_parallelism.clamp(1, MAX_CONCURRENT_LEAF_DRAINS);
    let mut selected = Vec::with_capacity(limit);
    let mut batch_bytes = 0;
    for idx in worthwhile {
        if !selected.is_empty()
            && (children[idx].height > 0
                || selected.len() >= limit
                || batch_bytes + children[idx].object_size > MAX_BATCH_OBJECT_BYTES)
        {
            break;
        }
        selected.push(idx);
        batch_bytes += children[idx].object_size;
        // Interior drains recurse, so running them together would multiply the bound.
        if children[idx].height > 0 {
            break;
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment_metadata::action;
    use rstest::rstest;

    fn children(count: usize, height: u32, object_size: u64) -> Vec<pb::FragmentMetadataChild> {
        vec![
            pb::FragmentMetadataChild {
                height,
                object_size,
                ..Default::default()
            };
            count
        ]
    }

    fn bucket(id: u64, actions: usize) -> Vec<pb::FragmentMetadataMutation> {
        vec![
            pb::FragmentMetadataMutation {
                action: Some(action::remove_fragment(id)),
                ..Default::default()
            };
            actions
        ]
    }

    #[rstest]
    #[case::below_the_gate_waits(1, vec![])]
    #[case::worthwhile_batches_drain_together(64, vec![9, 8, 7, 6, 5, 4, 3, 2])]
    #[test]
    fn selection_respects_the_gate_and_concurrency(
        #[case] actions_per_child: usize,
        #[case] expected: Vec<usize>,
    ) {
        let children = children(10, 0, 1024 * 1024);
        let config = FragmentMetadataTreeConfig::default();
        let buckets: Vec<_> = (0..10)
            .map(|id| bucket(id, actions_per_child * (id as usize + 1) * 200))
            .collect();
        assert_eq!(select_children(&children, &buckets, &config, 8), expected);
    }

    #[test]
    fn fair_share_binds_a_wide_directory() {
        let children = children(256, 0, 1024 * 1024);
        let config = FragmentMetadataTreeConfig::default();
        let share = fair_share(&children, &config);
        assert!(share < 1024 * 1024 / REWRITE_AMORTIZATION_DIVISOR);
        assert_eq!(amortization_gate(share, &children[0]), share);
        let narrow = children[..2].to_vec();
        let share = fair_share(&narrow, &config);
        assert_eq!(
            amortization_gate(share, &narrow[0]),
            1024 * 1024 / REWRITE_AMORTIZATION_DIVISOR
        );
    }

    #[test]
    fn interior_children_drain_alone_and_leaves_respect_the_store_limit() {
        let config = FragmentMetadataTreeConfig::default();
        let mut mixed = children(4, 0, 4096);
        mixed[3].height = 1;
        let buckets: Vec<_> = (0..4).map(|id| bucket(id, 4000)).collect();
        assert_eq!(select_children(&mixed, &buckets, &config, 8), vec![3]);
        let leaves = children(4, 0, 4096);
        assert_eq!(select_children(&leaves, &buckets, &config, 2), vec![3, 2]);
        assert!(select_children(&leaves, &vec![vec![]; 4], &config, 8).is_empty());
    }

    #[test]
    fn selection_bounds_encoded_bytes_in_flight() {
        let oversized = children(3, 0, MAX_BATCH_OBJECT_BYTES / 2 + 1);
        let config = FragmentMetadataTreeConfig::default().with_hard_capacity_bytes(u64::MAX / 4);
        let buckets: Vec<_> = (0..3).map(|id| bucket(id, 200_000)).collect();
        assert_eq!(select_children(&oversized, &buckets, &config, 8), vec![2]);
    }
}
