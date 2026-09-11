// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Ordered bulk materialization at any height. Keep one pending leaf across
//! routing boundaries so split/coalesce is decided before writing final leaves.

use std::collections::BTreeMap;

use lance_core::{Error, Result};

use super::node::{self, FragmentMetadataTreeConfig};
use super::store::NodeStore;
use crate::format::{Fragment, pb};

#[derive(Default)]
pub(super) struct BulkResult {
    pub children: Vec<pb::FragmentMetadataChild>,
    pub io_bytes: u64,
    pub flushes: u64,
    pub splits: u64,
    pub merges: u64,
    pub materialized: u64,
    pub max_flush_depth: u32,
}

enum PendingLeaf {
    Unchanged(pb::FragmentMetadataChild),
    Changed(Vec<Fragment>),
}

impl PendingLeaf {
    async fn bytes(&self, store: &NodeStore) -> Result<u64> {
        Ok(match self {
            Self::Unchanged(child) => child.object_size,
            Self::Changed(fragments) => store.encode_leaf(fragments).await?.len() as u64,
        })
    }

    async fn load(self, store: &NodeStore) -> Result<Vec<Fragment>> {
        match self {
            Self::Unchanged(child) => store.read_leaf(&child).await,
            Self::Changed(fragments) => Ok(fragments),
        }
    }

    async fn write(
        self,
        store: &NodeStore,
        config: &FragmentMetadataTreeConfig,
        watermark: u64,
        output: &mut BulkResult,
    ) -> Result<()> {
        match self {
            Self::Unchanged(child) => output.children.push(child),
            Self::Changed(fragments) => {
                let written = store.write_leaves(&fragments, watermark, config).await?;
                output.splits += written.len().saturating_sub(1) as u64;
                for written in written {
                    output.io_bytes += written.io_bytes;
                    output.children.push(written.child_ref);
                }
            }
        }
        Ok(())
    }
}

/// Visit old routing in order, carrying each ancestor's actions to its owning
/// leaf. Defer the last output leaf across parent boundaries so a later sparse
/// sibling cannot force a read/rewrite of an already emitted replacement.
/// Unchanged leaves are reused unless coalescing needs them. The caller builds
/// final routing over the returned leaf references, without intermediate nodes.
pub(super) async fn materialize(
    store: &NodeStore,
    config: &FragmentMetadataTreeConfig,
    children: Vec<pb::FragmentMetadataChild>,
    buffer: Vec<pb::FragmentMetadataMutation>,
    watermark: u64,
) -> Result<BulkResult> {
    let mut output = BulkResult::default();
    let mut pending: Option<PendingLeaf> = None;
    if children.is_empty() {
        let mut fragments = BTreeMap::new();
        output.materialized = buffer.len() as u64;
        store.apply_verified(&mut fragments, buffer)?;
        if !fragments.is_empty() {
            PendingLeaf::Changed(fragments.into_values().collect())
                .write(store, config, watermark, &mut output)
                .await?;
        }
        return Ok(output);
    }
    let buckets = node::partition_buffer_by_child(&children, buffer);
    node::validate_routed(&children, &buckets, node::ROOT_EXCLUSIVE_END)?;
    let mut stack: Vec<_> = children
        .into_iter()
        .zip(buckets)
        .map(|(child, actions)| (child, actions, 0))
        .rev()
        .collect();
    while let Some((child, actions, depth)) = stack.pop() {
        if child.height > 0 {
            let mut internal = store.read_internal(&child).await?;
            if internal.children.is_empty() {
                return Err(Error::invalid_input(format!(
                    "fragment metadata bulk encountered childless interior {} at height {}",
                    child.path, child.height
                )));
            }
            internal.buffer.extend(actions);
            let buckets = node::partition_buffer_by_child(&internal.children, internal.buffer);
            node::validate_routed(&internal.children, &buckets, node::ROOT_EXCLUSIVE_END)?;
            stack.extend(
                internal
                    .children
                    .into_iter()
                    .zip(buckets)
                    .map(|(child, actions)| (child, actions, depth + 1))
                    .rev(),
            );
            continue;
        }
        let current = if actions.is_empty() {
            PendingLeaf::Unchanged(child)
        } else {
            output.flushes += 1;
            output.max_flush_depth = output.max_flush_depth.max(depth);
            output.materialized += actions.len() as u64;
            let mut fragments: BTreeMap<_, _> = store
                .read_leaf(&child)
                .await?
                .into_iter()
                .map(|f| (f.id, f))
                .collect();
            store.apply_verified(&mut fragments, actions)?;
            if fragments.is_empty() {
                continue;
            }
            PendingLeaf::Changed(fragments.into_values().collect())
        };
        if let Some(previous) = pending.take() {
            let previous_bytes = previous.bytes(store).await?;
            let current_bytes = current.bytes(store).await?;
            let small = previous_bytes <= config.leaf_merge_floor()
                || current_bytes <= config.leaf_merge_floor();
            let fits = previous_bytes
                .checked_add(current_bytes)
                .is_some_and(|bytes| bytes <= config.leaf_coalesce_ceiling());
            if small && fits {
                let mut fragments = previous.load(store).await?;
                fragments.extend(current.load(store).await?);
                pending = Some(PendingLeaf::Changed(fragments));
                output.merges += 1;
                continue;
            }
            previous
                .write(store, config, watermark, &mut output)
                .await?;
        }
        pending = Some(current);
    }
    if let Some(leaf) = pending {
        leaf.write(store, config, watermark, &mut output).await?;
    }
    Ok(output)
}
