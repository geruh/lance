// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Compile authoritative Lance transaction results into fragment storage changes.

use std::collections::BTreeSet;
use std::sync::Arc;

use lance_core::{Error, Result};
use lance_table::format::{Fragment, IndexMetadata, Manifest};
use lance_table::fragment_metadata::{
    FragmentMetadataTree, TouchedFragments, ValidatedCommit, action,
};

use super::super::transaction::{Operation, Transaction, validate_operation};
use super::super::{Dataset, ManifestWriteConfig};
use crate::index::load_all_indices;
use crate::io::commit::{
    check_column_indices, check_storage_version, fix_schema, migrate_indices, migrate_manifest,
};
use lance_table::system_index::mem_wal::MEM_WAL_INDEX_NAME;
use lance_table::transaction::ReadVersionState;

pub(super) struct PreparedOperation {
    pub manifest: Manifest,
    pub indices: Vec<IndexMetadata>,
    pub touched: TouchedFragments,
    pub changes: ValidatedCommit,
    fragments: Arc<Vec<Fragment>>,
    complete: bool,
}

impl PreparedOperation {
    pub fn materialized_fragments(&self, dataset: &Dataset) -> Option<Vec<Fragment>> {
        if self.complete {
            let mut fragments = self.fragments.as_ref().clone();
            fragments.sort_unstable_by_key(|fragment| fragment.id);
            return Some(fragments);
        }
        if dataset.lazy_fragments.is_some() {
            return None;
        }
        let mut replacements = self.fragments.as_ref().clone();
        replacements.sort_unstable_by_key(|fragment| fragment.id);
        let mut replacements = replacements.into_iter().peekable();
        let mut fragments = Vec::with_capacity(dataset.manifest.fragments.len());
        for old in dataset.manifest.fragments.iter() {
            while let Some(fragment) = replacements.next_if(|fragment| fragment.id < old.id) {
                fragments.push(fragment);
            }
            if let Some(fragment) = replacements.next_if(|fragment| fragment.id == old.id) {
                fragments.push(fragment);
            } else if !self.touched.ids.contains(&old.id) {
                fragments.push(old.clone());
            }
        }
        fragments.extend(replacements);
        Some(fragments)
    }
}

/// A partial manifest is an attempt-local input to the ordinary builder, never
/// a snapshot. Global transformations require the complete list. Index retention
/// needs only evidence that a generation still covers a live, untouched fragment.
pub(super) async fn prepare(
    dataset: &Dataset,
    tree: &mut FragmentMetadataTree,
    transaction: &Transaction,
    original_read_version: u64,
    config: &ManifestWriteConfig,
    bulk: bool,
) -> Result<PreparedOperation> {
    let mut indices = load_all_indices(dataset).await?.as_ref().clone();
    // Coverage is proved against the transaction's original read version. A
    // sparse manifest cannot prove that an index covers every live fragment.
    let read_state = if indices.iter().any(|index| index.name == MEM_WAL_INDEX_NAME) {
        let mut read_dataset = if original_read_version == 0 {
            dataset.clone()
        } else {
            dataset.checkout_version(original_read_version).await?
        };
        read_dataset.hydrate_fragments_for_maintenance().await?;
        let read_indices = load_all_indices(&read_dataset).await?;
        Some((read_dataset, read_indices))
    } else {
        None
    };
    validate_allocation(tree.next_fragment_id(), &transaction.operation)?;
    let selected_ids: Option<Vec<u64>> = match &transaction.operation {
        Operation::Append { fragments } => Some(
            fragments
                .iter()
                .filter(|fragment| fragment.id != 0)
                .map(|fragment| fragment.id)
                .collect(),
        ),
        Operation::ReserveFragments { .. } => Some(Vec::new()),
        Operation::DataReplacement { replacements } => {
            Some(replacements.iter().map(|group| group.0).collect())
        }
        Operation::Delete {
            updated_fragments,
            deleted_fragment_ids,
            ..
        } => Some(
            updated_fragments
                .iter()
                .map(|fragment| fragment.id)
                .chain(deleted_fragment_ids.iter().copied())
                .collect(),
        ),
        Operation::Update {
            updated_fragments,
            removed_fragment_ids,
            new_fragments,
            ..
        } => Some(
            updated_fragments
                .iter()
                .map(|fragment| fragment.id)
                .chain(removed_fragment_ids.iter().copied())
                .chain(
                    new_fragments
                        .iter()
                        .filter(|fragment| fragment.id != 0)
                        .map(|fragment| fragment.id),
                )
                .collect(),
        ),
        Operation::Rewrite { groups, .. } => Some(
            groups
                .iter()
                .flat_map(|group| {
                    group
                        .old_fragments
                        .iter()
                        .chain(
                            group
                                .new_fragments
                                .iter()
                                .filter(|fragment| fragment.id != 0),
                        )
                        .map(|fragment| fragment.id)
                })
                .collect(),
        ),
        Operation::DataOverlay { groups } => {
            Some(groups.iter().map(|group| group.fragment_id).collect())
        }
        Operation::Clone { .. } => {
            return Err(Error::not_supported_source(
                format!(
                    "fragment metadata tree cannot publish {} through this commit path",
                    transaction.operation.name()
                )
                .into(),
            ));
        }
        _ => None,
    };
    let complete = selected_ids.is_none();
    let mut touched = if complete {
        if bulk {
            tree.resolve_touched_for_bulk(&[]).await?;
        }
        let fragments = if dataset.lazy_fragments.is_none() {
            dataset.manifest.fragments.as_ref().clone()
        } else {
            tree.materialize().await?
        };
        TouchedFragments {
            ids: fragments.iter().map(|fragment| fragment.id).collect(),
            fragments: fragments
                .into_iter()
                .map(|fragment| (fragment.id, fragment))
                .collect(),
        }
    } else {
        let ids = selected_ids.unwrap_or_default();
        resolve_touched(dataset, tree, &ids, bulk).await?
    };
    if !complete
        && matches!(
            transaction.operation,
            Operation::Delete { .. } | Operation::Update { .. }
        )
    {
        // Native retention distinguishes empty/nonempty index generations. One
        // unchanged live record per bitmap preserves that decision without
        // enumerating all fragment metadata on every indexed mutation. A fully
        // stale bitmap may require checking all its candidates to prove absence.
        for index in &indices {
            let Some(bitmap) = &index.fragment_bitmap else {
                continue;
            };
            if touched
                .fragments
                .keys()
                .any(|id| !touched.ids.contains(id) && bitmap.contains(*id as u32))
            {
                continue;
            }
            let mut candidates = bitmap
                .iter()
                .map(u64::from)
                .filter(|id| !touched.ids.contains(id));
            let mut batch_size = 1;
            loop {
                let ids: Vec<_> = candidates.by_ref().take(batch_size).collect();
                batch_size = 64;
                if ids.is_empty() {
                    break;
                }
                let resolved = resolve_touched(dataset, tree, &ids, bulk).await?;
                if let Some((id, fragment)) = resolved.fragments.into_iter().next() {
                    touched.fragments.insert(id, fragment);
                    break;
                }
            }
        }
        touched.ids.extend(touched.fragments.keys().copied());
    }
    let mut current = dataset.manifest.as_ref().clone();
    current.set_fragments(touched.fragments.values().cloned().collect());
    validate_operation(Some(&current), &transaction.operation)?;
    let (mut manifest, prepared_indices) =
        if let Operation::Restore { version } = transaction.operation {
            let (mut restored, indices) = Transaction::restore_old_manifest(
                &dataset.object_store,
                dataset.commit_handler.as_ref(),
                &dataset.base,
                version,
                &config.to_build_config(),
                "",
                &current,
            )
            .await?;
            if restored.fragment_metadata.is_some() {
                restored.fragments = Arc::new(
                    super::tree_from_manifest(
                        dataset.object_store.clone(),
                        dataset.base.clone(),
                        &restored,
                    )
                    .await?
                    .materialize()
                    .await?,
                );
            }
            restored.version = current.version + 1;
            (restored, indices)
        } else {
            transaction.build_manifest_with_read_version(
                Some(&current),
                indices,
                "",
                &config.to_build_config(),
                read_state
                    .as_ref()
                    .map(|(dataset, indices)| ReadVersionState {
                        manifest: dataset.manifest.as_ref(),
                        indices: indices.as_slice(),
                    }),
            )?
        };
    indices = prepared_indices;
    super::tree_config_from(&manifest.config)?;
    super::publication::policy(&manifest)?;
    // Byte targets are writer policy and may change. The layout and the hard
    // capacity are properties of the objects already written.
    for key in [
        lance_table::fragment_metadata::MANIFEST_LAYOUT_KEY,
        "lance.fragment_metadata.hard_capacity_bytes",
    ] {
        if manifest.config.get(key) != current.config.get(key) {
            return Err(Error::invalid_input(format!(
                "fragment metadata setting {key} cannot change from {:?} to {:?}; it is fixed when the metadata tree is created, so rebuild the metadata tree to change it",
                current.config.get(key),
                manifest.config.get(key)
            )));
        }
    }

    // Untouched lazy Fragments may still require these reader capabilities.
    // The ordinary builder sees only the selected state in the sparse case.
    if !complete {
        manifest.reader_feature_flags |= current.reader_feature_flags;
        manifest.writer_feature_flags |= current.writer_feature_flags;
    }
    migrate_manifest(dataset, &mut manifest, current.writer_version.is_none()).await?;
    fix_schema(&mut manifest)?;
    check_storage_version(&mut manifest)?;
    check_column_indices(&manifest)?;
    let recovered_coverage = migrate_indices(dataset, &mut indices).await?;
    Transaction::withdraw_coverage_invalidated_after_build(
        &mut indices,
        &recovered_coverage,
        manifest.version,
    )?;

    let final_ids: BTreeSet<_> = manifest
        .fragments
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    if complete {
        // A complete enumeration also resolves absence, including previously
        // reserved IDs that an authoritative Rewrite or Merge now introduces.
        touched.ids.extend(final_ids.iter().copied());
    }
    let mut actions: Vec<_> = touched
        .fragments
        .keys()
        .filter(|id| !final_ids.contains(id))
        .copied()
        .map(action::remove_fragment)
        .collect();
    actions.extend(
        manifest
            .fragments
            .iter()
            .filter(|fragment| touched.get(fragment.id) != Some(*fragment))
            .map(action::add_fragment),
    );
    // Preserve compact location changes when they exactly produce the native
    // result. The reducer is never responsible for validating a transaction.
    if let Operation::DataReplacement { replacements } = &transaction.operation
        && let Ok(compact) = super::replacement_actions(replacements, &touched)
    {
        let mut result = touched.fragments.clone();
        let messages = compact
            .iter()
            .enumerate()
            .map(
                |(action_sequence, action)| lance_table::format::pb::FragmentMetadataMutation {
                    action_sequence: action_sequence as u64 + 1,
                    action: Some(action.clone()),
                    ..Default::default()
                },
            )
            .collect();
        if lance_table::fragment_metadata::node::apply_actions(&mut result, messages).is_ok()
            && result.values().eq(manifest.fragments.iter())
        {
            actions = compact;
        }
    }
    let fragments = std::mem::replace(&mut manifest.fragments, Arc::new(Vec::new()));
    let next_fragment_id = super::publication::next_fragment_id(&manifest);
    Ok(PreparedOperation {
        manifest,
        indices,
        touched,
        fragments,
        complete,
        changes: ValidatedCommit {
            fragment_actions: actions,
            next_fragment_id: Some(next_fragment_id),
        },
    })
}

async fn resolve_touched(
    dataset: &Dataset,
    tree: &mut FragmentMetadataTree,
    ids: &[u64],
    bulk: bool,
) -> Result<TouchedFragments> {
    if dataset.lazy_fragments.is_none() {
        let ids: BTreeSet<u64> = ids.iter().copied().collect();
        let fragments = ids
            .iter()
            .filter_map(|id| {
                dataset
                    .manifest
                    .fragments
                    .binary_search_by_key(id, |fragment| fragment.id)
                    .ok()
                    .map(|index| (*id, dataset.manifest.fragments[index].clone()))
            })
            .collect();
        Ok(TouchedFragments { ids, fragments })
    } else if bulk {
        tree.resolve_touched_for_bulk(ids).await
    } else {
        tree.resolve_touched(ids).await
    }
}

fn validate_allocation(next_id: u64, operation: &Operation) -> Result<()> {
    let new_fragments: Vec<_> = match operation {
        Operation::Append { fragments } | Operation::Overwrite { fragments, .. } => {
            fragments.iter().collect()
        }
        Operation::Update { new_fragments, .. } => new_fragments.iter().collect(),
        Operation::Rewrite { groups, .. } => groups
            .iter()
            .flat_map(|group| group.new_fragments.iter())
            .collect(),
        Operation::ReserveFragments { num_fragments } => {
            if next_id
                .checked_add(u64::from(*num_fragments))
                .is_none_or(|end| end > u64::from(u32::MAX) + 1)
            {
                return Err(Error::invalid_input(
                    "Fragment ID allocation exceeds Lance's u32 address space",
                ));
            }
            return Ok(());
        }
        _ => return Ok(()),
    };
    let start = next_id;
    let allocated = if matches!(operation, Operation::Overwrite { .. }) {
        new_fragments.len()
    } else {
        new_fragments
            .iter()
            .filter(|fragment| fragment.id == 0)
            .count()
    } as u64;
    if (!matches!(operation, Operation::Overwrite { .. })
        && new_fragments
            .iter()
            .any(|fragment| fragment.id > u64::from(u32::MAX)))
        || start
            .checked_add(allocated)
            .is_none_or(|end| end > u64::from(u32::MAX) + 1)
    {
        return Err(Error::invalid_input(
            "Fragment ID allocation exceeds Lance's u32 address space",
        ));
    }
    Ok(())
}
