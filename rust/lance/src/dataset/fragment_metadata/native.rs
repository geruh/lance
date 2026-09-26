// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Compile authoritative Lance transaction results into fragment storage changes.

use std::collections::BTreeSet;
use std::sync::Arc;

use lance_core::{Error, Result};
use lance_file::version::ConcreteFileVersion;
use lance_table::format::{DataFile, Fragment, IndexMetadata, Manifest, pb};
use lance_table::fragment_metadata::{FragmentTree, TouchedFragments, ValidatedCommit, action};

use super::super::transaction::{Operation, Transaction, validate_operation};
use super::super::{Dataset, ManifestWriteConfig};
use crate::index::load_all_indices;
use crate::io::commit::{check_fragment_ids, fix_schema, migrate_indices, migrate_manifest};
use lance_table::system_index::mem_wal::MEM_WAL_INDEX_NAME;
use lance_table::transaction::ReadVersionState;
use prost::Message;

pub(super) struct PreparedOperation {
    pub manifest: Manifest,
    pub indices: Vec<IndexMetadata>,
    pub touched: TouchedFragments,
    pub changes: ValidatedCommit,
    fragments: Arc<Vec<Fragment>>,
    complete: bool,
}

impl PreparedOperation {
    pub fn materialized_fragments(&mut self, dataset: &Dataset) -> Option<Vec<Fragment>> {
        if self.complete {
            let mut fragments = Arc::unwrap_or_clone(std::mem::take(&mut self.fragments));
            fragments.sort_unstable_by_key(|fragment| fragment.id);
            return Some(fragments);
        }
        if dataset.lazy_fragments.is_some() {
            return None;
        }
        let mut replacements = Arc::unwrap_or_clone(std::mem::take(&mut self.fragments));
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
    tree: &mut FragmentTree,
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
    // Legacy storage validation needs the complete file inventory.
    let complete = selected_ids.is_none()
        || dataset.manifest.data_storage_format.lance_file_format() == ConcreteFileVersion::V1;
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
            if restored.fragment_tree.is_some() {
                let mut restored_tree = super::tree_from_manifest(
                    dataset.object_store.clone(),
                    dataset.session.store_registry(),
                    dataset.base.clone(),
                    &restored,
                )
                .await?;
                restored_tree.set_foreign_bases(dataset.tree_foreign_bases_for(&restored).await?);
                restored.fragments = Arc::new(restored_tree.materialize().await?);
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
    super::super::versions::finalize_manifest_storage_version(&mut manifest)?;
    check_fragment_ids(&manifest)?;
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
    let upserts = manifest
        .fragments
        .iter()
        .filter(|fragment| touched.get(fragment.id) != Some(*fragment));
    // Row deletions change only a deletion file, which buffers in a few dozen
    // bytes instead of the whole fragment record.
    let compact = match &transaction.operation {
        Operation::DataReplacement { replacements } => {
            super::replacement_actions(replacements, &touched).ok()
        }
        Operation::Delete { .. } | Operation::Update { .. } => {
            let mut compact = actions.clone();
            compact.extend(
                upserts
                    .clone()
                    .map(|fragment| match touched.get(fragment.id) {
                        Some(previous) if changes_only_deletion_file(previous, fragment) => {
                            match &fragment.deletion_file {
                                Some(file) => action::add_deletion_file(fragment.id, file),
                                None => action::clear_deletion_file(fragment.id),
                            }
                        }
                        _ => action::upsert_fragment(fragment),
                    }),
            );
            Some(compact)
        }
        _ => None,
    };
    // Small backfills can stay buffered. Larger ones keep whole records so
    // a drain can rebuild leaves from their headers.
    let backfill = matches!(transaction.operation, Operation::Merge { .. }).then(|| {
        upserts
            .clone()
            .flat_map(|fragment| backfill_actions(&touched, fragment))
            .collect::<Vec<_>>()
    });
    match backfill {
        Some(backfill)
            if tree.can_buffer(
                backfill
                    .iter()
                    .map(|action| action.encoded_len() as u64)
                    .sum(),
            ) =>
        {
            actions.extend(backfill);
        }
        _ => actions.extend(upserts.map(action::upsert_fragment)),
    }
    // Keep compact actions only when they exactly produce the native result.
    // The reducer is never responsible for validating a transaction.
    if let Some(compact) = compact
        && reproduces(&touched, &compact, &manifest.fragments)
    {
        actions = compact;
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

/// The actions that turn the stored record into `next`: one `AddDataFile`
/// per appended file when nothing else changed, otherwise the whole record.
fn backfill_actions(touched: &TouchedFragments, next: &Fragment) -> Vec<pb::FragmentAction> {
    match touched
        .get(next.id)
        .and_then(|previous| appended_files(previous, next))
    {
        Some(appended) => appended
            .iter()
            .map(|file| action::add_data_file(next.id, file))
            .collect(),
        None => vec![action::upsert_fragment(next)],
    }
}

/// The files `next` appends to `previous` when that is its only change.
/// Every other field is named so a new field cannot slip past unchecked.
fn appended_files<'a>(previous: &Fragment, next: &'a Fragment) -> Option<&'a [DataFile]> {
    let Fragment {
        id,
        files,
        overlays,
        deletion_file,
        row_id_meta,
        physical_rows,
        last_updated_at_version_meta,
        created_at_version_meta,
    } = previous;
    let unchanged = *id == next.id
        && *overlays == next.overlays
        && *deletion_file == next.deletion_file
        && *row_id_meta == next.row_id_meta
        && *physical_rows == next.physical_rows
        && *last_updated_at_version_meta == next.last_updated_at_version_meta
        && *created_at_version_meta == next.created_at_version_meta;
    (unchanged && next.files.len() > files.len() && next.files.starts_with(files))
        .then(|| &next.files[files.len()..])
}

fn changes_only_deletion_file(previous: &Fragment, next: &Fragment) -> bool {
    previous.deletion_file != next.deletion_file
        && Fragment {
            deletion_file: next.deletion_file.clone(),
            ..previous.clone()
        } == *next
}

fn reproduces(
    touched: &TouchedFragments,
    actions: &[lance_table::format::pb::FragmentAction],
    expected: &[Fragment],
) -> bool {
    let mut result = touched.fragments.clone();
    let messages = actions
        .iter()
        .enumerate()
        .map(
            |(action_sequence, action)| lance_table::format::pb::FragmentTreeMutation {
                action_sequence: action_sequence as u64 + 1,
                action: Some(action.clone()),
                ..Default::default()
            },
        )
        .collect();
    lance_table::fragment_metadata::node::apply_actions(&mut result, messages).is_ok()
        && result.values().eq(expected.iter())
}

async fn resolve_touched(
    dataset: &Dataset,
    tree: &mut FragmentTree,
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

#[cfg(test)]
mod tests {
    use super::*;
    use lance_table::format::pb::fragment_action::Action;
    use lance_table::format::{DeletionFile, DeletionFileType, RowDatasetVersionMeta};
    use lance_table::fragment_metadata::support::{make_backfill_data_file, make_fragment};
    use std::collections::BTreeMap;

    fn backfilled(id: u64, columns: impl IntoIterator<Item = u32>) -> Fragment {
        let mut fragment = make_fragment(id);
        for column in columns {
            fragment.files.push(make_backfill_data_file(id, column));
        }
        fragment
    }

    #[test]
    fn appended_files_is_a_strict_suffix_with_nothing_else_changed() {
        let previous = backfilled(7, []);
        let next = backfilled(7, [0, 1]);
        assert_eq!(appended_files(&previous, &next), Some(&next.files[1..]));
        assert_eq!(appended_files(&previous, &previous), None);
        assert_eq!(appended_files(&next, &previous), None);

        let mut reordered = next.clone();
        reordered.files.swap(0, 1);
        assert_eq!(appended_files(&previous, &reordered), None);
        let mut replaced = next.clone();
        replaced.files[0].path = "elsewhere.lance".into();
        assert_eq!(appended_files(&previous, &replaced), None);
        let mut deleted = next.clone();
        deleted.deletion_file = Some(DeletionFile {
            read_version: 1,
            id: 1,
            file_type: DeletionFileType::Bitmap,
            num_deleted_rows: Some(1),
            base_id: None,
        });
        assert_eq!(appended_files(&previous, &deleted), None);
        let mut rows = next.clone();
        rows.physical_rows = Some(2);
        assert_eq!(appended_files(&previous, &rows), None);
        let mut renumbered = next.clone();
        renumbered.id = 8;
        assert_eq!(appended_files(&previous, &renumbered), None);
        let mut versioned = next;
        versioned.last_updated_at_version_meta =
            Some(RowDatasetVersionMeta::Inline(Arc::from([1u8, 2, 3])));
        assert_eq!(appended_files(&previous, &versioned), None);
    }

    #[test]
    fn backfill_actions_add_files_in_order_or_upsert_the_record() {
        let touched = TouchedFragments {
            ids: BTreeSet::from([7]),
            fragments: BTreeMap::from([(7, backfilled(7, []))]),
        };
        let next = backfilled(7, [0, 1]);
        let actions = backfill_actions(&touched, &next);
        assert_eq!(actions.len(), 2);
        for (action, file) in actions.iter().zip(&next.files[1..]) {
            match &action.action {
                Some(Action::AddDataFile(add)) => {
                    assert_eq!(add.frag_id, 7);
                    assert_eq!(add.file.as_ref().unwrap().path, file.path);
                }
                other => panic!("expected AddDataFile, got {other:?}"),
            }
        }
        let fresh = backfilled(8, [0]);
        assert!(matches!(
            backfill_actions(&touched, &fresh)[..],
            [pb::FragmentAction {
                action: Some(Action::UpsertFragment(_))
            }]
        ));
        let mut reordered = next;
        reordered.files.swap(0, 1);
        assert!(matches!(
            backfill_actions(&touched, &reordered)[..],
            [pb::FragmentAction {
                action: Some(Action::UpsertFragment(_))
            }]
        ));
    }
}
