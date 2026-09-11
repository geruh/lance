// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Validation inputs and storage-action semantics.
//!
//! Native transactions are validated against current fragment state before
//! lowering. The tree only replays the resulting state changes.

use std::collections::{BTreeMap, BTreeSet};

use crate::format::pb::{self, fragment_action::Action};
use crate::format::{DataFile, Fragment};
use crate::fragment_metadata::action;
use lance_core::{Error, Result};

/// The current state of the fragments one commit touches, and nothing else.
///
/// `ids` records every id that was resolved, so an id that is absent from
/// `fragments` is known to be missing rather than unresolved. A commit may
/// only mutate ids in this set, or append ids the tree has never assigned.
#[derive(Debug, Clone, Default)]
pub struct TouchedFragments {
    pub ids: BTreeSet<u64>,
    pub fragments: BTreeMap<u64, Fragment>,
}

impl TouchedFragments {
    pub fn get(&self, fragment_id: u64) -> Option<&Fragment> {
        self.fragments.get(&fragment_id)
    }

    pub fn resolved(&self, fragment_id: u64) -> bool {
        self.ids.contains(&fragment_id)
    }
}

/// A commit whose every action has been validated against
/// [`TouchedFragments`] and is safe to store.
#[derive(Debug, Clone)]
pub struct ValidatedCommit {
    /// Advance the ID allocator, including reservations; never decrease it.
    pub next_fragment_id: Option<u64>,
    pub fragment_actions: Vec<pb::FragmentAction>,
}

impl ValidatedCommit {
    pub fn fragment_actions(fragment_actions: Vec<pb::FragmentAction>) -> Self {
        Self {
            next_fragment_id: None,
            fragment_actions,
        }
    }
}

/// Production `Operation::DataReplacement` for one fragment, decided against
/// the fragment's current state:
///
/// * a data file with the same fields and file version is swapped in place,
///   which becomes a [`pb::ReplaceDataFile`] naming the file it replaces;
/// * a replacement whose fields the fragment does not cover at all is the
///   add-column case and becomes an [`pb::AddDataFile`];
/// * a missing fragment, a partial field overlap, or a replacement identical
///   to the existing file is rejected, as the flat commit rejects them.
pub fn data_replacement(
    current: Option<&Fragment>,
    fragment_id: u64,
    replacement: &DataFile,
) -> Result<Vec<pb::FragmentAction>> {
    let fragment = current.ok_or_else(|| {
        Error::invalid_input(format!(
            "DataReplacement targets fragment {fragment_id} which does not exist"
        ))
    })?;
    let matching: Vec<&DataFile> = fragment
        .files
        .iter()
        .filter(|file| {
            file.fields == replacement.fields
                && file.file_major_version == replacement.file_major_version
                && file.file_minor_version == replacement.file_minor_version
        })
        .collect();
    let actions = if !matching.is_empty() {
        let unchanged = matching.iter().all(|file| {
            file.path == replacement.path
                && file.file_size_bytes == replacement.file_size_bytes
                && file.base_id == replacement.base_id
        });
        if unchanged {
            return Err(Error::invalid_input(format!(
                "DataReplacement for fragment {fragment_id} made no changes: the replacement \
                 matches the existing data file {} exactly",
                replacement.path
            )));
        }
        matching
            .iter()
            .map(|file| action::replace_data_file(fragment_id, &file.path, replacement))
            .collect::<Vec<_>>()
    } else {
        let covered = fragment
            .files
            .iter()
            .flat_map(|file| file.fields.iter())
            .any(|field_id| replacement.fields.contains(field_id));
        if covered {
            return Err(Error::invalid_input(format!(
                "DataReplacement for fragment {fragment_id} partially overlaps existing fields: \
                 replacement fields={:?}",
                replacement.fields
            )));
        }
        vec![action::add_data_file(fragment_id, replacement)]
    };

    let mut paths = BTreeSet::new();
    let aliases = fragment.files.iter().any(|file| !paths.insert(&file.path));
    let rename_collision =
        matching.len() > 1 && matching.iter().any(|file| file.path == replacement.path);
    let mut overlays = fragment.overlays.clone();
    let fields: Vec<u32> = replacement
        .fields
        .iter()
        .filter_map(|field| u32::try_from(*field).ok())
        .collect();
    crate::format::overlay::tombstone_overlay_fields(&mut overlays, &fields);
    if overlays == fragment.overlays && !(aliases || rename_collision) {
        return Ok(actions);
    }

    // Validation selects by fields/version, while a storage replacement selects
    // by first path. A whole-state update retains that decision when aliases,
    // rename collisions, or overlay tombstones cannot be expressed by the
    // smaller location-only action language.
    let mut updated = fragment.clone();
    updated.overlays = overlays;
    if matching.is_empty() {
        updated.files.push(replacement.clone());
    } else {
        for file in &mut updated.files {
            if file.fields == replacement.fields
                && file.file_major_version == replacement.file_major_version
                && file.file_minor_version == replacement.file_minor_version
            {
                file.path = replacement.path.clone();
                file.file_size_bytes = replacement.file_size_bytes.clone();
                file.base_id = replacement.base_id;
            }
        }
    }
    Ok(vec![action::add_fragment(&updated)])
}

/// Check that every storage action targets a fragment the commit resolved,
/// or appends an id the tree has never assigned, then apply the actions to
/// the touched snapshot to derive each action's fragment-count and row-count
/// contribution. Application errors here are storage invariant violations,
/// not user errors: a validated action must always apply.
pub(crate) fn aggregate_deltas(
    actions: &[pb::FragmentAction],
    touched: &TouchedFragments,
    next_fragment_id: u64,
) -> Result<Vec<ActionDeltas>> {
    let mut snapshot = touched.fragments.clone();
    let mut created = BTreeSet::new();
    let mut deltas = Vec::with_capacity(actions.len());
    for (offset, fragment_action) in actions.iter().enumerate() {
        let fragment_id = action::target_frag_id(fragment_action).ok_or_else(|| {
            Error::invalid_input("fragment metadata tree commit contains an empty fragment action")
        })?;
        let fresh_append = matches!(
            fragment_action.action,
            Some(Action::AddFragment(_)) if fragment_id >= next_fragment_id
        );
        if !fresh_append && !touched.resolved(fragment_id) && !created.contains(&fragment_id) {
            return Err(Error::invalid_input(format!(
                "fragment metadata tree commit mutates fragment {fragment_id} without resolving it first"
            )));
        }
        let before = snapshot.get(&fragment_id).cloned();
        crate::fragment_metadata::node::apply_actions(
            &mut snapshot,
            vec![pb::FragmentMetadataMutation {
                action_sequence: offset as u64,
                action: Some(fragment_action.clone()),
                fragment_count_delta: 0,
                total_rows_delta: 0,
                visible_rows_delta: 0,
            }],
        )?;
        if fresh_append {
            created.insert(fragment_id);
        }
        let after = snapshot.get(&fragment_id);
        if let Some(after) = after {
            require_known_counts(after)?;
        }
        deltas.push(ActionDeltas {
            fragment_count: i64::from(after.is_some()) - i64::from(before.is_some()),
            physical_rows: physical_rows(after)? - physical_rows(before.as_ref())?,
            visible_rows: visible_rows(after)? - visible_rows(before.as_ref())?,
        });
    }
    Ok(deltas)
}

/// The writer invariant behind exact visible-row aggregates: every fragment
/// stored in the tree has a known physical row count, and every deletion
/// file a known deleted count. Production writers always produce both;
/// conversion of older data must compute the missing count or refuse.
pub fn require_known_counts(fragment: &Fragment) -> Result<()> {
    if fragment.id > u64::from(u32::MAX) {
        return Err(Error::invalid_input(format!(
            "Fragment ID {} exceeds u32",
            fragment.id
        )));
    }
    if fragment.physical_rows.is_none() {
        return Err(Error::invalid_input(format!(
            "fragment {} has no physical row count; the fragment metadata tree requires known counts",
            fragment.id
        )));
    }
    if let Some(deletion_file) = &fragment.deletion_file
        && deletion_file.num_deleted_rows.is_none()
    {
        return Err(Error::invalid_input(format!(
            "fragment {} has a deletion file without a deleted-row count; the fragment metadata tree requires known counts",
            fragment.id
        )));
    }
    if let (Some(rows), Some(deleted)) = (
        fragment.physical_rows,
        fragment
            .deletion_file
            .as_ref()
            .and_then(|file| file.num_deleted_rows),
    ) && deleted > rows
    {
        return Err(Error::invalid_input(format!(
            "fragment {} has {deleted} deleted rows but only {rows} physical rows",
            fragment.id
        )));
    }
    Ok(())
}

/// One action's contribution to the tree aggregates, derived by applying it
/// to the touched snapshot.
#[derive(Debug, Clone, Copy)]
pub struct ActionDeltas {
    pub fragment_count: i64,
    pub physical_rows: i64,
    /// Physical minus deleted rows, exact by [`require_known_counts`].
    pub visible_rows: i64,
}

/// A fragment's visible rows. An absent fragment contributes zero; counts
/// are known by [`require_known_counts`], enforced before this is read.
fn visible_rows(fragment: Option<&Fragment>) -> Result<i64> {
    let Some(fragment) = fragment else {
        return Ok(0);
    };
    let rows = fragment.num_rows().ok_or_else(|| {
        Error::internal(format!(
            "fragment {} reached visible-row accounting with unknown counts; \
             require_known_counts must run first",
            fragment.id
        ))
    })?;
    to_i64(rows)
}

fn to_i64(rows: usize) -> Result<i64> {
    i64::try_from(rows).map_err(|_| {
        Error::invalid_input(format!(
            "row count does not fit aggregate delta: rows={rows}"
        ))
    })
}

fn physical_rows(fragment: Option<&Fragment>) -> Result<i64> {
    let rows = fragment
        .and_then(|fragment| fragment.physical_rows)
        .unwrap_or(0);
    i64::try_from(rows).map_err(|_| {
        Error::invalid_input(format!(
            "fragment physical_rows does not fit aggregate delta: physical_rows={rows}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment_metadata::support::{
        make_backfill_data_file, make_fragment, make_replacement_data_file,
    };

    #[test]
    fn data_replacement_swaps_appends_or_rejects_like_production() {
        let fragment = make_fragment(7);

        // Exact fields and file version: swap the named file in place.
        let matching = make_replacement_data_file(7, 0);
        let actions = data_replacement(Some(&fragment), 7, &matching).unwrap();
        assert_eq!(actions.len(), 1);
        match actions[0].action.as_ref().unwrap() {
            Action::ReplaceDataFile(replace) => {
                assert_eq!(replace.expected_path, fragment.files[0].path);
                assert_eq!(replace.path, matching.path);
            }
            other => panic!("expected ReplaceDataFile, got {other:?}"),
        }

        // Disjoint fields: the all-NULL add-column case appends verbatim.
        let disjoint = make_backfill_data_file(7, 0);
        let actions = data_replacement(Some(&fragment), 7, &disjoint).unwrap();
        assert!(matches!(
            actions[0].action.as_ref().unwrap(),
            Action::AddDataFile(add) if add.frag_id == 7
        ));

        // Identical file: rejected as a no-op.
        let error = data_replacement(Some(&fragment), 7, &fragment.files[0]).unwrap_err();
        assert!(error.to_string().contains("no changes"), "{error}");

        // Partial field overlap: rejected.
        let mut overlapping = make_replacement_data_file(7, 1);
        overlapping.fields = vec![1, 99].into();
        let error = data_replacement(Some(&fragment), 7, &overlapping).unwrap_err();
        assert!(error.to_string().contains("partially overlaps"), "{error}");

        // Missing fragment: rejected.
        let error = data_replacement(None, 99, &matching).unwrap_err();
        assert!(error.to_string().contains("fragment 99"), "{error}");
    }

    #[test]
    fn aggregate_deltas_reject_unresolved_mutations_and_stale_appends() {
        let touched = TouchedFragments::default();
        let error = aggregate_deltas(&[action::remove_fragment(3)], &touched, 10).unwrap_err();
        assert!(error.to_string().contains("without resolving"), "{error}");

        let error =
            aggregate_deltas(&[action::add_fragment(&make_fragment(3))], &touched, 10).unwrap_err();
        assert!(error.to_string().contains("without resolving"), "{error}");

        let deltas =
            aggregate_deltas(&[action::add_fragment(&make_fragment(10))], &touched, 10).unwrap();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].fragment_count, 1);
        assert_eq!(deltas[0].physical_rows, 1);
        assert_eq!(deltas[0].visible_rows, 1);

        // The writer invariant: unknown counts are rejected at the door.
        let mut unknown_physical = make_fragment(10);
        unknown_physical.physical_rows = None;
        let error = require_known_counts(&unknown_physical).unwrap_err();
        assert!(error.to_string().contains("physical row count"), "{error}");
        let empty = make_fragment(10).with_physical_rows(0);
        let deltas = aggregate_deltas(&[action::add_fragment(&empty)], &touched, 10).unwrap();
        assert_eq!(deltas[0].physical_rows, 0);
    }
}
