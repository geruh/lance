// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Validate persisted metadata before routing or replay can discard information.

use std::collections::BTreeSet;

use lance_core::{Error, Result};

use super::{action, node};
use crate::format::{Fragment, pb};
use pb::fragment_action::Action;

pub(super) fn corrupt(message: impl Into<String>) -> Error {
    Error::corrupt_file_named("Fragment metadata", message.into())
}

/// Counts in this format are always known, including zero. The shared flat
/// protobuf conversion treats zero as a legacy unknown count.
pub(super) fn fragment(encoded: pb::DataFragment) -> Result<Fragment> {
    let rows = usize::try_from(encoded.physical_rows)
        .map_err(|_| corrupt(format!("fragment {} row count exceeds usize", encoded.id)))?;
    let deleted = encoded
        .deletion_file
        .as_ref()
        .map(|file| file.num_deleted_rows);
    if encoded.id > u64::from(u32::MAX)
        || deleted.is_some_and(|count| count > encoded.physical_rows)
    {
        return Err(corrupt(format!(
            "fragment {} has physical_rows={}, deleted_rows={deleted:?}",
            encoded.id, encoded.physical_rows,
        )));
    }
    let mut fragment = Fragment::try_from(encoded)?;
    fragment.physical_rows = Some(rows);
    if let Some(file) = &mut fragment.deletion_file {
        file.num_deleted_rows = deleted.map(|count| count as usize);
    }
    Ok(fragment)
}

pub(super) fn action(encoded: &pb::FragmentAction) -> Result<u64> {
    let id = action::target_frag_id(encoded)
        .ok_or_else(|| corrupt("Buffered action has no recognized variant"))?;
    if id > u64::from(u32::MAX) {
        return Err(corrupt(format!(
            "Buffered action Fragment ID {id} exceeds u32"
        )));
    }
    match &encoded.action {
        Some(Action::AddFragment(value)) => {
            fragment(value.clone())?;
        }
        Some(Action::AddDataFile(value)) if value.file.is_none() => {
            return Err(corrupt(format!(
                "AddDataFile for fragment {id} has no file"
            )));
        }
        Some(Action::ReplaceDataFile(value))
            if value.expected_path.is_empty() || value.path.is_empty() =>
        {
            return Err(corrupt(format!(
                "ReplaceDataFile for fragment {id} is missing expected_path or path"
            )));
        }
        Some(Action::AddDeletionFile(value)) if value.deletion_file.is_none() => {
            return Err(corrupt(format!(
                "AddDeletionFile for fragment {id} has no file"
            )));
        }
        _ => {}
    }
    Ok(id)
}

pub(super) fn buffer(
    actions: &[pb::FragmentMetadataMutation],
    first_action_sequence: u64,
    next_action_sequence: u64,
) -> Result<()> {
    let mut seen = BTreeSet::new();
    for tagged in actions {
        if tagged.action_sequence < first_action_sequence
            || tagged.action_sequence >= next_action_sequence
            || !seen.insert(tagged.action_sequence)
        {
            return Err(corrupt(format!(
                "Buffered action sequence number {} is duplicated or outside [{first_action_sequence}, {next_action_sequence})",
                tagged.action_sequence,
            )));
        }
        action(tagged.action.as_ref().ok_or_else(|| {
            corrupt(format!(
                "Buffered action sequence number {} has no action",
                tagged.action_sequence
            ))
        })?)?;
    }
    Ok(())
}

/// `lower_bound` is the range start the parent assigned to this list. It is 0
/// for the root and the parent's entry for an interior.
pub(super) fn children(
    children: &[pb::FragmentMetadataChild],
    next_action_sequence: u64,
    lower_bound: u64,
) -> Result<()> {
    let mut paths = BTreeSet::new();
    for (index, child) in children.iter().enumerate() {
        if (index == 0 && child.min_key != lower_bound)
            || child.path.is_empty()
            || !paths.insert(&child.path)
            || child.min_key > u64::from(u32::MAX)
            || child.object_size == 0
            || child.visible_rows > child.total_rows
            || child.height != children[0].height
            || child.height == u32::MAX
            || (child.height == 0
                && (child.num_keys == 0
                    || child.num_children != 0
                    || child.materialized_through_action_sequence >= next_action_sequence))
            || (child.height > 0
                && (child.num_children < 2 || child.materialized_through_action_sequence != 0))
            || (index > 0 && children[index - 1].min_key >= child.min_key)
        {
            return Err(corrupt(format!(
                "Invalid child reference at position {index}: {child:?}"
            )));
        }
    }
    Ok(())
}

/// Validate the root and derive its fragment, physical-row and visible-row
/// totals. `next_fragment_id` comes from the Version Manifest's high-water mark.
pub(super) fn root(
    root: &pb::FragmentMetadataRoot,
    next_fragment_id: u64,
) -> Result<(u64, u64, u64)> {
    if root.next_action_sequence == 0 {
        return Err(corrupt("Root next_action_sequence must be at least 1"));
    }
    children(&root.children, root.next_action_sequence, 0)?;
    buffer(&root.buffer, 1, root.next_action_sequence)?;
    let summary = node::internal_ref(String::new(), &root.children, &root.buffer, 0)?;
    if summary.visible_rows > summary.total_rows {
        return Err(corrupt(
            "Root visible rows exceed physical rows derived from children and buffer",
        ));
    }
    if next_fragment_id == 0 {
        if !root.children.is_empty() || !root.buffer.is_empty() {
            return Err(corrupt(
                "Tree names fragment state but Manifest.max_fragment_id is absent",
            ));
        }
    } else if root
        .buffer
        .iter()
        .any(|tagged| node::action_key(tagged) >= next_fragment_id)
    {
        return Err(corrupt(format!(
            "Root contains a fragment beyond allocation high-water mark {next_fragment_id}"
        )));
    }
    Ok((summary.num_keys, summary.total_rows, summary.visible_rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn rejects_height_overflow_without_panicking() {
        let root = pb::FragmentMetadataRoot {
            next_action_sequence: 1,
            children: vec![pb::FragmentMetadataChild {
                path: "_bt/node/overflow.node".into(),
                height: u32::MAX,
                num_children: 2,
                object_size: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        let error = super::root(&root, 1).unwrap_err();
        assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
        assert!(error.to_string().contains("height"), "{error}");
    }

    #[rstest]
    #[case::missing_payload(None)]
    #[case::missing_variant(Some(pb::FragmentAction { action: None }))]
    #[case::missing_file(Some(pb::FragmentAction { action: Some(Action::AddDataFile(pb::AddDataFile { frag_id: 1, file: None })) }))]
    fn rejects_incomplete_actions(#[case] action: Option<pb::FragmentAction>) {
        let error = buffer(
            &[pb::FragmentMetadataMutation {
                action_sequence: 1,
                action,
                ..Default::default()
            }],
            1,
            2,
        )
        .unwrap_err();
        assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
    }

    #[test]
    fn sequence_numbers_may_have_gaps_but_must_be_unique() {
        let make = |action_sequence| pb::FragmentMetadataMutation {
            action_sequence,
            action: Some(action::remove_fragment(1)),
            ..Default::default()
        };
        buffer(&[make(5), make(2)], 1, 6).unwrap();
        for actions in [vec![make(2), make(2)], vec![make(0)], vec![make(6)]] {
            let error = buffer(&actions, 1, 6).unwrap_err();
            assert!(matches!(error, Error::CorruptFile { .. }), "{error}");
            assert!(error.to_string().contains("action sequence number"));
        }
    }

    #[test]
    fn zero_is_a_known_row_count() {
        let fragment = fragment(pb::DataFragment {
            id: 1,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(fragment.physical_rows, Some(0));
        assert_eq!(fragment.num_rows(), Some(0));
    }
}
