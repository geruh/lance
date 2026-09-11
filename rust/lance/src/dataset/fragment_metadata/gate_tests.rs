// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Native transaction validation and conflict parity against flat manifests.

use super::differential::{
    Differential, add_column, delete_fragments, production_style_append,
    production_style_append_with_version, replace_base, set_deletion_file,
};
use crate::dataset::transaction::{DataReplacementGroup, Operation, RewriteGroup, UpdateMode};
use lance_core::Error;
use lance_file::version::LanceFileVersion;
use lance_table::format::DataFile;
use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
use lance_table::fragment_metadata::support::{make_backfill_data_file, make_fragment};
use std::num::NonZero;

const N: u64 = 300;

#[rstest::rstest]
#[case::update_columns("update_columns")]
#[case::update_rows("update_rows")]
#[case::rewrite("rewrite")]
#[case::overwrite("overwrite")]
#[case::reserved_rewrite("reserved_rewrite")]
#[tokio::test]
async fn native_operation_results_match_flat(#[case] kind: &str) {
    let mut diff = Differential::create(N).await;
    let initial = diff.flat_state().await;
    let mut old = vec![initial.fragments[&7].clone(), initial.fragments[&8].clone()];
    let mut new = old.clone();
    for fragment in &mut new {
        fragment.files[0].path = format!("{kind}-{}.lance", fragment.id);
    }
    let operation = match kind {
        "update_columns" => Operation::Update {
            removed_fragment_ids: Vec::new(),
            updated_fragments: new,
            new_fragments: Vec::new(),
            fields_modified: vec![0, 1],
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: Vec::new(),
            update_mode: Some(UpdateMode::RewriteColumns),
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        },
        "update_rows" => {
            for fragment in &mut new {
                fragment.id = 0;
            }
            Operation::Update {
                removed_fragment_ids: vec![7, 8],
                updated_fragments: Vec::new(),
                new_fragments: new,
                fields_modified: Vec::new(),
                compacted_sstables: Vec::new(),
                fields_for_preserving_frag_bitmap: Vec::new(),
                update_mode: Some(UpdateMode::RewriteRows),
                inserted_rows_filter: None,
                updated_fragment_offsets: None,
            }
        }
        "rewrite" | "reserved_rewrite" => {
            if kind == "reserved_rewrite" {
                let reserved = diff
                    .apply_latest("reserve", Operation::ReserveFragments { num_fragments: 2 })
                    .await;
                assert!(
                    reserved.both_accepted_and_equal(),
                    "flat={:?} tree={:?}",
                    reserved.flat,
                    reserved.fragment_metadata
                );
                new[0].id = N;
                new[1].id = N + 1;
            } else {
                for fragment in &mut new {
                    fragment.id = 0;
                }
            }
            Operation::Rewrite {
                groups: vec![RewriteGroup {
                    old_fragments: std::mem::take(&mut old),
                    new_fragments: new,
                }],
                rewritten_indices: Vec::new(),
                frag_reuse_index: None,
            }
        }
        "overwrite" => {
            for fragment in &mut new {
                fragment.id = 0;
            }
            Operation::Overwrite {
                fragments: new,
                schema: initial.schema.clone(),
                config_upsert_values: None,
                initial_bases: None,
            }
        }
        _ => unreachable!(),
    };
    let result = diff.apply_latest(kind, operation).await;
    assert!(
        result.both_accepted_and_equal(),
        "flat={:?} tree={:?} diff={:?}",
        result.flat,
        result.fragment_metadata,
        result.diff
    );
    let appended = diff
        .apply_latest("append_after", production_style_append(3))
        .await;
    assert!(
        appended.both_accepted_and_equal(),
        "flat={:?} tree={:?} diff={:?}",
        appended.flat,
        appended.fragment_metadata,
        appended.diff
    );
    let tree = diff.fragment_metadata_reader().await;
    tree.tree.verify_watermarks().await.unwrap();
}

#[tokio::test]
async fn replacement_validation_resolves_fields_before_aliased_paths() {
    let mut fragment = make_fragment(0);
    let mut backfill = make_backfill_data_file(0, 0);
    backfill.path = fragment.files[0].path.clone();
    fragment.files.push(backfill.clone());
    let mut diff = Differential::create_with_fragments(vec![fragment]).await;
    backfill.path = "replacement-of-second-occurrence.lance".to_string();
    let result = diff
        .apply_latest(
            "replace_alias",
            Operation::DataReplacement {
                replacements: vec![DataReplacementGroup(0, backfill)],
            },
        )
        .await;
    assert!(
        result.both_accepted_and_equal(),
        "flat={:?}, tree={:?}, diff={:?}",
        result.flat,
        result.fragment_metadata,
        result.diff
    );
    let mut stale = make_backfill_data_file(0, 0);
    stale.path = "stale-replacement.lance".to_string();
    let result = diff
        .apply(
            "stale_alias",
            1,
            Operation::DataReplacement {
                replacements: vec![DataReplacementGroup(0, stale)],
            },
        )
        .await;
    assert!(
        result.both_rejected(),
        "Raw replacement intent must conflict even when storage used a whole Fragment SET: {:?}",
        result.diff
    );
}

#[rstest::rstest]
#[case::fully_superseded(false)]
#[case::partly_superseded(true)]
#[tokio::test]
async fn replacement_tombstones_overlays_like_flat(#[case] partial: bool) {
    let mut fragment = make_fragment(0);
    let mut overlay = fragment.files[0].clone();
    overlay.path = "overlay.lance".to_string();
    if partial {
        overlay.fields = vec![0, 1, 2].into();
        overlay.column_indices = vec![0, 1, 2].into();
    }
    fragment.overlays.push(DataOverlayFile {
        data_file: overlay,
        coverage: OverlayCoverage::Shared(std::sync::Arc::new([0].into_iter().collect())),
        committed_version: 1,
    });
    let mut diff = Differential::create_with_fragments(vec![fragment]).await;
    let result = diff
        .apply_latest("replace_overlay_base", replace_base([0], 1))
        .await;
    assert!(
        result.both_accepted_and_equal(),
        "flat={:?}, tree={:?}, diff={:?}",
        result.flat,
        result.fragment_metadata,
        result.diff
    );
    let state = diff.flat_state().await;
    let overlays = &state.fragments[&0].overlays;
    if partial {
        assert_eq!(overlays[0].data_file.fields.as_ref(), &[-2, -2, 2]);
    } else {
        assert!(overlays.is_empty());
    }
}

#[tokio::test]
async fn add_then_replace_keeps_original_column_mapping_like_flat() {
    let mut diff = Differential::create(N).await;
    let first = diff.apply_latest("add_file", add_column([7], 0)).await;
    assert!(first.both_accepted_and_equal(), "{:?}", first.diff);
    let mut replacement = make_backfill_data_file(7, 0);
    replacement.path = "replacement-with-different-column-mapping.lance".to_string();
    replacement.column_indices = vec![9].into();
    let second = diff
        .apply_latest(
            "replace_file",
            Operation::DataReplacement {
                replacements: vec![DataReplacementGroup(7, replacement)],
            },
        )
        .await;
    assert!(second.both_accepted_and_equal(), "{:?}", second.diff);
    let current = diff.flat_state().await;
    assert_eq!(current.fragments[&7].files[1].column_indices.as_ref(), &[0]);
}

/// Scattered ids that avoid the range the mixed stream deletes.
fn scattered(round: u64, count: u64) -> Vec<u64> {
    (0..count)
        .map(|i| (i * 37 + round * 11) % N)
        .filter(|id| !(100..140).contains(id))
        .collect()
}

#[tokio::test]
async fn semantic_matrix_matches_flat_through_leaf_application() {
    let mut diff = Differential::create(N).await;

    let steps: Vec<(&str, Operation)> = vec![
        ("append_prod_ids", production_style_append(5)),
        ("append_prod_ids", production_style_append(3)),
        ("add_column_contiguous", add_column(0..10, 0)),
        ("add_column_no_op_rejected", add_column(0..3, 0)),
        ("add_column_scattered", add_column(scattered(0, 10), 1)),
        ("replace_contiguous", replace_base(0..10, 0)),
        ("replace_same_fragment_again", replace_base(0..10, 1)),
        ("add_column_second_col_same_frags", add_column(0..10, 2)),
        ("delete_whole_fragments", delete_fragments([7, 8, 9])),
    ];
    for (step, operation) in steps {
        let outcome = diff.apply_latest(step, operation).await;
        if step == "add_column_no_op_rejected" {
            assert!(
                outcome.both_rejected(),
                "{step}: flat={:?} fragment_metadata={:?}",
                outcome.flat.as_ref().err(),
                outcome.fragment_metadata.as_ref().err()
            );
            continue;
        }
        assert!(
            outcome.both_accepted_and_equal(),
            "{step}: flat={:?} fragment_metadata={:?} diff={:?}",
            outcome.flat.as_ref().err(),
            outcome.fragment_metadata.as_ref().err(),
            outcome.diff
        );
    }

    let dv_target = N;
    let flat_now = diff.flat_state().await;
    let fragment = flat_now
        .fragments
        .get(&dv_target)
        .unwrap_or_else(|| {
            panic!(
                "fragment {dv_target} missing on flat side; ids={:?}",
                flat_now.fragments.keys().collect::<Vec<_>>()
            )
        })
        .clone();
    let outcome = diff
        .apply_latest("add_deletion_file", set_deletion_file(fragment, 1))
        .await;
    assert!(
        outcome.both_accepted_and_equal(),
        "add_deletion_file: {:?}",
        outcome.diff
    );
    let fragment = diff.flat_state().await.fragments[&dv_target].clone();
    let outcome = diff
        .apply_latest("replace_deletion_file", set_deletion_file(fragment, 2))
        .await;
    assert!(
        outcome.both_accepted_and_equal(),
        "replace_deletion_file: {:?}",
        outcome.diff
    );
    let fragment = diff.flat_state().await.fragments[&5].clone();
    let outcome = diff
        .apply_latest(
            "deletion_file_covering_all_rows_prunes",
            set_deletion_file(fragment, 3),
        )
        .await;
    assert!(
        outcome.both_accepted_and_equal(),
        "deletion_file_covering_all_rows_prunes: {:?}",
        outcome.diff
    );
    assert!(!diff.flat_state().await.fragments.contains_key(&5));

    for round in 0..40u64 {
        let col = 3 + round as u32;
        let live: Vec<u64> = diff.flat_state().await.fragments.keys().copied().collect();
        let ids: Vec<u64> = (0..10)
            .map(|i| live[((i * 37 + round * 11) % live.len() as u64) as usize])
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let replace_ids: Vec<u64> = ids.iter().copied().take(4).collect();
        let delete_id = *live
            .iter()
            .rev()
            .find(|id| !ids.contains(id))
            .expect("a live fragment outside the touched set");
        let ops: Vec<(&str, Operation)> = vec![
            ("mixed_append", production_style_append(2)),
            ("mixed_add_column", add_column(ids.clone(), col)),
            ("mixed_replace", replace_base(replace_ids, 2 + round as u32)),
            ("mixed_delete", delete_fragments([delete_id])),
        ];
        for (step, operation) in ops {
            let outcome = diff.apply_latest(step, operation).await;
            assert!(
                outcome.both_accepted_and_equal(),
                "round {round} {step}: flat={:?} fragment_metadata={:?} diff={:?}",
                outcome.flat.as_ref().err(),
                outcome.fragment_metadata.as_ref().err(),
                outcome.diff
            );
        }
    }

    let reader = diff.fragment_metadata_reader().await;
    let buffered = reader.tree.buffered_action_keys().await.unwrap();
    let state = diff.fragment_metadata_state().await;
    let leaf_applied = state
        .fragments
        .keys()
        .filter(|id| !buffered.contains(id))
        .count();
    assert!(
        reader.tree.height() >= 2,
        "stream must run against a multi-level tree, height={}",
        reader.tree.height()
    );
    assert!(
        leaf_applied > 0 && buffered.len() < state.fragments.len(),
        "actions must have reached leaves: buffered={} fragments={}",
        buffered.len(),
        state.fragments.len()
    );
}

async fn invalid_replacement_fails_its_own_commit(step: &str, operation: Operation) {
    let mut diff = Differential::create(N).await;
    let outcome = diff.apply_latest(step, operation).await;
    assert!(
        outcome.flat.is_err(),
        "{step}: flat must reject at commit, got version {:?}",
        outcome.flat
    );
    let mut later_failures = Vec::new();
    for round in 0..12 {
        let later = diff
            .apply_latest(
                &format!("{step}_later_append_{round}"),
                production_style_append(1),
            )
            .await;
        if let Err(error) = &later.fragment_metadata {
            later_failures.push(format!("round {round}: {error}"));
        }
    }
    assert!(
        outcome.fragment_metadata.is_err(),
        "{step}: fragment metadata tree accepted an invalid replacement at version {:?}; later commits failed: {:?}",
        outcome.fragment_metadata,
        later_failures
    );
    assert!(
        later_failures.is_empty(),
        "{step}: benign commits after a rejected replacement must succeed: {later_failures:?}"
    );
    assert!(diff.states_equal().await, "{step}: states diverged");
}

#[tokio::test]
async fn replacement_of_missing_fragment_fails_its_own_commit() {
    invalid_replacement_fails_its_own_commit("replace_missing_fragment", replace_base([9_999], 0))
        .await;
}

#[tokio::test]
async fn replacement_with_partial_field_overlap_fails_its_own_commit() {
    let mut file = make_backfill_data_file(42, 0);
    file = DataFile::new(
        file.path.clone(),
        vec![1, 99],
        vec![0, 1],
        lance_file::version::ConcreteFileVersion::from_data_file_numbers(
            file.file_major_version,
            file.file_minor_version,
        )
        .unwrap(),
        NonZero::new(4096),
        None,
    );
    invalid_replacement_fails_its_own_commit(
        "replace_partial_overlap",
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(42, file)],
        },
    )
    .await;
}

#[tokio::test]
async fn replacement_with_no_matching_file_version_fails_its_own_commit() {
    let (major, minor) = LanceFileVersion::V2_1.resolve().to_data_file_numbers();
    let base = make_fragment(42).files[0].clone();
    let file = DataFile::new(
        "data/replacement-v21.lance",
        vec![0, 1],
        vec![0, 1],
        lance_file::version::ConcreteFileVersion::from_data_file_numbers(major, minor).unwrap(),
        NonZero::new(1024),
        None,
    );
    assert_ne!(
        base.file_major_version.max(base.file_minor_version),
        u32::MAX
    );
    invalid_replacement_fails_its_own_commit(
        "replace_no_matching_file_version",
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(42, file)],
        },
    )
    .await;
}

#[tokio::test]
async fn replacement_of_fragment_deleted_earlier_fails_its_own_commit() {
    let mut diff = Differential::create(N).await;
    let outcome = diff.apply_latest("delete_42", delete_fragments([42])).await;
    assert!(outcome.both_accepted_and_equal());
    let outcome = diff
        .apply_latest("replace_deleted_42", replace_base([42], 0))
        .await;
    assert!(outcome.flat.is_err());
    assert!(
        outcome.fragment_metadata.is_err(),
        "fragment metadata tree accepted a replacement of a fragment deleted at the previous version"
    );
}

async fn conflict_pair(label: &str, a: Operation, b: Operation) -> (bool, bool, bool) {
    let mut diff = Differential::create(N).await;
    let base = diff.flat_state().await.version;
    let first = diff.apply(&format!("{label}_A"), base, a).await;
    assert!(
        first.both_accepted_and_equal(),
        "{label}: writer A must land: flat={:?} fragment_metadata={:?} diff={:?}",
        first.flat.as_ref().err(),
        first.fragment_metadata.as_ref().err(),
        first.diff
    );
    let second = diff.apply(&format!("{label}_B"), base, b).await;
    let parity = match (&second.flat, &second.fragment_metadata) {
        (Ok(_), Ok(_)) => second.parity == Some(true),
        (Err(_), Err(_)) => true,
        _ => false,
    };
    (
        second.flat.is_ok(),
        second.fragment_metadata.is_ok(),
        parity,
    )
}

macro_rules! conflict_test {
    ($name:ident, $csv:literal, $a:expr, $b:expr) => {
        #[tokio::test]
        async fn $name() {
            let (flat_ok, fragment_metadata_ok, parity) =
                conflict_pair(stringify!($name), $a, $b).await;
            assert!(
                parity,
                "{}: flat_accepted={flat_ok} fragment_metadata_accepted={fragment_metadata_ok}",
                stringify!($name)
            );
        }
    };
}

conflict_test!(
    conflict_append_vs_append,
    "conflict-append-append.csv",
    production_style_append(2),
    production_style_append(2)
);
conflict_test!(
    conflict_replace_f42_vs_replace_f42,
    "conflict-replace-replace.csv",
    replace_base([42], 0),
    replace_base([42], 1)
);
conflict_test!(
    conflict_replace_f42_vs_delete_f42,
    "conflict-replace-delete.csv",
    replace_base([42], 0),
    delete_fragments([42])
);
conflict_test!(
    conflict_delete_f42_vs_replace_f42,
    "conflict-delete-replace.csv",
    delete_fragments([42]),
    replace_base([42], 0)
);
conflict_test!(
    conflict_add_column_f42_vs_delete_f42,
    "conflict-addcol-delete.csv",
    add_column([42], 0),
    delete_fragments([42])
);
conflict_test!(
    conflict_delete_f42_vs_add_column_f42,
    "conflict-delete-addcol.csv",
    delete_fragments([42]),
    add_column([42], 0)
);
conflict_test!(
    conflict_add_column_f42_vs_add_column_f42_same_field,
    "conflict-addcol-addcol-same.csv",
    add_column([42], 0),
    add_column([42], 0)
);
conflict_test!(
    conflict_add_column_f42_vs_add_column_f42_other_field,
    "conflict-addcol-addcol-other.csv",
    add_column([42], 0),
    add_column([42], 1)
);
conflict_test!(
    conflict_deletion_file_f42_vs_delete_f42,
    "conflict-dv-delete.csv",
    set_deletion_file(make_fragment(42), 1),
    delete_fragments([42])
);
conflict_test!(
    conflict_delete_f42_vs_deletion_file_f42,
    "conflict-delete-dv.csv",
    delete_fragments([42]),
    set_deletion_file(make_fragment(42), 1)
);
conflict_test!(
    conflict_update_f42_vs_update_f99,
    "conflict-replace-different.csv",
    replace_base([42], 0),
    replace_base([99], 0)
);
conflict_test!(
    conflict_separate_subtrees,
    "conflict-separate-subtrees.csv",
    add_column([1, 2, 3], 0),
    add_column([290, 291, 292], 0)
);

#[tokio::test]
async fn append_with_id_zero_gets_fresh_id_and_keeps_fragment_zero() {
    let mut diff = Differential::create(N).await;
    let outcome = diff
        .apply_latest("append_single_id_zero", production_style_append(1))
        .await;
    assert!(
        outcome.both_accepted_and_equal(),
        "flat={:?} fragment_metadata={:?} diff={:?}",
        outcome.flat.as_ref().err(),
        outcome.fragment_metadata.as_ref().err(),
        outcome.diff
    );
    let state = diff.fragment_metadata_state().await;
    assert_eq!(state.fragments.len(), N as usize + 1);
    assert!(state.fragments.contains_key(&N), "fresh id must be max + 1");
}

#[tokio::test]
async fn add_columns_updates_schema_and_files_atomically_like_flat() {
    let mut diff = Differential::create(N).await;
    let v1 = diff.flat_state().await.version;
    let ids: Vec<u64> = (0..10).collect();
    let outcome = diff.apply_add_columns("add_col0", v1, &ids, 0).await;
    assert!(
        outcome.both_accepted_and_equal(),
        "add_col0: flat={:?} fragment_metadata={:?} diff={:?}",
        outcome.flat.as_ref().err(),
        outcome.fragment_metadata.as_ref().err(),
        outcome.diff
    );
    let after = diff.fragment_metadata_state().await;
    assert_eq!(after.schema.fields.len(), 3);
    assert_eq!(after.fragments[&0].files.len(), 2);

    let previous = crate::dataset::fragment_metadata::test_support::Reader::open_uri_at(
        &diff.fragment_metadata.uri,
        v1,
    )
    .await
    .unwrap();
    assert_eq!(previous.schema().unwrap().fields.len(), 2);
    assert_eq!(
        previous
            .resolve_fragment(0)
            .await
            .unwrap()
            .unwrap()
            .files
            .len(),
        1
    );

    let v2 = diff.flat_state().await.version;
    let scattered_ids = scattered(1, 10);
    let outcome = diff
        .apply_add_columns("add_col1", v2, &scattered_ids, 1)
        .await;
    assert!(
        outcome.both_accepted_and_equal(),
        "add_col1: {:?}",
        outcome.diff
    );
    for round in 0..6 {
        let outcome = diff
            .apply_latest(
                &format!("post_schema_append_{round}"),
                production_style_append(1),
            )
            .await;
        assert!(
            outcome.both_accepted_and_equal(),
            "append after schema change: {:?}",
            outcome.diff
        );
    }
    let reader = diff.fragment_metadata_reader().await;
    assert!(reader.tree.verify_reachable().await.unwrap() > 0);
    assert!(
        diff.states_equal().await,
        "schema and fragments must survive the fold"
    );
    assert_eq!(diff.fragment_metadata_state().await.schema.fields.len(), 4);
}

#[tokio::test]
async fn add_columns_conflicts_match_flat() {
    let mut diff = Differential::create(N).await;
    let base = diff.flat_state().await.version;
    let outcome = diff.apply("delete_42", base, delete_fragments([42])).await;
    assert!(outcome.both_accepted_and_equal());
    let outcome = diff
        .apply_add_columns("stale_add_col_on_42", base, &[42, 43], 0)
        .await;
    assert!(
        outcome.flat.is_err() && outcome.fragment_metadata.is_err(),
        "stale add-column over a deleted target: flat={:?} fragment_metadata={:?}",
        outcome.flat,
        outcome.fragment_metadata
    );

    let base = diff.flat_state().await.version;
    let outcome = diff.apply_add_columns("add_col0", base, &[1, 2], 0).await;
    assert!(outcome.both_accepted_and_equal(), "{:?}", outcome.diff);
    let outcome = diff
        .apply(
            "stale_append_after_schema_change",
            base,
            production_style_append(1),
        )
        .await;
    assert!(
        matches!(outcome.flat, Err(Error::RetryableCommitConflict { .. }))
            && matches!(
                outcome.fragment_metadata,
                Err(Error::RetryableCommitConflict { .. })
            ),
        "stale append retries after a schema change on both sides: flat={:?} fragment_metadata={:?} diff={:?}",
        outcome.flat.as_ref().err(),
        outcome.fragment_metadata.as_ref().err(),
        outcome.diff
    );
}

#[tokio::test]
async fn transaction_history_is_owned_by_version_manifest() {
    let mut diff = Differential::create(N).await;
    let mut rows = vec!["operation,transaction_file_bytes,manifest_bytes".to_string()];
    for (step, operation) in [
        ("replace_10", replace_base(0..10, 0)),
        ("append_2", production_style_append(2)),
        ("delete_3", delete_fragments([100, 101, 102])),
    ] {
        let expected_operation = operation.name().to_string();
        let before: std::collections::HashSet<_> = std::fs::read_dir(
            std::path::Path::new(&diff.fragment_metadata.uri).join("_transactions"),
        )
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
        let outcome = diff.apply_latest(step, operation).await;
        assert!(
            outcome.both_accepted_and_equal(),
            "{step}: {:?}",
            outcome.diff
        );
        let version = outcome.fragment_metadata.as_ref().unwrap();
        let transaction_bytes: u64 = std::fs::read_dir(
            std::path::Path::new(&diff.fragment_metadata.uri).join("_transactions"),
        )
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| !before.contains(path))
        .map(|path| std::fs::metadata(path).unwrap().len())
        .sum();
        let dataset =
            crate::dataset::builder::DatasetBuilder::from_uri(&diff.fragment_metadata.uri)
                .load()
                .await
                .unwrap();
        assert_eq!(
            dataset
                .read_transaction()
                .await
                .unwrap()
                .unwrap()
                .operation
                .name(),
            expected_operation
        );
        assert_eq!(dataset.version().version, *version);
        assert!(dataset.manifest.fragment_metadata.is_some());
        assert!(
            !std::path::Path::new(&diff.fragment_metadata.uri)
                .join("_bt/root")
                .exists()
        );
        let delta_bytes = std::fs::metadata(
            std::path::Path::new(&diff.fragment_metadata.uri)
                .join("_versions")
                .join(dataset.manifest_location.path.filename().unwrap()),
        )
        .unwrap()
        .len();
        rows.push(format!("{step},{transaction_bytes},{delta_bytes}"));
        assert!(transaction_bytes > 0 && delta_bytes > 0);
    }
    if let Ok(dir) = std::env::var("FRAGMENT_METADATA_OVERNIGHT_RESULTS") {
        let mut text = rows.join("\n");
        text.push('\n');
        std::fs::write(
            std::path::Path::new(&dir).join("txn-manifest-history.csv"),
            text,
        )
        .unwrap();
    }
}

#[tokio::test]
async fn delete_with_missing_updated_fragment_matches_flat() {
    let mut diff = Differential::create(N).await;
    let outcome = diff.apply_latest("delete_42", delete_fragments([42])).await;
    assert!(outcome.both_accepted_and_equal());
    let mut ghost = make_fragment(42);
    ghost.physical_rows = Some(10);
    let outcome = diff
        .apply_latest("update_missing_42", set_deletion_file(ghost, 1))
        .await;
    assert!(
        outcome.both_accepted_and_equal() || outcome.both_rejected(),
        "flat={:?} fragment_metadata={:?} diff={:?}",
        outcome.flat.as_ref().err(),
        outcome.fragment_metadata.as_ref().err(),
        outcome.diff
    );
}

#[tokio::test]
async fn empty_dataset_bootstraps_and_grows_like_flat() {
    let mut diff = Differential::create(0).await;
    let storage_version = LanceFileVersion::default();
    for round in 0..12 {
        let outcome = diff
            .apply_latest(
                &format!("append_{round}"),
                production_style_append_with_version(3, storage_version),
            )
            .await;
        assert!(
            outcome.both_accepted_and_equal(),
            "round {round}: flat={:?} fragment_metadata={:?} diff={:?}",
            outcome.flat.as_ref().err(),
            outcome.fragment_metadata.as_ref().err(),
            outcome.diff
        );
    }
    assert_eq!(diff.fragment_metadata_state().await.fragments.len(), 36);
}

/// Writers A and B both read the same version; A lands; B commits stale.
/// For every wired operation pair and key relation, flat and fragment
/// metadata tree must agree on the verdict category (accept, retryable
/// conflict, incompatible transaction, invalid input) and, when both
/// accept, on the logical state.
#[rstest::rstest]
#[case::append_append(production_style_append(2), production_style_append(2))]
#[case::append_delete(production_style_append(2), delete_fragments([42]))]
#[case::append_replace(production_style_append(2), replace_base([42], 0))]
#[case::append_add_column(production_style_append(2), add_column([42], 0))]
#[case::delete_append(delete_fragments([42]), production_style_append(2))]
#[case::delete_same(delete_fragments([42]), delete_fragments([42]))]
#[case::delete_disjoint(delete_fragments([42]), delete_fragments([99]))]
#[case::delete_dv_same(delete_fragments([42]), set_deletion_file(make_fragment(42), 1))]
#[case::delete_dv_disjoint(delete_fragments([42]), set_deletion_file(make_fragment(99), 1))]
#[case::delete_replace_same(delete_fragments([42]), replace_base([42], 0))]
#[case::delete_replace_disjoint(delete_fragments([42]), replace_base([99], 0))]
#[case::delete_add_column_same(delete_fragments([42]), add_column([42], 0))]
#[case::delete_add_column_disjoint(delete_fragments([42]), add_column([99], 0))]
#[case::dv_delete_same(set_deletion_file(make_fragment(42), 1), delete_fragments([42]))]
#[case::dv_same(
    set_deletion_file(make_fragment(42), 1),
    set_deletion_file(make_fragment(42), 1)
)]
#[case::dv_disjoint(
    set_deletion_file(make_fragment(42), 1),
    set_deletion_file(make_fragment(99), 1)
)]
#[case::dv_replace_same(set_deletion_file(make_fragment(42), 1), replace_base([42], 0))]
#[case::replace_append(replace_base([42], 0), production_style_append(2))]
#[case::replace_delete_same(replace_base([42], 0), delete_fragments([42]))]
#[case::replace_delete_disjoint(replace_base([42], 0), delete_fragments([99]))]
#[case::replace_dv_same(replace_base([42], 0), set_deletion_file(make_fragment(42), 1))]
#[case::replace_same_fragment_same_fields(replace_base([42], 0), replace_base([42], 1))]
#[case::replace_disjoint_fragments(replace_base([42], 0), replace_base([99], 1))]
#[case::replace_add_column_disjoint_fields(replace_base([42], 0), add_column([42], 0))]
#[case::add_column_delete_same(add_column([42], 0), delete_fragments([42]))]
#[case::add_column_replace_disjoint_fields(add_column([42], 0), replace_base([42], 0))]
#[case::add_column_same_field(add_column([42], 0), add_column([43], 0))]
#[case::add_column_other_field(add_column([42], 0), add_column([42], 1))]
#[case::add_column_disjoint_fragments(add_column([42], 0), add_column([99], 0))]
#[tokio::test]
async fn conflict_matrix_matches_flat_for_every_wired_pair(
    #[case] a: Operation,
    #[case] b: Operation,
) {
    let mut diff = Differential::create(100).await;
    let base = diff.flat_state().await.version;
    let first = diff.apply("writer A", base, a).await;
    assert!(
        first.both_accepted_and_equal(),
        "writer A must land: flat={:?} fragment_metadata={:?} diff={:?}",
        first.flat.as_ref().err(),
        first.fragment_metadata.as_ref().err(),
        first.diff
    );
    let committed = diff.flat_state().await;
    let second = diff.apply("stale writer B", base, b).await;
    let flat_verdict = second
        .flat
        .as_ref()
        .err()
        .map(super::differential::error_category);
    let fragment_metadata_verdict = second
        .fragment_metadata
        .as_ref()
        .err()
        .map(super::differential::error_category);
    assert_eq!(flat_verdict, fragment_metadata_verdict, "{:?}", second.diff);
    match (&second.flat, &second.fragment_metadata) {
        (Ok(_), Ok(_)) => assert_eq!(second.parity, Some(true), "{:?}", second.diff),
        (Err(_), Err(_)) => {
            assert_eq!(
                diff.flat_state().await,
                committed,
                "Flat rejection changed state"
            );
            assert_eq!(
                diff.fragment_metadata_state().await,
                committed,
                "Tree rejection changed state"
            );
        }
        _ => panic!(
            "Writer verdicts differ: flat={:?}, fragment_metadata={:?}",
            second.flat, second.fragment_metadata
        ),
    }
}

#[tokio::test]
async fn stale_index_coverage_uses_the_original_read_version() {
    use lance_table::system_index::mem_wal::{
        CompactedSsTable, MEM_WAL_INDEX_NAME, MemWalIndexDetails, load_mem_wal_index_details,
        new_mem_wal_index_meta,
    };
    let mut diff = Differential::create(2).await;
    let shard = uuid::Uuid::new_v4();
    let wal = new_mem_wal_index_meta(
        1,
        MemWalIndexDetails {
            compacted_sstables: vec![CompactedSsTable::new(shard, 5)],
            ..Default::default()
        },
    )
    .unwrap();
    let outcome = diff
        .apply_latest(
            "wal",
            Operation::CreateIndex {
                new_indices: vec![wal],
                removed_indices: Vec::new(),
            },
        )
        .await;
    assert!(outcome.both_accepted_and_equal(), "{:?}", outcome.diff);
    let read_version = diff.flat_state().await.version;
    let outcome = diff
        .apply_latest(
            "append",
            Operation::Append {
                fragments: vec![make_fragment(0)],
            },
        )
        .await;
    assert!(outcome.both_accepted_and_equal(), "{:?}", outcome.diff);
    let index = lance_table::format::IndexMetadata {
        uuid: uuid::Uuid::new_v4(),
        name: "id_idx".into(),
        fields: vec![0],
        covering_fields: Vec::new(),
        dataset_version: read_version,
        fragment_bitmap: Some([0, 1].into_iter().collect()),
        index_details: Some(std::sync::Arc::new(prost_types::Any {
            type_url: "type.googleapis.com/lance.index.BTreeIndexDetails".into(),
            value: Vec::new(),
        })),
        index_version: 0,
        created_at: None,
        base_id: None,
        files: Some(Vec::new()),
    };
    let outcome = diff
        .apply(
            "index_after_append",
            read_version,
            Operation::CreateIndex {
                new_indices: vec![index],
                removed_indices: Vec::new(),
            },
        )
        .await;
    assert!(
        outcome.both_accepted_and_equal(),
        "flat={:?}, tree={:?}, diff={:?}",
        outcome.flat,
        outcome.fragment_metadata,
        outcome.diff
    );
    let mut details = Vec::new();
    for uri in [&diff.flat.uri, &diff.fragment_metadata.uri] {
        let dataset = crate::Dataset::open(uri).await.unwrap();
        let indices = crate::index::load_all_indices(&dataset).await.unwrap();
        let wal = indices
            .iter()
            .find(|index| index.name == MEM_WAL_INDEX_NAME)
            .unwrap();
        details.push(load_mem_wal_index_details(wal.clone()).unwrap());
    }
    assert_eq!(details[0], details[1]);
    let catchup = details[0]
        .index_catchup
        .iter()
        .find(|entry| entry.index_name == "id_idx")
        .unwrap();
    assert_eq!(
        catchup.caught_up_generations,
        vec![CompactedSsTable::new(shard, 5)]
    );
}
