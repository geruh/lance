// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! One real Lance scan through the lazy fragment source, compared against
//! the same data in a flat dataset, plus the bounds a lazily opened table
//! must keep at scale.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use datafusion::common::stats::Precision;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use lance_core::datatypes::Schema;
use lance_file::version::LanceFileVersion;
use lance_index::{IndexType, scalar::ScalarIndexParams};
use lance_io::object_store::ObjectStoreParams;
use lance_io::utils::tracking_store::IOTracker;
use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
use lance_table::format::pb::{fragment_action::Action, fragment_tree::Root};
use lance_table::fragment_metadata::support::make_fragment;
use lance_table::fragment_metadata::{FragmentTreeConfig, SnapshotPolicy};
use lance_table::io::deletion::deletion_file_path;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use rstest::rstest;

use super::{FragmentMetadataOptions, MAX_LEAF_BYTES_KEY, MAX_NODE_BYTES_KEY, Materialization};
use crate::dataset::builder::DatasetBuilder;
use crate::dataset::cleanup::{CleanupPolicy, cleanup_old_versions};
use crate::dataset::optimize::{CompactionOptions, compact_files};
use crate::dataset::transaction::{DataOverlayGroup, Operation, Transaction};
use crate::dataset::write::CommitBuilder;
use crate::dataset::write::update::UpdateBuilder;
use crate::dataset::{
    Dataset, InsertBuilder, NewColumnTransform, ReadParams, WriteMode, WriteParams,
};
use crate::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use crate::index::DatasetIndexExt;
use crate::io::exec::{LanceScanConfig, LanceScanExec};
use lance_table::fragment_metadata::MANIFEST_LAYOUT_KEY;

fn rows(start: i32, count: i32) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(start..start + count)),
            Arc::new(StringArray::from_iter_values(
                (start..start + count).map(|i| format!("row-{i}")),
            )),
        ],
    )
    .unwrap()
}

async fn write_flat(uri: &str, start: i32, count: i32, mode: WriteMode) -> Dataset {
    let batch = rows(start, count);
    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
    Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            max_rows_per_file: 10,
            max_rows_per_group: 10,
            mode,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
}

/// Copy the flat dataset's data and deletion files next to the tree layout so
/// both datasets read the same bytes.
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    if !from.exists() {
        return;
    }
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[rstest]
#[case::inline(64 * 1024)]
#[case::external(0)]
#[tokio::test]
async fn column_replacement_reuses_metadata_leaves(#[case] inline_root_bytes: usize) {
    let flat_dir = tempfile::tempdir().unwrap();
    let tree_dir = tempfile::tempdir().unwrap();
    let mut flat = write_flat(flat_dir.path().to_str().unwrap(), 0, 20, WriteMode::Create).await;
    flat.add_columns(
        NewColumnTransform::SqlExpressions(vec![("double_id".into(), "id * 2".into())]),
        None,
        None,
    )
    .await
    .unwrap();
    copy_dir(&flat_dir.path().join("data"), &tree_dir.path().join("data"));
    let options = super::FragmentMetadataOptions {
        publication: lance_table::fragment_metadata::SnapshotPolicy {
            inline_root_bytes,
            ..Default::default()
        },
        ..Default::default()
    };
    let dataset = CommitBuilder::new(tree_dir.path().to_str().unwrap())
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: flat.fragments().to_vec(),
                schema: flat.schema().clone(),
                config_upsert_values: Some(options.into_table_config().unwrap()),
                initial_bases: None,
            },
        ))
        .await
        .unwrap();
    let old = dataset.get_fragment(0).unwrap().metadata().clone();
    assert_eq!(old.files.len(), 2);
    assert_eq!(dataset.fragments().len(), 2);
    let schema = lance_core::datatypes::Schema {
        fields: vec![dataset.schema().field("double_id").unwrap().clone()],
        metadata: Default::default(),
    };
    let batch =
        arrow_array::record_batch!(("double_id", Int32, [0, 3, 6, 9, 12, 15, 18, 21, 24, 27]))
            .unwrap();
    let replacement = dataset
        .get_fragment(0)
        .unwrap()
        .write_columns(futures::stream::iter([Ok(batch)]), &schema)
        .await
        .unwrap();
    let replacement_path = replacement.1.path.clone();
    let before = super::publication::descriptor(&dataset.manifest).unwrap();
    let leaf_count = std::fs::read_dir(tree_dir.path().join("_bt/leaf"))
        .unwrap()
        .count();
    let updated = CommitBuilder::new(Arc::new(dataset.clone()))
        .execute(Transaction::new_from_version(
            dataset.version_id(),
            Operation::DataReplacement {
                replacements: vec![replacement],
            },
        ))
        .await
        .unwrap();
    let after = super::publication::descriptor(&updated.manifest).unwrap();
    let pending = match (&before.root, &after.root) {
        (Some(Root::InlineRoot(old)), Some(Root::InlineRoot(new))) => {
            assert_eq!(old.children, new.children);
            &new.buffer
        }
        (Some(Root::RootUuid(old)), Some(Root::RootUuid(new))) => {
            assert_eq!(old, new);
            &after.mutations_since_root
        }
        _ => panic!("replacement should keep the same publication layout"),
    };
    assert_eq!(pending.len(), 1);
    assert!(matches!(
        pending[0].action.as_ref().unwrap().action,
        Some(Action::ReplaceDataFile(_))
    ));
    assert_eq!(
        std::fs::read_dir(tree_dir.path().join("_bt/leaf"))
            .unwrap()
            .count(),
        leaf_count
    );
    let current = updated.get_fragment(0).unwrap().metadata().clone();
    assert_eq!(current.files[0], old.files[0]);
    assert_eq!(current.files[1].path, replacement_path);
    assert_eq!(current.files[1].fields, old.files[1].fields);
    assert_eq!(current.files[1].column_indices, old.files[1].column_indices);
    assert_eq!(updated.fragments()[1], dataset.fragments()[1]);
    assert_eq!(
        updated
            .count_rows(Some("id < 10 AND double_id != id * 3".into()))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        updated
            .count_rows(Some("id >= 10 AND double_id != id * 2".into()))
            .await
            .unwrap(),
        0
    );
    let historical = DatasetBuilder::from_uri(tree_dir.path().to_str().unwrap())
        .with_version(dataset.version_id())
        .load()
        .await
        .unwrap();
    assert_eq!(
        historical
            .count_rows(Some("double_id != id * 2".into()))
            .await
            .unwrap(),
        0
    );
}

/// A fragment metadata dataset holding exactly the flat dataset's fragments, over copies
/// of its files.
async fn mirror_into_fragment_metadata(
    flat: &Dataset,
    flat_dir: &std::path::Path,
    fragment_metadata_uri: &str,
    materialization: &str,
) {
    for sub in ["data", "_deletions"] {
        copy_dir(
            &flat_dir.join(sub),
            &std::path::Path::new(fragment_metadata_uri).join(sub),
        );
    }
    let config = HashMap::from([
        (
            MANIFEST_LAYOUT_KEY.to_string(),
            lance_table::fragment_metadata::MANIFEST_LAYOUT_TREE.to_string(),
        ),
        (MAX_NODE_BYTES_KEY.to_string(), (2 * 1024).to_string()),
        (
            "lance.fragment_metadata.materialization".to_string(),
            materialization.to_string(),
        ),
    ]);
    let mut fragments = flat.fragments().to_vec();
    if flat.manifest.uses_stable_row_ids() {
        for fragment in &mut fragments {
            fragment.row_id_meta = None;
            fragment.created_at_version_meta = None;
            fragment.last_updated_at_version_meta = None;
        }
    }
    let dataset = CommitBuilder::new(fragment_metadata_uri)
        .use_stable_row_ids(flat.manifest.uses_stable_row_ids())
        .with_storage_format(
            flat.manifest
                .data_storage_format
                .lance_file_format()
                .to_selector(),
        )
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: Vec::new(),
                schema: flat.schema().clone(),
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ))
        .await
        .unwrap();
    let deletions: HashMap<_, _> = fragments
        .iter()
        .filter_map(|fragment| {
            fragment
                .deletion_file
                .clone()
                .map(|deletion| (fragment.id, deletion))
        })
        .collect();
    for fragment in &mut fragments {
        fragment.deletion_file = None;
    }
    let dataset = CommitBuilder::new(Arc::new(dataset))
        .execute(Transaction::new_from_version(
            1,
            Operation::Append { fragments },
        ))
        .await
        .unwrap();
    let updated_fragments: Vec<_> = dataset
        .fragments()
        .iter()
        .filter_map(|fragment| {
            deletions.get(&fragment.id).map(|deletion| {
                let mut fragment = fragment.clone();
                fragment.deletion_file = Some(deletion.clone());
                fragment
            })
        })
        .collect();
    if !updated_fragments.is_empty() {
        CommitBuilder::new(Arc::new(dataset))
            .execute(Transaction::new_from_version(
                2,
                Operation::Delete {
                    updated_fragments,
                    deleted_fragment_ids: Vec::new(),
                    predicate: "fixture".into(),
                },
            ))
            .await
            .unwrap();
    }
}

async fn scan_sorted(
    dataset: &Dataset,
    columns: Option<&[&str]>,
    filter: Option<&str>,
) -> Vec<String> {
    let mut scanner = dataset.scan();
    if let Some(columns) = columns {
        scanner.project(columns).unwrap();
    }
    if let Some(filter) = filter {
        scanner.filter(filter).unwrap();
    }
    let batches = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut lines = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for column in batch.columns() {
                cells.push(arrow_cast::display::array_value_to_string(column, row).unwrap());
            }
            lines.push(cells.join("|"));
        }
    }
    lines.sort();
    lines
}

#[rstest]
#[tokio::test]
async fn legacy_data_files_stream_through_lazy_metadata(#[values(true, false)] ordered: bool) {
    let flat_dir = tempfile::tempdir().unwrap();
    let tree_dir = tempfile::tempdir().unwrap();
    let batch = rows(0, 60);
    let mut flat = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        flat_dir.path().to_str().unwrap(),
        Some(WriteParams {
            max_rows_per_file: 10,
            max_rows_per_group: 10,
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    flat.delete("id < 5").await.unwrap();
    mirror_into_fragment_metadata(
        &flat,
        flat_dir.path(),
        tree_dir.path().to_str().unwrap(),
        "buffered",
    )
    .await;
    let tree = DatasetBuilder::from_uri(tree_dir.path().to_str().unwrap())
        .load_unmaterialized()
        .await
        .unwrap();
    assert!(tree.fragment_source().is_lazy());
    assert_eq!(tree.fragment_source().row_count().unwrap(), 55);
    assert_eq!(tree.count_rows(None).await.unwrap(), 55);
    assert_eq!(tree.count_deleted_rows().await.unwrap(), 5);
    assert_eq!(
        tree.filter_deleted_ids(&[0, 5, 1_u64 << 32]).await.unwrap(),
        vec![5, 1_u64 << 32]
    );
    tree.validate().await.unwrap();
    for retain_deleted in [false, true] {
        let plan = LanceScanExec::new(
            Arc::new(tree.clone()),
            tree.fragment_source(),
            None,
            Arc::new(tree.schema().clone()),
            LanceScanConfig {
                with_make_deletions_null: retain_deleted,
                ..Default::default()
            },
        );
        assert_eq!(
            plan.partition_statistics(None).unwrap().num_rows,
            if retain_deleted {
                Precision::Absent
            } else {
                Precision::Exact(55)
            }
        );
    }
    let batch = tree
        .scan()
        .scan_in_order(ordered)
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 55);
    assert_eq!(
        scan_sorted(&tree, None, None).await,
        scan_sorted(&flat, None, None).await
    );
    let fragments = tree.get_fragments_async().await.unwrap();
    let missing = &fragments.last().unwrap().metadata().files[0].path;
    std::fs::remove_file(tree_dir.path().join("data").join(missing)).unwrap();
    let reopened = DatasetBuilder::from_uri(tree_dir.path().to_str().unwrap())
        .load_unmaterialized()
        .await
        .unwrap();
    let error = reopened.validate().await.unwrap_err();
    assert!(matches!(error, lance_core::Error::NotFound { .. }));
    assert!(error.to_string().contains(missing));
}

#[rstest]
#[tokio::test]
async fn native_clone_rows_and_branch_retention(#[values("shallow", "deep", "branch")] mode: &str) {
    let flat_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let clone_dir = tempfile::tempdir().unwrap();
    let flat = write_flat(flat_dir.path().to_str().unwrap(), 0, 60, WriteMode::Create).await;
    mirror_into_fragment_metadata(
        &flat,
        flat_dir.path(),
        source_dir.path().to_str().unwrap(),
        "bulk",
    )
    .await;
    let mut source = Dataset::open(source_dir.path().to_str().unwrap())
        .await
        .unwrap();
    let original_fragment = source.get_fragments_async().await.unwrap()[0]
        .metadata()
        .clone();
    let mut overlay_file = original_fragment.files[0].clone();
    overlay_file.path = "clone-overlay.lance".to_string();
    source
        .object_store
        .copy(
            &source
                .base
                .clone()
                .join("data")
                .join(original_fragment.files[0].path.as_str()),
            &source
                .base
                .clone()
                .join("data")
                .join(overlay_file.path.as_str()),
        )
        .await
        .unwrap();
    source = CommitBuilder::new(Arc::new(source.clone()))
        .execute(Transaction::new_from_version(
            source.version().version,
            Operation::DataOverlay {
                groups: vec![DataOverlayGroup {
                    fragment_id: original_fragment.id,
                    overlays: vec![DataOverlayFile {
                        data_file: overlay_file,
                        coverage: OverlayCoverage::Shared(Arc::new((0..10).collect())),
                        committed_version: 0,
                    }],
                }],
            },
        ))
        .await
        .unwrap();
    source.delete("id < 5").await.unwrap();
    let original = scan_sorted(&source, None, None).await;
    assert_eq!(original.len(), 55);
    let version = source.version().version;
    let mut cloned = match mode {
        "shallow" => source
            .shallow_clone(clone_dir.path().to_str().unwrap(), version, None)
            .await
            .unwrap(),
        "deep" => source
            .deep_clone(clone_dir.path().to_str().unwrap(), version, None)
            .await
            .unwrap(),
        "branch" => source
            .create_branch("metadata-test", version, None)
            .await
            .unwrap(),
        _ => unreachable!(),
    };
    assert!(cloned.manifest.fragment_tree.is_some());
    assert_eq!(scan_sorted(&cloned, None, None).await, original);
    let clone_version = cloned.version().version;
    cloned.delete("id >= 55").await.unwrap();
    assert_eq!(cloned.count_rows(None).await.unwrap(), 50);
    assert_eq!(scan_sorted(&source, None, None).await, original);
    assert_eq!(
        scan_sorted(
            &cloned.checkout_version(clone_version).await.unwrap(),
            None,
            None
        )
        .await,
        original
    );
    source.delete("id >= 5").await.unwrap();
    if mode != "shallow" {
        crate::dataset::cleanup::cleanup_old_versions(
            &source,
            crate::dataset::cleanup::CleanupPolicy {
                before_version: Some(source.version().version),
                delete_unverified: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(scan_sorted(&cloned, None, None).await.len(), 50);
    }
    let reopened = Dataset::open(cloned.uri()).await.unwrap();
    assert_eq!(scan_sorted(&reopened, None, None).await.len(), 50);
}

/// An empty tree layout table whose leaves and routing nodes stay small
/// enough that a few dozen four row fragments cross every budget. The root is
/// always external so its retention is visible under cleanup, and buffered
/// materialization rewrites touched leaves in place of a full rebuild.
async fn create_small_budget_table(uri: &str) -> Dataset {
    let mut tree = FragmentTreeConfig::default();
    tree.max_node_bytes = 1024;
    tree.max_leaf_bytes = 512;
    tree.semantic_buffer_bytes = 512;
    let config = FragmentMetadataOptions {
        tree,
        publication: SnapshotPolicy {
            inline_root_bytes: 0,
            ..SnapshotPolicy::default()
        },
        materialization: Materialization::Buffered,
    }
    .into_table_config()
    .unwrap();
    CommitBuilder::new(uri)
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: Vec::new(),
                schema: Schema::try_from(rows(0, 1).schema().as_ref()).unwrap(),
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ))
        .await
        .unwrap()
}

/// Append `count` rows through the production insert path, four rows per
/// fragment.
async fn append_rows(dataset: &Dataset, start: i32, count: i32) -> Dataset {
    let params = WriteParams {
        max_rows_per_file: 4,
        max_rows_per_group: 4,
        mode: WriteMode::Append,
        ..Default::default()
    };
    InsertBuilder::new(Arc::new(dataset.clone()))
        .with_params(&params)
        .execute(vec![rows(start, count)])
        .await
        .unwrap()
}

/// Node paths the dataset's own tree references at `version`, read through a
/// fresh lazy open so a hydrated handle cannot mask a missing object.
async fn local_node_paths_at(uri: &str, version: Option<u64>) -> BTreeSet<String> {
    let mut builder = DatasetBuilder::from_uri(uri);
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    let dataset = builder.load_unmaterialized().await.unwrap();
    dataset
        .lazy_fragments
        .as_ref()
        .unwrap()
        .tree()
        .local_node_paths()
        .await
        .unwrap()
        .into_iter()
        .collect()
}

/// A branch still using nodes its parent has replaced.
struct BranchOverAbandonedNodes {
    parent: Dataset,
    branch: Dataset,
    clone_version: u64,
    rows_at_clone: Vec<String>,
    at_clone: BTreeSet<String>,
    now: BTreeSet<String>,
    /// Nodes still needed by the branch, but no longer by the parent head.
    abandoned: BTreeSet<String>,
}

impl BranchOverAbandonedNodes {
    async fn create(parent_uri: &str, branch: &str) -> Self {
        let mut parent = create_small_budget_table(parent_uri).await;
        // The branch must inherit both leaves and interior nodes.
        parent = append_rows(&parent, 0, 32).await;
        parent = append_rows(&parent, 32, 32).await;
        let clone_version = parent.version_id();
        let rows_at_clone = scan_sorted(&parent, Some(&["id"]), None).await;
        let branch = parent
            .create_branch(branch, clone_version, None)
            .await
            .unwrap();
        assert_eq!(
            scan_sorted(&branch, Some(&["id"]), None).await,
            rows_at_clone
        );

        // Replace inherited nodes while the branch still references them.
        parent.delete("id % 2 = 0").await.unwrap();
        parent = append_rows(&parent, 64, 32).await;
        let at_clone = local_node_paths_at(parent_uri, Some(clone_version)).await;
        let now = local_node_paths_at(parent_uri, None).await;
        let abandoned: BTreeSet<String> = at_clone.difference(&now).cloned().collect();
        assert!(
            abandoned.iter().any(|path| path.starts_with("_bt/node/")),
            "the parent must have rewritten an interior node the branch still routes through: {abandoned:?}"
        );
        assert!(
            abandoned.iter().any(|path| path.starts_with("_bt/leaf/")),
            "the parent must have rewritten a leaf the branch still routes through: {abandoned:?}"
        );
        Self {
            parent,
            branch,
            clone_version,
            rows_at_clone,
            at_clone,
            now,
            abandoned,
        }
    }

    async fn assert_branch_survived_parent_cleanup(&self) {
        for path in &self.abandoned {
            let object = Path::from(format!("{}/{path}", self.parent.base));
            assert!(
                self.parent.object_store.inner.head(&object).await.is_ok(),
                "{path} is reachable from the branch and must survive the parent's cleanup"
            );
        }
        let reopened = DatasetBuilder::from_uri(self.branch.uri())
            .load_unmaterialized()
            .await
            .unwrap();
        assert_eq!(reopened.version_id(), self.branch.version_id());
        reopened
            .lazy_fragments
            .as_ref()
            .unwrap()
            .tree()
            .verify_reachable()
            .await
            .unwrap();
        assert_eq!(
            scan_sorted(&reopened, Some(&["id"]), None).await,
            self.rows_at_clone
        );
    }
}

// Retain the fork's root and inherited nodes; remove other abandoned nodes.
#[tokio::test]
async fn branch_left_at_its_clone_version_survives_parent_cleanup_of_abandoned_nodes() {
    let parent_dir = tempfile::tempdir().unwrap();
    let parent_uri = parent_dir.path().to_str().unwrap();
    let fixture = BranchOverAbandonedNodes::create(parent_uri, "left-at-clone-version").await;
    let parent = &fixture.parent;

    cleanup_old_versions(
        parent,
        CleanupPolicy {
            before_version: Some(parent.version_id()),
            delete_unverified: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Only the parent head and retained fork may keep tree objects alive.
    let forked = parent
        .checkout_version(fixture.clone_version)
        .await
        .unwrap();
    let mut expected: BTreeSet<String> = fixture.now.union(&fixture.at_clone).cloned().collect();
    for manifest in [&parent.manifest, &forked.manifest] {
        let Some(Root::RootUuid(root)) = &manifest.fragment_tree.as_ref().unwrap().root else {
            panic!("a small budget table publishes its root as an object");
        };
        expected.insert(lance_table::fragment_metadata::store::root_path(root).unwrap());
    }
    let survivors: BTreeSet<String> = parent
        .object_store
        .inner
        .list(Some(&Path::from(format!("{}/_bt", parent.base))))
        .map_ok(|object| {
            object
                .location
                .as_ref()
                .strip_prefix(parent.base.as_ref())
                .unwrap()
                .trim_start_matches('/')
                .to_string()
        })
        .try_collect()
        .await
        .unwrap();
    assert_eq!(survivors, expected);
    assert_eq!(
        scan_sorted(&forked, Some(&["id"]), None).await,
        fixture.rows_at_clone
    );
    fixture.assert_branch_survived_parent_cleanup().await;
}

// Node ownership must use the resolved location, not the recorded URI spelling.
#[tokio::test]
async fn branch_recorded_under_another_spelling_of_the_parent_uri_keeps_parent_nodes() {
    let parent_dir = tempfile::tempdir().unwrap();
    let parent_uri = parent_dir.path().to_str().unwrap();
    let fixture = BranchOverAbandonedNodes::create(parent_uri, "recorded-under-plain-path").await;
    let recorded_parent_uri = fixture
        .branch
        .manifest
        .base_paths
        .values()
        .find(|base| base.is_dataset_root)
        .map(|base| base.path.as_str())
        .expect("a branch names its parent as a dataset root base");
    assert_eq!(recorded_parent_uri, parent_uri);

    let respelled = DatasetBuilder::from_uri(format!("file://{parent_uri}"))
        .load()
        .await
        .unwrap();
    assert_ne!(respelled.uri(), recorded_parent_uri);
    assert_eq!(respelled.base, fixture.parent.base);
    assert_eq!(respelled.version_id(), fixture.parent.version_id());
    cleanup_old_versions(
        &respelled,
        CleanupPolicy {
            before_version: Some(respelled.version_id()),
            delete_unverified: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    fixture.assert_branch_survived_parent_cleanup().await;
}

#[tokio::test]
async fn retained_fork_keeps_files_no_surviving_branch_version_uses() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut parent = create_small_budget_table(uri).await;
    parent = append_rows(&parent, 0, 8).await;
    parent.delete("id = 0").await.unwrap();
    parent
        .create_index(
            &["id"],
            IndexType::Scalar,
            Some("id_idx".to_string()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    let fork_version = parent.version_id();
    let fork_rows = scan_sorted(&parent, Some(&["id"]), None).await;
    let fragment = &parent.fragments()[0];
    let data_path = parent.data_dir().join(fragment.files[0].path.as_str());
    let deletion_path = deletion_file_path(
        &parent.base,
        fragment.id,
        fragment.deletion_file.as_ref().unwrap(),
    );
    let index_paths = parent
        .object_store
        .inner
        .list(Some(&parent.indices_dir()))
        .map_ok(|object| object.location)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(!index_paths.is_empty());
    let mut branch = parent
        .create_branch("advanced", fork_version, None)
        .await
        .unwrap();
    let branch_fork_version = branch.version_id();

    // Both heads stop using one fragment and the index. After branch cleanup,
    // only the retained parent fork still needs those data and deletion files.
    for dataset in [&mut parent, &mut branch] {
        dataset.delete("id < 4").await.unwrap();
        dataset.drop_index("id_idx").await.unwrap();
        assert_eq!(dataset.count_fragments(), 1);
    }
    cleanup_old_versions(
        &branch,
        CleanupPolicy {
            before_version: Some(branch.version_id()),
            delete_unverified: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        branch
            .versions()
            .await
            .unwrap()
            .iter()
            .all(|version| version.version != branch_fork_version)
    );
    cleanup_old_versions(
        &parent,
        CleanupPolicy {
            before_version: Some(parent.version_id()),
            delete_unverified: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let fork = DatasetBuilder::from_uri(uri)
        .with_version(fork_version)
        .load_unmaterialized()
        .await
        .unwrap();
    for path in [data_path, deletion_path].into_iter().chain(index_paths) {
        assert!(
            parent.object_store.inner.head(&path).await.is_ok(),
            "retained fork {fork_version} still references {path}"
        );
    }
    assert_eq!(scan_sorted(&fork, Some(&["id"]), None).await, fork_rows);
    assert_eq!(
        scan_sorted(&fork, Some(&["id"]), Some("id >= 0")).await,
        fork_rows
    );
}

fn tree_root_path(dataset: &Dataset) -> String {
    let Some(Root::RootUuid(root)) = &dataset.manifest.fragment_tree.as_ref().unwrap().root else {
        panic!("a small budget table publishes its root as an object");
    };
    lance_table::fragment_metadata::store::root_path(root).unwrap()
}

async fn tree_object_paths(dataset: &Dataset) -> BTreeSet<String> {
    dataset
        .object_store
        .inner
        .list(Some(&Path::from(format!("{}/_bt", dataset.base))))
        .map_ok(|object| {
            object
                .location
                .as_ref()
                .strip_prefix(dataset.base.as_ref())
                .unwrap()
                .trim_start_matches('/')
                .to_string()
        })
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn fork_the_branch_no_longer_needs_is_removed_with_its_files() {
    let parent_dir = tempfile::tempdir().unwrap();
    let parent_uri = parent_dir.path().to_str().unwrap();
    let fixture = BranchOverAbandonedNodes::create(parent_uri, "rewrites-everything").await;
    let mut parent = fixture.parent.clone();
    let mut branch = fixture.branch.clone();
    let forked = parent
        .checkout_version(fixture.clone_version)
        .await
        .unwrap();
    let fork_root = tree_root_path(&forked);
    let first_fragment = parent
        .fragments()
        .iter()
        .find(|fragment| fragment.id == 0)
        .unwrap();
    let fork_data_path = parent
        .data_dir()
        .join(first_fragment.files[0].path.as_str());

    // The config change uses the old policy; the delete runs in bulk mode.
    branch
        .update_config([("lance.fragment_metadata.materialization", "bulk")])
        .await
        .unwrap();
    branch.delete("true").await.unwrap();
    branch = append_rows(&branch, 96, 32).await;
    let branch_rows = scan_sorted(&branch, Some(&["id"]), None).await;
    cleanup_old_versions(
        &branch,
        CleanupPolicy {
            before_version: Some(branch.version_id()),
            delete_unverified: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let branch_versions: Vec<u64> = branch
        .versions()
        .await
        .unwrap()
        .iter()
        .map(|version| version.version)
        .collect();
    assert_eq!(branch_versions, vec![branch.version_id()]);
    assert!(
        branch
            .fragments()
            .iter()
            .flat_map(|fragment| &fragment.files)
            .all(|file| file.base_id.is_none()),
        "the surviving branch version must own every data file it reads"
    );
    let rewritten = DatasetBuilder::from_uri(branch.uri())
        .load_unmaterialized()
        .await
        .unwrap();
    let inherited = rewritten
        .lazy_fragments
        .as_ref()
        .unwrap()
        .tree()
        .node_paths_owned_by(&parent.object_store, &parent.base)
        .await
        .unwrap();
    assert!(
        inherited.is_empty(),
        "the surviving branch version must own every node it routes through: {inherited:?}"
    );

    // Leave this data file referenced only by the fork manifest.
    parent.delete("id < 4").await.unwrap();
    assert_eq!(parent.count_fragments(), 23);
    let parent_rows = scan_sorted(&parent, Some(&["id"]), None).await;
    let now = local_node_paths_at(parent_uri, None).await;
    cleanup_old_versions(
        &parent,
        CleanupPolicy {
            before_version: Some(parent.version_id()),
            delete_unverified: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let parent_versions: Vec<u64> = parent
        .versions()
        .await
        .unwrap()
        .iter()
        .map(|version| version.version)
        .collect();
    assert_eq!(parent_versions, vec![parent.version_id()]);
    assert!(
        matches!(
            parent.object_store.inner.head(&fork_data_path).await,
            Err(object_store::Error::NotFound { .. })
        ),
        "{fork_data_path} was named only by the removed fork {}",
        fixture.clone_version
    );
    let mut expected = now;
    expected.insert(tree_root_path(&parent));
    assert!(!expected.contains(&fork_root));
    assert_eq!(tree_object_paths(&parent).await, expected);
    assert_eq!(scan_sorted(&parent, Some(&["id"]), None).await, parent_rows);
    let reopened = DatasetBuilder::from_uri(branch.uri())
        .load_unmaterialized()
        .await
        .unwrap();
    reopened
        .lazy_fragments
        .as_ref()
        .unwrap()
        .tree()
        .verify_reachable()
        .await
        .unwrap();
    assert_eq!(
        scan_sorted(&reopened, Some(&["id"]), None).await,
        branch_rows
    );
}

/// Every recorded version opens lazily, holds its watermarks and scans the
/// rows it held when it was published.
async fn assert_history_scans(uri: &str, history: &BTreeMap<u64, Vec<String>>) {
    for (version, rows) in history {
        let dataset = DatasetBuilder::from_uri(uri)
            .with_version(*version)
            .load_unmaterialized()
            .await
            .unwrap();
        let tree = dataset.lazy_fragments.as_ref().unwrap().tree();
        tree.verify_watermarks().await.unwrap();
        assert_eq!(
            &scan_sorted(&dataset, Some(&["id"]), None).await,
            rows,
            "version {version}"
        );
    }
}

/// Emptying and regrowing a tree preserves its ID frontier and older versions.
/// Regrowth splits a childless root whose first live ID is above zero.
#[tokio::test]
async fn emptying_every_fragment_keeps_the_frontier_and_history_then_regrows() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let mut dataset = create_small_budget_table(uri).await;
    let mut history = BTreeMap::new();
    for start in [0, 32, 64] {
        dataset = append_rows(&dataset, start, 32).await;
        history.insert(
            dataset.version_id(),
            scan_sorted(&dataset, Some(&["id"]), None).await,
        );
    }
    let frontier = dataset.manifest.max_fragment_id.unwrap();
    let last_id = u64::from(frontier);
    assert_eq!(dataset.count_fragments(), 24);
    assert_eq!(last_id, 23);

    dataset.delete("true").await.unwrap();
    history.insert(dataset.version_id(), Vec::new());
    assert_eq!(dataset.count_fragments(), 0);
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    assert!(dataset.fragments().is_empty());
    assert_eq!(
        dataset.manifest.max_fragment_id,
        Some(frontier),
        "the allocation frontier survives an empty table"
    );
    let emptied = DatasetBuilder::from_uri(uri)
        .load_unmaterialized()
        .await
        .unwrap();
    let tree = emptied.lazy_fragments.as_ref().unwrap().tree();
    assert_eq!(tree.count_fragments(), 0);
    assert_eq!(tree.next_fragment_id(), last_id + 1);
    tree.verify_reachable().await.unwrap();

    // The materialization mode is read from the manifest a commit starts
    // from, so the commit that switches to bulk still runs buffered. The
    // marker commit that follows is the first bulk drain of the empty table.
    dataset
        .update_config([("lance.fragment_metadata.materialization", "bulk")])
        .await
        .unwrap();
    history.insert(dataset.version_id(), Vec::new());
    dataset
        .update_config([("lifecycle.marker", "drain")])
        .await
        .unwrap();
    history.insert(dataset.version_id(), Vec::new());
    let drained = DatasetBuilder::from_uri(uri)
        .load_unmaterialized()
        .await
        .unwrap();
    let tree = drained.lazy_fragments.as_ref().unwrap().tree();
    let report = tree.shape_report().await.unwrap();
    assert!(
        report.leaf_object_bytes.is_empty(),
        "an emptied table keeps no leaves after a drain: {report:?}"
    );
    assert!(report.node_bytes.is_empty(), "{report:?}");
    assert_eq!(report.height, 1);
    assert!(tree.buffered_action_keys().await.unwrap().is_empty());
    assert_eq!(
        tree.next_fragment_id(),
        last_id + 1,
        "the allocation frontier survives the drain of an empty table"
    );

    // Twenty singleton leaves exceed the fanout and split the childless root.
    dataset
        .update_config([("lance.fragment_metadata.materialization", "buffered")])
        .await
        .unwrap();
    history.insert(dataset.version_id(), Vec::new());
    dataset = append_rows(&dataset, 96, 80).await;
    history.insert(
        dataset.version_id(),
        scan_sorted(&dataset, Some(&["id"]), None).await,
    );
    let regrown: Vec<u64> = dataset
        .fragments()
        .iter()
        .map(|fragment| fragment.id)
        .collect();
    assert_eq!(
        regrown,
        (last_id + 1..last_id + 21).collect::<Vec<_>>(),
        "new ids continue above the old frontier"
    );
    assert_eq!(dataset.manifest.max_fragment_id, Some(frontier + 20));
    let fresh = DatasetBuilder::from_uri(uri)
        .load_unmaterialized()
        .await
        .unwrap();
    let tree = fresh.lazy_fragments.as_ref().unwrap().tree();
    let regrown_shape = tree.shape_report().await.unwrap();
    assert!(
        regrown_shape.height >= 2,
        "the regrowth must split the root to cover the lifted leaves: {regrown_shape:?}"
    );
    tree.verify_reachable().await.unwrap();
    assert!(
        !regrown_shape.leaf_object_bytes.is_empty(),
        "regrowth writes leaves again"
    );
    for old in 0..=last_id {
        assert!(
            tree.resolve_fragment(old).await.unwrap().is_none(),
            "fragment {old} was deleted"
        );
    }
    for id in &regrown {
        let fragment = tree.resolve_fragment(*id).await.unwrap().unwrap();
        assert_eq!(fragment.physical_rows, Some(4));
    }
    assert_history_scans(uri, &history).await;
}

/// Projected values of the columns written by `rounds` on every row of
/// `dataset`, matched against the expression each round wrote, `id + round`.
async fn assert_backfilled_values(dataset: &Dataset, rounds: &[u32]) {
    let names: Vec<String> = std::iter::once("id".to_string())
        .chain(rounds.iter().map(|round| format!("c{round}")))
        .collect();
    let projected: Vec<&str> = names.iter().map(String::as_str).collect();
    let batch = dataset
        .scan()
        .project(&projected)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), dataset.count_rows(None).await.unwrap());
    let ids = batch["id"].as_primitive::<arrow_array::types::Int32Type>();
    for round in rounds {
        let values =
            batch[format!("c{round}").as_str()].as_primitive::<arrow_array::types::Int64Type>();
        for row in 0..batch.num_rows() {
            assert_eq!(
                values.value(row),
                i64::from(ids.value(row)) + i64::from(*round),
                "column c{round} row {}",
                ids.value(row)
            );
        }
    }
}

/// Backfill rounds kept small enough for a unit test.
const BACKFILL_ROUNDS: u32 = 16;

/// Repeated native backfills widen every fragment by one real file per
/// round. A fresh open projects the value each round wrote, and a compaction
/// afterwards folds the files into one fragment that projects the same
/// values.
#[tokio::test]
async fn repeated_native_backfills_widen_fragments_and_survive_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let dataset = create_small_budget_table(uri).await;
    let mut dataset = append_rows(&dataset, 0, 8).await;
    assert_eq!(dataset.count_fragments(), 2);
    let rounds: Vec<u32> = (0..BACKFILL_ROUNDS).collect();
    for round in &rounds {
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![(
                    format!("c{round}"),
                    format!("CAST(id AS BIGINT) + {round}"),
                )]),
                Some(vec!["id".to_string()]),
                None,
            )
            .await
            .unwrap();
    }
    let files_per_fragment = rounds.len() + 1;
    let mut checked = vec![
        rounds[0],
        rounds[rounds.len() / 2],
        rounds[rounds.len() - 1],
    ];
    checked.dedup();

    let reopened = Dataset::open(uri).await.unwrap();
    assert_eq!(reopened.count_fragments(), 2);
    for fragment in reopened.fragments().iter() {
        assert_eq!(
            fragment.files.len(),
            files_per_fragment,
            "fragment {}",
            fragment.id
        );
    }
    assert_backfilled_values(&reopened, &checked).await;
    let lazy = DatasetBuilder::from_uri(uri)
        .load_unmaterialized()
        .await
        .unwrap();
    let tree = lazy.lazy_fragments.as_ref().unwrap().tree();
    tree.verify_watermarks().await.unwrap();
    assert_eq!(
        tree.materialize().await.unwrap().as_slice(),
        reopened.fragments().as_slice()
    );
    assert_backfilled_values(&lazy, &checked).await;

    compact_files(
        &mut dataset,
        CompactionOptions {
            target_rows_per_fragment: 8,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let compacted = Dataset::open(uri).await.unwrap();
    assert_eq!(compacted.count_fragments(), 1);
    let fragment = &compacted.fragments()[0];
    assert!(fragment.id >= 2, "the rewrite takes a fresh id");
    assert_eq!(
        fragment.files.len(),
        1,
        "compaction folds every column into one file"
    );
    assert_eq!(
        fragment.files[0].fields.len(),
        files_per_fragment + 1,
        "the rewritten file covers id, name and every backfilled column"
    );
    assert_backfilled_values(&compacted, &checked).await;
    let lazy = DatasetBuilder::from_uri(uri)
        .load_unmaterialized()
        .await
        .unwrap();
    assert_backfilled_values(&lazy, &checked).await;
}

#[tokio::test]
async fn deep_clone_rebuilds_a_multi_level_tree() {
    let flat_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let clone_dir = tempfile::tempdir().unwrap();
    let flat = write_flat(flat_dir.path().to_str().unwrap(), 0, 400, WriteMode::Create).await;
    copy_dir(
        &flat_dir.path().join("data"),
        &source_dir.path().join("data"),
    );
    // Small budgets force nested interior nodes with forty fragments.
    let table_config = HashMap::from([
        (
            MANIFEST_LAYOUT_KEY.to_string(),
            lance_table::fragment_metadata::MANIFEST_LAYOUT_TREE.to_string(),
        ),
        (MAX_NODE_BYTES_KEY.to_string(), "512".to_string()),
        (MAX_LEAF_BYTES_KEY.to_string(), "256".to_string()),
    ]);
    CommitBuilder::new(source_dir.path().to_str().unwrap())
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: flat.fragments().to_vec(),
                schema: flat.schema().clone(),
                config_upsert_values: Some(table_config),
                initial_bases: None,
            },
        ))
        .await
        .unwrap();
    let mut source = DatasetBuilder::from_uri(source_dir.path().to_str().unwrap())
        .load_unmaterialized()
        .await
        .unwrap();
    let source_height = source.lazy_fragments.as_ref().unwrap().tree().height();
    assert!(
        source_height >= 3,
        "expected nested interior nodes, got height {source_height}"
    );
    let expected = scan_sorted(&source, None, None).await;
    assert_eq!(expected.len(), 400);
    let version = source.version().version;
    let cloned = source
        .deep_clone(clone_dir.path().to_str().unwrap(), version, None)
        .await
        .unwrap();
    assert_eq!(scan_sorted(&cloned, None, None).await, expected);
    let reopened = DatasetBuilder::from_uri(cloned.uri())
        .load_unmaterialized()
        .await
        .unwrap();
    let clone_height = reopened.lazy_fragments.as_ref().unwrap().tree().height();
    assert!(
        clone_height >= 3,
        "deep clone lost nested interior nodes: height {clone_height}"
    );
    assert_eq!(scan_sorted(&reopened, None, None).await, expected);
}

#[rstest]
#[tokio::test]
async fn native_row_mutations_and_compaction_match_flat(
    #[values(false, true)] stable_ids: bool,
    #[values("buffered", "bulk")] materialization: &str,
    #[values(false, true)] indexed: bool,
) {
    let flat_dir = tempfile::tempdir().unwrap();
    let fragment_metadata_dir = tempfile::tempdir().unwrap();
    let flat_uri = flat_dir.path().to_str().unwrap();
    let fragment_metadata_uri = fragment_metadata_dir.path().to_str().unwrap();
    let batch = rows(0, 60);
    let flat = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        flat_uri,
        Some(WriteParams {
            max_rows_per_file: 10,
            max_rows_per_group: 10,
            enable_stable_row_ids: stable_ids,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    mirror_into_fragment_metadata(
        &flat,
        flat_dir.path(),
        fragment_metadata_uri,
        materialization,
    )
    .await;
    let fragment_metadata = DatasetBuilder::from_uri(fragment_metadata_uri)
        .load_unmaterialized()
        .await
        .unwrap();
    assert_eq!(
        fragment_metadata.manifest.next_row_id,
        flat.manifest.next_row_id
    );
    let initial = scan_sorted(&flat, None, None).await;
    let mut results = Vec::new();
    for mut dataset in [flat, fragment_metadata] {
        let initial_version = dataset.version_id();
        if indexed {
            dataset
                .create_index(
                    &["id"],
                    IndexType::Scalar,
                    Some("id_idx".to_string()),
                    &ScalarIndexParams::default(),
                    true,
                )
                .await
                .unwrap();
            let indices = dataset.load_indices().await.unwrap();
            assert_eq!(indices[0].fragment_bitmap.as_ref().unwrap().len(), 6);
            assert_eq!(scan_sorted(&dataset, None, Some("id >= 55")).await.len(), 5);
        }
        let update = UpdateBuilder::new(Arc::new(dataset))
            .update_where("id >= 15 AND id < 25")
            .unwrap()
            .set("name", "'updated-' || cast(id as string)")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert_eq!(update.rows_updated, 10);
        let after_update = scan_sorted(&update.new_dataset, None, None).await;
        assert_eq!(after_update.len(), 60);
        let source = rows(55, 10);
        let (merged, stats) =
            MergeInsertBuilder::try_new(update.new_dataset.clone(), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                ))
                .await
                .unwrap();
        assert_eq!(stats.num_inserted_rows, 5);
        assert_eq!(stats.num_updated_rows, 5);
        if indexed {
            assert_eq!(scan_sorted(&merged, None, Some("id >= 55")).await.len(), 10);
        }
        let before_compaction = scan_sorted(&merged, None, None).await;
        let metadata = merged
            .fragment_source()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            metadata
                .iter()
                .map(|fragment| fragment.num_rows().unwrap())
                .sum::<usize>(),
            65,
            "Merged metadata row count for {}",
            merged.uri()
        );
        assert_eq!(
            before_compaction.len(),
            65,
            "Merged rows for {}: {before_compaction:?}",
            merged.uri()
        );
        let mut compacted = merged.as_ref().clone();
        let metrics = compact_files(
            &mut compacted,
            CompactionOptions {
                target_rows_per_fragment: 20,
                materialize_deletions: true,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        assert!(metrics.fragments_removed > 0);
        let after_compaction = scan_sorted(&compacted, None, None).await;
        let mut index_coverage = Vec::new();
        for index in compacted.load_indices().await.unwrap().iter() {
            // Independent compaction tasks can assign different physical IDs.
            // Compare the actual indexed rows rather than those allocation labels.
            let bitmap = index.fragment_bitmap.as_ref().unwrap();
            let mut covered_ids = Vec::new();
            let fragments = compacted.get_fragments_async().await.unwrap();
            for fragment in fragments
                .iter()
                .filter(|fragment| bitmap.contains(fragment.id() as u32))
            {
                let batch = fragment
                    .scan()
                    .project(&["id"])
                    .unwrap()
                    .try_into_batch()
                    .await
                    .unwrap();
                covered_ids.extend(
                    batch
                        .column_by_name("id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied(),
                );
            }
            covered_ids.sort_unstable();
            assert!(!covered_ids.is_empty());
            index_coverage.push((
                index.name.clone(),
                index.fields.clone(),
                covered_ids,
                index.dataset_version - initial_version,
            ));
        }
        assert_eq!(before_compaction, after_compaction);
        assert_eq!(
            scan_sorted(
                &compacted.checkout_version(initial_version).await.unwrap(),
                None,
                None
            )
            .await,
            initial
        );
        assert_eq!(compacted.count_rows(None).await.unwrap(), 65);
        compacted
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![(
                    "double_id".to_string(),
                    "id * 2".to_string(),
                )]),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(compacted.count_rows(None).await.unwrap(), 65);
        assert_eq!(
            compacted
                .count_rows(Some("double_id != id * 2".to_string()))
                .await
                .unwrap(),
            0
        );
        let backfilled = scan_sorted(&compacted, None, None).await;
        let maximum_id = compacted.manifest.max_fragment_id;
        let mut restored = compacted.checkout_version(initial_version).await.unwrap();
        restored.restore().await.unwrap();
        assert_eq!(scan_sorted(&restored, None, None).await, initial);
        assert_eq!(restored.manifest.max_fragment_id, maximum_id);
        results.push((after_update, after_compaction, backfilled, index_coverage));
    }
    assert_eq!(results[0], results[1]);
}

#[tokio::test]
async fn lazy_scan_matches_flat_scan_on_real_files() {
    let flat_dir = tempfile::tempdir().unwrap();
    let fragment_metadata_dir = tempfile::tempdir().unwrap();
    let flat_uri = flat_dir.path().to_str().unwrap();
    let fragment_metadata_uri = fragment_metadata_dir.path().to_str().unwrap();

    write_flat(flat_uri, 0, 35, WriteMode::Create).await;
    write_flat(flat_uri, 35, 23, WriteMode::Append).await;
    let mut flat = Dataset::open(flat_uri).await.unwrap();
    flat.delete("id = 3 OR id = 40").await.unwrap();
    assert!(flat.fragments().len() >= 6, "want a multi-fragment table");

    mirror_into_fragment_metadata(&flat, flat_dir.path(), fragment_metadata_uri, "buffered").await;

    let fragment_metadata = DatasetBuilder::from_uri(fragment_metadata_uri)
        .load_unmaterialized()
        .await
        .unwrap();
    assert!(
        fragment_metadata.fragment_source().is_lazy(),
        "fragment metadata open must not materialize fragments"
    );
    assert!(
        fragment_metadata.fragments().is_empty(),
        "the manifest list stays empty"
    );
    // The lazy table plans through the ordinary modern read path.
    let plan = fragment_metadata.scan().explain_plan(true).await.unwrap();
    assert!(
        plan.contains("LanceRead"),
        "expected the modern read path, got:\n{plan}"
    );

    for (columns, filter) in [
        (None, None),
        (Some(["name"].as_slice()), None),
        (None, Some("id > 5 AND id < 50")),
        (Some(["id"].as_slice()), Some("id % 7 = 0")),
    ] {
        let expected = scan_sorted(&flat, columns, filter).await;
        let actual = scan_sorted(&fragment_metadata, columns, filter).await;
        assert_eq!(actual, expected, "columns={columns:?} filter={filter:?}");
        assert!(!expected.is_empty());
    }
    assert_eq!(
        fragment_metadata.count_rows(None).await.unwrap(),
        flat.count_rows(None).await.unwrap()
    );
    assert_eq!(
        fragment_metadata
            .count_rows(Some("id < 10".into()))
            .await
            .unwrap(),
        flat.count_rows(Some("id < 10".into())).await.unwrap()
    );
    let error = fragment_metadata
        .take(&[0, 1], flat.schema().clone())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("complete fragment list"),
        "{error}"
    );
}

#[tokio::test]
async fn lazy_open_and_first_fragment_stay_bounded() {
    let n = 1024;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = lance_core::datatypes::Schema::try_from(&ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
    .unwrap();
    let config = HashMap::from([
        (
            MANIFEST_LAYOUT_KEY.to_string(),
            lance_table::fragment_metadata::MANIFEST_LAYOUT_TREE.to_string(),
        ),
        (MAX_NODE_BYTES_KEY.to_string(), (256 * 1024).to_string()),
    ]);
    {
        CommitBuilder::new(uri)
            .execute(Transaction::new_from_version(
                0,
                Operation::Overwrite {
                    fragments: (0..n).map(make_fragment).collect(),
                    schema,
                    config_upsert_values: Some(config),
                    initial_bases: None,
                },
            ))
            .await
            .unwrap();
    }

    let io = IOTracker::default();
    let params = ReadParams {
        store_options: Some(ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(io.clone())),
            ..Default::default()
        }),
        ..Default::default()
    };
    let dataset = DatasetBuilder::from_uri(uri)
        .with_read_params(params)
        .load_unmaterialized()
        .await
        .unwrap();
    let open_reads = io.incremental_stats().read_iops;
    assert!(dataset.fragment_source().is_lazy());

    let mut stream = dataset.fragment_source().stream();
    let first = stream.try_next().await.unwrap().unwrap();
    let first_reads = io.incremental_stats().read_iops;
    assert_eq!(first.id, 0);
    assert!(
        open_reads <= 4,
        "open must read the root chain only, read {open_reads} objects"
    );
    assert!(
        first_reads <= 8,
        "first fragment must arrive after one root-to-leaf path, read {first_reads} objects"
    );

    let mut count = 0u64;
    while let Some(fragment) = stream.try_next().await.unwrap() {
        count += 1;
        assert!(fragment.id > 0);
    }
    assert_eq!(count + 1, n);
}

#[tokio::test]
async fn checkout_latest_at_the_current_version_reads_no_tree_objects() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = lance_core::datatypes::Schema::try_from(&ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
    .unwrap();
    let config = HashMap::from([(
        MANIFEST_LAYOUT_KEY.to_string(),
        lance_table::fragment_metadata::MANIFEST_LAYOUT_TREE.to_string(),
    )]);
    CommitBuilder::new(uri)
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: (0..64).map(make_fragment).collect(),
                schema,
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ))
        .await
        .unwrap();
    let io = IOTracker::default();
    let params = ReadParams {
        store_options: Some(ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(io.clone())),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut dataset = DatasetBuilder::from_uri(uri)
        .with_read_params(params)
        .load()
        .await
        .unwrap();
    let fragments = dataset.fragments().clone();
    io.incremental_stats();

    dataset.checkout_latest().await.unwrap();

    let tree_reads: Vec<_> = io
        .incremental_stats()
        .requests
        .into_iter()
        .filter(|request| request.path.as_ref().contains("_bt/"))
        .collect();
    assert!(
        tree_reads.is_empty(),
        "unexpected tree reads: {tree_reads:?}"
    );
    assert_eq!(dataset.fragments(), &fragments);
}

/// A session that follows a table keeps each leaf it decodes twice, so a
/// later version whose commit changed no leaf reads none, and every version
/// it follows still matches a fresh load.
#[tokio::test]
async fn following_session_rereads_no_unchanged_leaf() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    let schema = lance_core::datatypes::Schema::try_from(&ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
    .unwrap();
    let config = HashMap::from([
        (
            MANIFEST_LAYOUT_KEY.to_string(),
            lance_table::fragment_metadata::MANIFEST_LAYOUT_TREE.to_string(),
        ),
        (MAX_LEAF_BYTES_KEY.to_string(), "16384".to_string()),
    ]);
    let mut writer = CommitBuilder::new(uri)
        .execute(Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: (0..256).map(make_fragment).collect(),
                schema,
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        ))
        .await
        .unwrap();
    let io = IOTracker::default();
    let params = ReadParams {
        store_options: Some(ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(io.clone())),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut follower = DatasetBuilder::from_uri(uri)
        .with_read_params(params)
        .load()
        .await
        .unwrap();

    let mut leaf_reads = Vec::new();
    for round in 0..3 {
        let version = writer.version().version;
        writer = CommitBuilder::new(Arc::new(writer))
            .execute(Transaction::new_from_version(
                version,
                Operation::Append {
                    fragments: vec![make_fragment(1000 + round)],
                },
            ))
            .await
            .unwrap();
        io.incremental_stats();
        follower.checkout_latest().await.unwrap();
        leaf_reads.push(
            io.incremental_stats()
                .requests
                .iter()
                .filter(|request| request.path.as_ref().contains("_bt/leaf/"))
                .count(),
        );
        let fresh = DatasetBuilder::from_uri(uri).load().await.unwrap();
        assert_eq!(follower.fragments(), fresh.fragments());
    }
    // The first follow decodes each leaf a second time and keeps it.
    assert!(leaf_reads[0] > 1, "{leaf_reads:?}");
    assert_eq!(leaf_reads[1..], [0, 0], "{leaf_reads:?}");
}

/// Delete against Delete on the same fragment must classify and rebase
/// exactly as flat Lance does through `DeleteBuilder`. Flat merges disjoint
/// row deletions on the same fragment, rejects overlapping ones as retryable
/// at the rebase step (then succeeds on retry by re-scanning), and removes a
/// fragment whose rows are all gone.
async fn delete_pair(
    flat_predicate_a: &str,
    predicate_b: &str,
) -> (
    crate::Result<u64>,
    crate::Result<u64>,
    Vec<String>,
    Vec<String>,
) {
    use crate::dataset::write::delete::DeleteBuilder;
    let flat_dir = tempfile::tempdir().unwrap();
    let fragment_metadata_dir = tempfile::tempdir().unwrap();
    let flat_uri = flat_dir.path().to_str().unwrap();
    let fragment_metadata_uri = fragment_metadata_dir.path().to_str().unwrap();
    write_flat(flat_uri, 0, 40, WriteMode::Create).await;
    let flat = Dataset::open(flat_uri).await.unwrap();
    mirror_into_fragment_metadata(&flat, flat_dir.path(), fragment_metadata_uri, "buffered").await;

    let run = |uri: String| {
        let a = flat_predicate_a.to_string();
        let b = predicate_b.to_string();
        async move {
            // Both writers hold the same version, then commit in turn with
            // zero conflict retries so the rebase itself is what we observe,
            // not a retry that re-scans.
            let stale_a = Arc::new(Dataset::open(&uri).await.unwrap());
            let stale_b = Arc::new(Dataset::open(&uri).await.unwrap());
            let first = DeleteBuilder::new(stale_a, &a)
                .conflict_retries(0)
                .execute()
                .await
                .map(|result| result.new_dataset.manifest.version);
            let second = DeleteBuilder::new(stale_b, &b)
                .conflict_retries(0)
                .execute()
                .await
                .map(|result| result.new_dataset.manifest.version);
            (first, second)
        }
    };
    let (flat_first, flat_second) = run(flat_uri.to_string()).await;
    assert!(flat_first.is_ok(), "{flat_first:?}");
    let (fragment_metadata_first, fragment_metadata_second) =
        run(fragment_metadata_uri.to_string()).await;
    assert!(
        fragment_metadata_first.is_ok(),
        "{fragment_metadata_first:?}"
    );

    let flat_state = scan_sorted(&Dataset::open(flat_uri).await.unwrap(), None, None).await;
    let fragment_metadata_state = scan_sorted(
        &DatasetBuilder::from_uri(fragment_metadata_uri)
            .load_unmaterialized()
            .await
            .unwrap(),
        None,
        None,
    )
    .await;
    (
        flat_second,
        fragment_metadata_second,
        flat_state,
        fragment_metadata_state,
    )
}

fn same_verdict(flat: &crate::Result<u64>, fragment_metadata: &crate::Result<u64>) -> bool {
    match (flat, fragment_metadata) {
        (Ok(_), Ok(_)) => true,
        (Err(flat), Err(fragment_metadata)) => {
            super::differential::error_category(flat)
                == super::differential::error_category(fragment_metadata)
        }
        _ => false,
    }
}

#[tokio::test]
async fn delete_delete_same_fragment_disjoint_rows_rebases_like_flat() {
    let (flat, fragment_metadata, flat_state, fragment_metadata_state) =
        delete_pair("id = 1", "id = 3").await;
    assert!(flat.is_ok(), "flat merges disjoint deletes: {flat:?}");
    assert!(
        same_verdict(&flat, &fragment_metadata),
        "flat={flat:?} fragment_metadata={fragment_metadata:?}"
    );
    assert_eq!(fragment_metadata_state, flat_state);
    assert_eq!(flat_state.len(), 38);
}

#[tokio::test]
async fn delete_delete_same_fragment_overlapping_rows_conflicts_like_flat() {
    let (flat, fragment_metadata, flat_state, fragment_metadata_state) =
        delete_pair("id = 1 OR id = 2", "id = 2").await;
    assert!(
        flat.is_err(),
        "flat rejects overlapping row deletes: {flat:?}"
    );
    assert!(
        same_verdict(&flat, &fragment_metadata),
        "flat={flat:?} fragment_metadata={fragment_metadata:?}"
    );
    assert_eq!(fragment_metadata_state, flat_state);
}

#[tokio::test]
async fn delete_delete_covering_all_rows_removes_fragment_like_flat() {
    // Fragment 0 holds ids 0..10; together the two deletes cover it entirely.
    let (flat, fragment_metadata, flat_state, fragment_metadata_state) =
        delete_pair("id < 5", "id >= 5 AND id < 10").await;
    assert!(flat.is_ok(), "{flat:?}");
    assert!(
        same_verdict(&flat, &fragment_metadata),
        "flat={flat:?} fragment_metadata={fragment_metadata:?}"
    );
    assert_eq!(fragment_metadata_state, flat_state);
    assert_eq!(flat_state.len(), 30);
}

#[tokio::test]
async fn stale_delete_after_whole_fragment_removal_conflicts_like_flat() {
    // Writer A removes fragment 0 outright; B's stale delete touches it.
    let (flat, fragment_metadata, flat_state, fragment_metadata_state) =
        delete_pair("id < 10", "id = 3").await;
    assert!(
        same_verdict(&flat, &fragment_metadata),
        "flat={flat:?} fragment_metadata={fragment_metadata:?}"
    );
    assert_eq!(fragment_metadata_state, flat_state);
}

#[tokio::test]
async fn public_lazy_handle_scans_commits_and_materializes_offsets() {
    let flat_dir = tempfile::tempdir().unwrap();
    let tree_dir = tempfile::tempdir().unwrap();
    let flat = write_flat(flat_dir.path().to_str().unwrap(), 0, 40, WriteMode::Create).await;
    let uri = tree_dir.path().to_str().unwrap();
    mirror_into_fragment_metadata(&flat, flat_dir.path(), uri, "buffered").await;
    let lazy = DatasetBuilder::from_uri(uri).load_lazy().await.unwrap();
    assert_eq!(lazy.count_rows(None).await.unwrap(), 40);
    assert_eq!(lazy.count_rows(Some("id >= 15".into())).await.unwrap(), 25);
    assert_eq!(lazy.scan().try_into_batch().await.unwrap().num_rows(), 40);
    let flat_lazy = DatasetBuilder::from_uri(flat_dir.path().to_str().unwrap())
        .load_lazy()
        .await
        .unwrap();
    for ids in [
        vec![],
        vec![3, 0, 3],
        vec![3, 0, 3, 40, u64::from(u32::MAX) + 1, u64::MAX],
        vec![40, u64::from(u32::MAX) + 1, u64::MAX],
    ] {
        let expected = flat_lazy.get_fragments(&ids).await.unwrap();
        let actual = lazy.get_fragments(&ids).await.unwrap();
        assert_eq!(actual, expected, "fragment IDs {ids:?}");
    }
    let committed = lazy
        .commit(Transaction::new_from_version(
            lazy.version_id(),
            Operation::Delete {
                updated_fragments: Vec::new(),
                deleted_fragment_ids: vec![1],
                predicate: "fixture".into(),
            },
        ))
        .await
        .unwrap();
    assert_eq!(committed.count_rows(None).await.unwrap(), 30);
    assert_eq!(lazy.count_rows(None).await.unwrap(), 40);
    assert_eq!(
        committed
            .checkout_version(lazy.version_id())
            .await
            .unwrap()
            .count_rows(None)
            .await
            .unwrap(),
        40
    );
    let dataset = committed.into_dataset().await.unwrap();
    let rows = dataset
        .take(&[0, 10, 29], dataset.schema().clone())
        .await
        .unwrap();
    assert_eq!(rows.num_rows(), 3);
    assert_eq!(
        rows.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0, 20, 39]
    );
}
