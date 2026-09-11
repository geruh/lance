// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Differential oracle: the same production `Transaction` committed through
//! `CommitBuilder` against a flat dataset and a fragment metadata tree, then
//! both logical states read back and compared. Flat Lance is the oracle.

use std::collections::BTreeMap;

use lance_core::datatypes::Schema;
use lance_core::{Error, Result};
use lance_file::version::LanceFileVersion;
use lance_table::format::{DeletionFile, DeletionFileType, Fragment};
use lance_table::fragment_metadata::support::{
    make_backfill_data_file, make_fragment, make_replacement_data_file,
};

use super::MAX_NODE_BYTES_KEY;
use super::test_support::{AddColumns, Reader, commit_target_for_uri, execute_add_columns};
use crate::dataset::builder::DatasetBuilder;
use crate::dataset::transaction::{DataReplacementGroup, Operation, Transaction};
use crate::dataset::write::CommitBuilder;
use lance_table::fragment_metadata::MANIFEST_LAYOUT_KEY;
use lance_table::io::commit::CommitConfig;

pub(super) fn table_schema() -> Schema {
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    Schema::try_from(&ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("name", DataType::Utf8, false),
    ]))
    .unwrap()
}

/// Canonical logical state of a dataset, layout independent.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct LogicalState {
    pub version: u64,
    pub schema: Schema,
    pub fragments: BTreeMap<u64, Fragment>,
}

pub(super) struct Side {
    pub uri: String,
    _dir: tempfile::TempDir,
}

pub(super) struct Differential {
    pub flat: Side,
    pub fragment_metadata: Side,
}

pub(super) struct Outcome {
    pub flat: Result<u64>,
    pub fragment_metadata: Result<u64>,
    /// `Some(true)` when both accepted and logical states are equal.
    pub parity: Option<bool>,
    /// The first divergence when both accepted and states differ.
    pub diff: Option<String>,
}

impl Outcome {
    pub fn both_accepted_and_equal(&self) -> bool {
        self.flat.is_ok() && self.fragment_metadata.is_ok() && self.parity == Some(true)
    }
    pub fn both_rejected(&self) -> bool {
        matches!((&self.flat, &self.fragment_metadata), (Err(flat), Err(tree)) if std::mem::discriminant(flat) == std::mem::discriminant(tree))
    }
}

pub(super) fn error_category(error: &Error) -> String {
    let text = error.to_string();
    text.split(':').next().unwrap_or("").trim().to_string()
}

impl Differential {
    pub async fn create(n: u64) -> Self {
        Self::create_with_fragments((0..n).map(make_fragment).collect()).await
    }

    pub async fn create_with_fragments(fragments: Vec<Fragment>) -> Self {
        let flat_dir = tempfile::tempdir().unwrap();
        let fragment_metadata_dir = tempfile::tempdir().unwrap();
        let flat = Side {
            uri: flat_dir.path().to_str().unwrap().to_string(),
            _dir: flat_dir,
        };
        let fragment_metadata = Side {
            uri: fragment_metadata_dir.path().to_str().unwrap().to_string(),
            _dir: fragment_metadata_dir,
        };
        CommitBuilder::new(flat.uri.as_str())
            .execute(Transaction::new_from_version(
                0,
                Operation::Overwrite {
                    fragments: fragments.clone(),
                    schema: table_schema(),
                    config_upsert_values: None,
                    initial_bases: None,
                },
            ))
            .await
            .unwrap();
        let config = std::collections::HashMap::from([
            (
                MANIFEST_LAYOUT_KEY.to_string(),
                lance_table::fragment_metadata::MANIFEST_LAYOUT_TREE.to_string(),
            ),
            (MAX_NODE_BYTES_KEY.to_string(), (4 * 1024).to_string()),
            (
                super::MAX_LEAF_BYTES_KEY.to_string(),
                (4 * 1024).to_string(),
            ),
            (
                "lance.fragment_metadata.allow_deep_writer".to_string(),
                "true".to_string(),
            ),
        ]);
        CommitBuilder::new(fragment_metadata.uri.as_str())
            .execute(Transaction::new_from_version(
                0,
                Operation::Overwrite {
                    fragments,
                    schema: table_schema(),
                    config_upsert_values: Some(config),
                    initial_bases: None,
                },
            ))
            .await
            .unwrap();
        let this = Self {
            flat,
            fragment_metadata,
        };
        let diff = this.states_diff().await;
        assert!(diff.is_none(), "bootstrap states differ: {diff:?}");
        this
    }

    pub async fn flat_state(&self) -> LogicalState {
        let dataset = DatasetBuilder::from_uri(&self.flat.uri)
            .load()
            .await
            .unwrap();
        LogicalState {
            version: dataset.manifest.version,
            schema: dataset.schema().clone(),
            fragments: dataset
                .fragments()
                .iter()
                .map(|fragment| (fragment.id, fragment.clone()))
                .collect(),
        }
    }

    pub async fn fragment_metadata_reader(&self) -> Reader {
        Reader::open_uri(&self.fragment_metadata.uri).await.unwrap()
    }

    pub async fn fragment_metadata_state(&self) -> LogicalState {
        let reader = self.fragment_metadata_reader().await;

        LogicalState {
            version: reader.version(),
            schema: reader.schema().unwrap(),
            fragments: reader
                .materialize()
                .await
                .unwrap()
                .into_iter()
                .map(|fragment| (fragment.id, fragment))
                .collect(),
        }
    }

    /// `None` when both sides describe the same table, otherwise a
    /// description of the first divergence found.
    pub async fn states_diff(&self) -> Option<String> {
        let flat = self.flat_state().await;
        let fragment_metadata = self.fragment_metadata_state().await;
        if flat.version != fragment_metadata.version {
            return Some(format!(
                "version differs: flat={} fragment_metadata={}",
                flat.version, fragment_metadata.version
            ));
        }
        if flat.schema != fragment_metadata.schema {
            return Some(format!(
                "schema differs:\nflat={:?}\nfragment_metadata={:?}",
                flat.schema, fragment_metadata.schema
            ));
        }
        if flat.fragments != fragment_metadata.fragments {
            let flat_ids: Vec<_> = flat.fragments.keys().copied().collect();
            let fragment_metadata_ids: Vec<_> =
                fragment_metadata.fragments.keys().copied().collect();
            if flat_ids != fragment_metadata_ids {
                let only_flat: Vec<_> = flat_ids
                    .iter()
                    .filter(|id| !fragment_metadata.fragments.contains_key(id))
                    .collect();
                let only_fragment_metadata: Vec<_> = fragment_metadata_ids
                    .iter()
                    .filter(|id| !flat.fragments.contains_key(id))
                    .collect();
                return Some(format!(
                    "fragment ids differ: only_flat={only_flat:?} only_fragment_metadata={only_fragment_metadata:?}"
                ));
            }
            for (id, fragment) in &flat.fragments {
                if fragment_metadata.fragments.get(id) != Some(fragment) {
                    return Some(format!(
                        "fragment {id} differs:\nflat={fragment:?}\nfragment_metadata={:?}",
                        fragment_metadata.fragments.get(id)
                    ));
                }
            }
            return Some("fragments differ".to_string());
        }
        None
    }

    pub async fn states_equal(&self) -> bool {
        self.states_diff().await.is_none()
    }

    /// Commit `operation` on both sides from `read_version`, record the row,
    /// and compare logical states when both accepted.
    pub async fn apply(&mut self, step: &str, read_version: u64, operation: Operation) -> Outcome {
        let flat = CommitBuilder::new(self.flat.uri.as_str())
            .execute(Transaction::new_from_version(
                read_version,
                operation.clone(),
            ))
            .await
            .map(|dataset| dataset.manifest.version);
        let fragment_metadata = CommitBuilder::new(self.fragment_metadata.uri.as_str())
            .execute(Transaction::new_from_version(
                read_version,
                operation.clone(),
            ))
            .await
            .map(|dataset| dataset.manifest.version);
        self.record(step, flat, fragment_metadata).await
    }

    async fn record(
        &mut self,
        step: &str,
        flat: Result<u64>,
        fragment_metadata: Result<u64>,
    ) -> Outcome {
        let diff = if flat.is_ok() && fragment_metadata.is_ok() {
            self.states_diff().await
        } else {
            None
        };
        let parity = if flat.is_ok() && fragment_metadata.is_ok() {
            Some(diff.is_none())
        } else {
            None
        };
        let diff = diff.map(|difference| format!("{step}: {difference}"));
        Outcome {
            flat,
            fragment_metadata,
            parity,
            diff,
        }
    }

    /// Commit from each side's current tip.
    pub async fn apply_latest(&mut self, step: &str, operation: Operation) -> Outcome {
        let version = self.flat_state().await.version;
        self.apply(step, version, operation).await
    }
}

/// The schema flat Lance produces when add-column round `col` lands: the
/// current schema merged with one Int32 field whose id follows the current
/// max, which is the id `make_backfill_data_file(_, col)` writes.
pub(super) fn schema_with_column(current: &Schema, col: u32) -> Schema {
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    let added = ArrowSchema::new(vec![ArrowField::new(
        format!("col{col}"),
        DataType::Int32,
        true,
    )]);
    let mut schema = current.merge(&added).unwrap();
    schema.set_field_id(current.max_field_id());
    assert_eq!(
        schema.max_field_id(),
        Some(2 + col as i32),
        "backfill file field id must match the new schema field id"
    );
    schema
}

impl Differential {
    pub async fn apply_add_columns(
        &mut self,
        step: &str,
        read_version: u64,
        ids: &[u64],
        col: u32,
    ) -> Outcome {
        let base = {
            let dataset = DatasetBuilder::from_uri(&self.flat.uri)
                .with_version(read_version)
                .load()
                .await
                .unwrap();
            LogicalState {
                version: dataset.manifest.version,
                schema: dataset.schema().clone(),
                fragments: dataset
                    .fragments()
                    .iter()
                    .map(|fragment| (fragment.id, fragment.clone()))
                    .collect(),
            }
        };
        let schema = schema_with_column(&base.schema, col);
        let merged_fragments: Vec<Fragment> = base
            .fragments
            .values()
            .cloned()
            .map(|mut fragment| {
                if ids.contains(&fragment.id) {
                    fragment
                        .files
                        .push(make_backfill_data_file(fragment.id, col));
                }
                fragment
            })
            .collect();
        let flat = CommitBuilder::new(self.flat.uri.as_str())
            .execute(Transaction::new_from_version(
                read_version,
                Operation::Merge {
                    preserves_nullability: false,
                    fragments: merged_fragments,
                    schema: schema.clone(),
                },
            ))
            .await
            .map(|dataset| dataset.manifest.version);
        let add_columns = AddColumns {
            read_version,
            schema,
            replacements: ids
                .iter()
                .map(|id| DataReplacementGroup(*id, make_backfill_data_file(*id, col)))
                .collect(),
        };
        let fragment_metadata = execute_add_columns(
            commit_target_for_uri(&self.fragment_metadata.uri)
                .await
                .unwrap(),
            &CommitConfig::default(),
            &add_columns,
        )
        .await
        .map(|dataset| dataset.manifest.version);
        self.record(step, flat, fragment_metadata).await
    }
}

pub(super) fn deletion_file(id: u64, read_version: u64) -> DeletionFile {
    DeletionFile {
        read_version,
        id,
        file_type: DeletionFileType::Bitmap,
        num_deleted_rows: Some(1),
        base_id: None,
    }
}

pub(super) fn production_style_append(count: usize) -> Operation {
    production_style_append_with_version(count, LanceFileVersion::V2_0)
}

/// The production writer emits fragments with id 0 and lets the commit
/// assign real ids (`Transaction::fragments_with_ids`). Data files must
/// carry the dataset's storage version, which an empty table defaults to
/// the current stable version.
pub(super) fn production_style_append_with_version(
    count: usize,
    version: LanceFileVersion,
) -> Operation {
    let (major, minor) = version.resolve().to_data_file_numbers();
    Operation::Append {
        fragments: (0..count)
            .map(|index| {
                let mut fragment = make_fragment(0);
                fragment.id = 0;
                fragment.physical_rows = Some(10);
                fragment.files[0].path =
                    format!("data/fresh-{index}-{}.lance", uuid::Uuid::new_v4());
                fragment.files[0].file_major_version = major;
                fragment.files[0].file_minor_version = minor;
                fragment
            })
            .collect(),
    }
}

pub(super) fn add_column(ids: impl IntoIterator<Item = u64>, col: u32) -> Operation {
    Operation::DataReplacement {
        replacements: ids
            .into_iter()
            .map(|id| DataReplacementGroup(id, make_backfill_data_file(id, col)))
            .collect(),
    }
}

pub(super) fn replace_base(ids: impl IntoIterator<Item = u64>, round: u32) -> Operation {
    Operation::DataReplacement {
        replacements: ids
            .into_iter()
            .map(|id| DataReplacementGroup(id, make_replacement_data_file(id, round)))
            .collect(),
    }
}

pub(super) fn delete_fragments(ids: impl IntoIterator<Item = u64>) -> Operation {
    Operation::Delete {
        updated_fragments: Vec::new(),
        deleted_fragment_ids: ids.into_iter().collect(),
        predicate: "true".to_string(),
    }
}

pub(super) fn set_deletion_file(mut fragment: Fragment, file_id: u64) -> Operation {
    let read_version = fragment.id;
    fragment.deletion_file = Some(deletion_file(file_id, read_version));
    Operation::Delete {
        updated_fragments: vec![fragment],
        deleted_fragment_ids: Vec::new(),
        predicate: "id = 1".to_string(),
    }
}
