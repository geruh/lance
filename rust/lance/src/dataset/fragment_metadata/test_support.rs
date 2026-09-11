// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Test-only entry points over the fragment metadata commit path: opening a
//! tree at a version, a lazy reader session, and a Merge commit that adds
//! columns.

use std::sync::Arc;

use lance_core::Result;
use lance_core::datatypes::Schema;
use lance_io::object_store::ObjectStore;
use lance_table::format::{Fragment, Manifest, pb};
use lance_table::fragment_metadata::{FragmentMetadataTree, TouchedFragments};
use lance_table::io::commit::{CommitConfig, ConditionalPutCommitHandler, commit_handler_from_url};

use super::publication::{read_version, tree_from_manifest};
use super::{CommitTarget, execute_commit, replacement_actions};
use crate::dataset::transaction::{DataReplacementGroup, Operation, Transaction};
use crate::dataset::{Dataset, ManifestWriteConfig};
use crate::session::Session;

pub(super) async fn open_tree(
    target: &CommitTarget,
    version: Option<u64>,
) -> Result<(FragmentMetadataTree, Manifest)> {
    let (manifest, _) = read_version(
        &target.object_store,
        &target.base_path,
        target.commit_handler.as_ref(),
        version,
    )
    .await?;
    let tree = tree_from_manifest(
        target.object_store.clone(),
        target.base_path.clone(),
        &manifest,
    )
    .await?;
    Ok((tree, manifest))
}

/// Build a commit target for a dataset uri the way `CommitBuilder` does.
pub(super) async fn commit_target_for_uri(uri: &str) -> Result<CommitTarget> {
    let session = Arc::new(Session::default());
    let (object_store, base_path) =
        ObjectStore::from_uri_and_params(session.store_registry(), uri, &Default::default())
            .await?;
    let commit_handler = commit_handler_from_url(uri, &None).await?;
    Ok(CommitTarget {
        context: None,
        materialize_fragments: false,
        object_store,
        base_path,
        uri: uri.to_string(),
        session,
        commit_handler,
    })
}

/// A new table schema plus the data files that back the new fields,
/// published as `Operation::Merge`.
pub(super) struct AddColumns {
    pub read_version: u64,
    pub schema: Schema,
    pub replacements: Vec<DataReplacementGroup>,
}

pub(super) async fn execute_add_columns(
    target: CommitTarget,
    commit_config: &CommitConfig,
    add_columns: &AddColumns,
) -> Result<Dataset> {
    let (tree, _) = open_tree(&target, Some(add_columns.read_version)).await?;
    let fragments = tree.materialize().await?;
    let touched = TouchedFragments {
        ids: fragments.iter().map(|fragment| fragment.id).collect(),
        fragments: fragments
            .into_iter()
            .map(|fragment| (fragment.id, fragment))
            .collect(),
    };
    let actions = replacement_actions(&add_columns.replacements, &touched)?;
    let mut fragments = touched.fragments;
    let messages = actions
        .into_iter()
        .enumerate()
        .map(|(action_sequence, action)| pb::FragmentMetadataMutation {
            action_sequence: action_sequence as u64 + 1,
            action: Some(action),
            ..Default::default()
        })
        .collect();
    lance_table::fragment_metadata::node::apply_actions(&mut fragments, messages)?;
    let transaction = Transaction::new_from_version(
        add_columns.read_version,
        Operation::Merge {
            fragments: fragments.into_values().collect(),
            schema: add_columns.schema.clone(),
            preserves_nullability: false,
        },
    );
    execute_commit(
        target,
        commit_config,
        &transaction,
        None,
        &ManifestWriteConfig::default(),
    )
    .await
}

/// Lazy metadata session. The named Version Manifest and its immutable root
/// base determine state; selective resolution reads only the necessary paths.
pub(super) struct Reader {
    pub(super) tree: FragmentMetadataTree,
    schema: Schema,
}

impl Reader {
    /// Open the latest version from a dataset uri, resolving the object store
    /// the same way the commit path does.
    pub async fn open_uri(uri: &str) -> Result<Self> {
        Self::open_uri_version(uri, None).await
    }

    /// Open a specific published version from a dataset uri.
    pub async fn open_uri_at(uri: &str, version: u64) -> Result<Self> {
        Self::open_uri_version(uri, Some(version)).await
    }

    async fn open_uri_version(uri: &str, version: Option<u64>) -> Result<Self> {
        let session = Arc::new(Session::default());
        let (object_store, base) =
            ObjectStore::from_uri_and_params(session.store_registry(), uri, &Default::default())
                .await?;
        let (manifest, _) =
            read_version(&object_store, &base, &ConditionalPutCommitHandler, version).await?;
        let tree = tree_from_manifest(object_store, base, &manifest).await?;
        Ok(Self {
            tree,
            schema: manifest.schema,
        })
    }

    /// The table schema stored with this version.
    pub fn schema(&self) -> Result<Schema> {
        Ok(self.schema.clone())
    }

    pub fn version(&self) -> u64 {
        self.tree.version()
    }

    pub fn count_fragments(&self) -> u64 {
        self.tree.count_fragments()
    }

    pub async fn resolve_fragment(&self, fragment_id: u64) -> Result<Option<Fragment>> {
        self.tree.resolve_fragment(fragment_id).await
    }

    /// Materialize the full fragment list, for benchmarks and global checks.
    pub async fn materialize(&self) -> Result<Vec<Fragment>> {
        self.tree.materialize().await
    }
}
