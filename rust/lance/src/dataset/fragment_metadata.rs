// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Fragment metadata trees behind the production commit entry point.
//!
//! `lance.manifest.layout=tree` selects this layout at creation. The Version
//! Manifest owns schema, configuration, transaction identity, and the
//! snapshot: an inline root, or `_bt/base/{uuid}.root` plus
//! `mutations_since_root`. Transaction files are for conflict detection.
//! Opening a named version reads that manifest and at most one external root.
//!
//! This layout is unstable. Use a disposable dataset. The format may change
//! without keeping compatibility with earlier revisions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lance_core::utils::backoff::SlotBackoff;

use lance_core::cache::LanceCache;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_table::format::{Fragment, Manifest, pb};
use lance_table::fragment_metadata::store::NodeStore;
use lance_table::fragment_metadata::{
    FragmentMetadataTree, FragmentMetadataTreeConfig, ManifestLayout, TouchedFragments,
    data_replacement,
};
use lance_table::io::commit::{
    CommitConfig, CommitHandler, ManifestLocation, ManifestNamingScheme,
};
use object_store::path::Path;
use prost::Message;

use lance_select::RowAddrTreeMap;
use lance_table::io::deletion::read_deletion_file;

use super::Dataset;
use super::transaction::{DataReplacementGroup, Operation, Transaction};
use crate::io::commit::{cleanup_transaction_file, write_transaction_file};
use crate::session::Session;
mod native;
mod publication;
#[cfg(test)]
mod test_support;
pub(crate) use publication::tree_from_manifest;

/// Encoded node-size budget in bytes, read from Overwrite config at create time.
pub const MAX_NODE_BYTES_KEY: &str = "lance.fragment_metadata.max_node_bytes";
/// Encoded leaf-size budget in bytes, read from Overwrite config at create time.
pub const MAX_LEAF_BYTES_KEY: &str = "lance.fragment_metadata.max_leaf_bytes";

/// Materialization policy for fragment metadata commits.
#[derive(Debug, Clone, Copy, Default)]
pub enum Materialization {
    /// Retain validated changes in buffers and flush under byte pressure.
    #[default]
    Buffered,
    /// Merge the complete fragment state into new leaves in one ordered pass.
    Bulk,
}

/// Creation-time options for external fragment metadata.
///
/// Tree and publication budgets measure different objects. Flat manifests remain the default.
///
/// ```
/// # use lance::dataset::fragment_metadata::FragmentMetadataOptions;
/// # fn example() -> lance::Result<()> {
/// let config = FragmentMetadataOptions::default().into_table_config()?;
/// assert_eq!(config["lance.manifest.layout"], "tree");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct FragmentMetadataOptions {
    /// Encoded directory, leaf, buffer, and hard object byte budgets.
    pub tree: FragmentMetadataTreeConfig,
    /// Independent inline-descriptor and cumulative-suffix byte budgets.
    pub publication: lance_table::fragment_metadata::SnapshotPolicy,
    /// Buffered commits by default; use bulk for wide metadata rewrites.
    pub materialization: Materialization,
    /// Permit experimental writers with more than a root and one leaf level.
    pub allow_deep_writer: bool,
}

impl FragmentMetadataOptions {
    /// Validate and encode options for `Operation::Overwrite.config_upsert_values`.
    /// Layout and structural budgets cannot change on an existing dataset.
    pub fn into_table_config(self) -> Result<HashMap<String, String>> {
        self.tree.validate()?;
        Ok(HashMap::from([
            (
                lance_table::fragment_metadata::MANIFEST_LAYOUT_KEY.into(),
                "tree".into(),
            ),
            (
                MAX_NODE_BYTES_KEY.into(),
                self.tree.max_node_bytes.to_string(),
            ),
            (
                MAX_LEAF_BYTES_KEY.into(),
                self.tree.max_leaf_bytes.to_string(),
            ),
            (
                "lance.fragment_metadata.semantic_buffer_bytes".into(),
                self.tree.semantic_buffer_bytes.to_string(),
            ),
            (
                "lance.fragment_metadata.hard_capacity_bytes".into(),
                self.tree.hard_capacity_bytes.to_string(),
            ),
            (
                publication::INLINE_ROOT_BYTES_KEY.into(),
                self.publication.inline_root_bytes.to_string(),
            ),
            (
                publication::SUFFIX_BYTES_KEY.into(),
                self.publication.max_suffix_bytes.to_string(),
            ),
            (
                "lance.fragment_metadata.materialization".into(),
                match self.materialization {
                    Materialization::Buffered => "buffered",
                    Materialization::Bulk => "bulk",
                }
                .into(),
            ),
            (
                "lance.fragment_metadata.allow_deep_writer".into(),
                self.allow_deep_writer.to_string(),
            ),
        ]))
    }
}

/// Everything the fragment metadata commit path needs from `CommitBuilder::execute_inner`.
#[derive(Clone)]
pub(crate) struct CommitTarget {
    pub context: Option<Arc<Dataset>>,
    pub materialize_fragments: bool,
    pub object_store: Arc<ObjectStore>,
    pub base_path: Path,
    pub uri: String,
    pub session: Arc<Session>,
    pub commit_handler: Arc<dyn CommitHandler>,
}

/// Whether `operation` is an Overwrite that selects the fragment metadata tree.
///
/// Rejects unknown layout values instead of ignoring them, so a typo cannot
/// silently create a flat dataset.
pub(crate) fn create_requested(operation: &Operation) -> Result<bool> {
    if let Operation::Overwrite {
        config_upsert_values: Some(config),
        ..
    } = operation
    {
        return Ok(ManifestLayout::from_config(config)? == ManifestLayout::Tree);
    }
    Ok(false)
}

/// Whether an opened manifest stores its fragment state in the tree.
pub(crate) fn dataset_uses_fragment_metadata(manifest: &Manifest) -> bool {
    manifest.fragment_metadata.is_some()
}

/// Bootstrap a fragment metadata tree from an Overwrite transaction.
pub(crate) async fn execute_create(
    target: CommitTarget,
    transaction: &Transaction,
    write_config: &super::ManifestWriteConfig,
) -> Result<Dataset> {
    let Operation::Overwrite {
        config_upsert_values: Some(config_upsert),
        ..
    } = &transaction.operation
    else {
        return Err(Error::invalid_input(
            "fragment metadata tree create requires an Overwrite operation with config_upsert_values",
        ));
    };
    let tree_config = tree_config_from(config_upsert)?;
    super::transaction::validate_operation(None, &transaction.operation)?;
    // Keep create semantics (IDs, base paths, format inference, schema and
    // feature flags) in the same authoritative builder as flat manifests.
    let (mut manifest, _) =
        transaction.build_manifest(None, Vec::new(), "", &write_config.to_build_config())?;
    // The tree's writer invariant needs known counts. Production writers
    // always produce them; converting older data computes a missing
    // deletion count from its deletion vector, and refuses a missing
    // physical count (computing that means reading data files, which is a
    // migration, not a create).
    let mut fragments = manifest.fragments.as_ref().clone();
    for fragment in &mut fragments {
        if let Some(deletion_file) = &mut fragment.deletion_file
            && deletion_file.num_deleted_rows.is_none()
        {
            let deletion_vector = read_deletion_file(
                fragment.id,
                deletion_file,
                &target.base_path,
                &target.object_store,
            )
            .await?;
            deletion_file.num_deleted_rows = Some(deletion_vector.len());
        }
    }
    let transaction_file = if write_config.disable_transaction_file() {
        None
    } else {
        Some(
            write_transaction_file(
                &target.object_store,
                &target.base_path,
                &pb::Transaction::from(transaction),
            )
            .await?,
        )
    };
    manifest.set_fragments(Vec::new());
    let policy = publication::policy(&manifest)?;
    let bootstrap = FragmentMetadataTree::bootstrap_snapshot(
        target.object_store.clone(),
        target.base_path.clone(),
        scheduler_for(&target.object_store),
        Arc::new(LanceCache::with_capacity(0)),
        tree_config,
        fragments,
        1,
        policy,
    )
    .await;
    match bootstrap {
        Ok((tree, snapshot, _)) => {
            publication::publish(
                target,
                tree,
                manifest,
                snapshot,
                transaction_file,
                Vec::new(),
                write_config,
                Some(transaction),
            )
            .await
        }
        Err(error) => {
            if let Some(transaction_file) = transaction_file {
                cleanup_transaction_file(
                    &target.object_store,
                    &target.base_path,
                    &transaction_file,
                )
                .await;
            }
            Err(error)
        }
    }
}

/// Commit one production transaction against an existing fragment metadata tree.
///
/// `affected_rows` carries a delete or update's row-level intent, exactly as
/// `CommitBuilder::with_affected_rows` does on the flat path; without it a
/// same-fragment delete conflict cannot rebase and is rejected as retryable.
pub(crate) async fn execute_commit(
    target: CommitTarget,
    commit_config: &CommitConfig,
    transaction: &Transaction,
    affected_rows: Option<&RowAddrTreeMap>,
    write_config: &super::ManifestWriteConfig,
) -> Result<Dataset> {
    execute_commit_with_timeout(
        target,
        commit_config,
        crate::io::commit::DEFAULT_COMMIT_RETRY_TIMEOUT,
        transaction,
        affected_rows,
        write_config,
    )
    .await
}

/// Every CAS attempt uses the native conflict resolver and manifest builder.
/// Only the resulting fragment changes are translated into tree mutations.
pub(crate) async fn execute_commit_with_timeout(
    target: CommitTarget,
    commit_config: &CommitConfig,
    retry_timeout: Duration,
    transaction: &Transaction,
    affected_rows: Option<&RowAddrTreeMap>,
    write_config: &super::ManifestWriteConfig,
) -> Result<Dataset> {
    let attempts = commit_config.num_retries.max(1);
    let start = Instant::now();
    let mut backoff = SlotBackoff::default();
    for attempt in 1..=attempts {
        let mut dataset = publication::open_dataset(&target, None).await?;
        lance_table::feature_flags::ensure_can_write_manifest(&dataset.manifest)?;
        let lazy = dataset.lazy_fragments.as_ref().ok_or_else(|| {
            Error::internal("opened fragment metadata dataset has no fragment source")
        })?;
        let mut tree = lazy.tree().as_ref().clone();
        let previous_snapshot = publication::descriptor(&dataset.manifest)?;
        // A retained handle is useful only for the exact state opened by this
        // attempt. Rebase and retries must not carry stale fragment metadata.
        if let Some(context) = target.context.as_ref().filter(|context| {
            context.lazy_fragments.is_none()
                && context.manifest.version == dataset.manifest.version
                && context.manifest.fragment_metadata == dataset.manifest.fragment_metadata
                && context.base == dataset.base
                && Arc::ptr_eq(&context.object_store.inner, &dataset.object_store.inner)
        }) {
            let mut manifest = dataset.manifest.as_ref().clone();
            manifest.fragments = context.manifest.fragments.clone();
            dataset.manifest = Arc::new(manifest);
            dataset.fragment_bitmap = context.fragment_bitmap.clone();
            dataset.lazy_fragments = None;
        }
        if transaction.read_version > tree.version() {
            return Err(Error::invalid_input(format!(
                "Transaction read version {} exceeds current version {}",
                transaction.read_version,
                tree.version()
            )));
        }
        let mut rebased = transaction.clone();
        let strict_overwrite = matches!(transaction.operation, Operation::Overwrite { .. })
            && commit_config.num_retries == 0;
        if strict_overwrite && transaction.read_version != tree.version() {
            return Err(Error::commit_conflict_source(
                transaction.read_version + 1,
                "Strict overwrite cannot rebase onto a newer version".into(),
            ));
        }
        if transaction.read_version < tree.version()
            && !(transaction.read_version == 0
                && matches!(transaction.operation, Operation::Overwrite { .. }))
        {
            let mut resolver = crate::io::commit::conflict_resolver::TransactionRebase::try_new(
                &dataset,
                rebased,
                affected_rows,
            )
            .await?;
            for (version, other) in
                publication::history(&target, transaction.read_version, tree.version()).await?
            {
                resolver.check_txn(&other, version)?;
            }
            rebased = resolver.finish(&dataset).await?;
        }
        let bulk = match dataset
            .manifest
            .config
            .get("lance.fragment_metadata.materialization")
            .map(String::as_str)
        {
            None | Some("buffered") => false,
            Some("bulk") => true,
            Some(value) => {
                return Err(Error::invalid_input(format!(
                    "lance.fragment_metadata.materialization must be buffered or bulk, got {value:?}"
                )));
            }
        };
        let prepared = native::prepare(
            &dataset,
            &mut tree,
            &rebased,
            transaction.read_version,
            write_config,
            bulk,
        )
        .await?;
        let fragments = target
            .materialize_fragments
            .then(|| prepared.materialized_fragments(&dataset))
            .flatten();
        let policy = publication::policy(&prepared.manifest)?;
        let transaction_file = if write_config.disable_transaction_file() {
            None
        } else {
            Some(
                write_transaction_file(
                    &target.object_store,
                    &target.base_path,
                    &pb::Transaction::from(&rebased),
                )
                .await?,
            )
        };
        let (snapshot, _) = tree
            .prepare_snapshot(
                prepared.changes,
                &prepared.touched,
                &previous_snapshot,
                policy,
                bulk,
            )
            .await?;
        match publication::publish(
            target.clone(),
            tree,
            prepared.manifest,
            snapshot,
            transaction_file,
            prepared.indices,
            write_config,
            Some(&rebased),
        )
        .await
        {
            Ok(mut committed) => {
                if let Some(fragments) = fragments {
                    committed.set_materialized_fragments(fragments);
                }
                if !commit_config.skip_auto_cleanup {
                    match super::cleanup::auto_cleanup_hook(&dataset, &committed.manifest).await {
                        Ok(Some(stats)) => log::info!("Auto cleanup triggered: {stats:?}"),
                        Err(error) => {
                            log::error!("Error encountered during auto_cleanup_hook: {error}")
                        }
                        _ => {}
                    }
                }
                return Ok(committed);
            }
            Err(Error::CommitConflict { .. }) if attempt < attempts => {
                if attempt == 1 {
                    backoff =
                        backoff.with_unit(start.elapsed().as_millis().min(u32::MAX as u128) as u32);
                }
                if start.elapsed() >= retry_timeout {
                    return Err(crate::io::commit::timeout_error(retry_timeout, attempt));
                }
                crate::io::commit::maybe_timeout(
                    attempt,
                    start,
                    retry_timeout,
                    tokio::time::sleep(backoff.next_backoff()),
                )
                .await?;
            }
            Err(error) => return Err(error),
        }
    }
    Err(Error::internal(
        "fragment metadata commit exhausted its nonempty attempt loop",
    ))
}

/// Copy a source tree's dest-owned root into `manifest`, stamping inherited
/// child and file references with `source_base_id`. Shared leaves stay in the
/// source dataset.
pub(crate) async fn clone_fragment_metadata(
    dest_store: Arc<ObjectStore>,
    dest_base: Path,
    source_store: Arc<ObjectStore>,
    source_base: Path,
    source_base_id: u32,
    manifest: &mut Manifest,
) -> Result<()> {
    let snapshot = publication::descriptor(manifest)?;
    let source_nodes = NodeStore::new(
        source_store.clone(),
        source_base.clone(),
        scheduler_for(&source_store),
        Arc::new(LanceCache::with_capacity(0)),
    );
    let mut root = match &snapshot.root {
        Some(pb::fragment_metadata_tree::Root::InlineRoot(root)) => root.clone(),
        Some(pb::fragment_metadata_tree::Root::RootPath(path)) => {
            source_nodes.read_root_base(path).await?
        }
        None => {
            return Err(Error::invalid_input(
                "cloned fragment metadata snapshot has no root",
            ));
        }
    };
    root.buffer
        .extend(snapshot.mutations_since_root.iter().cloned());
    root.next_action_sequence = snapshot.next_action_sequence;
    for child in &mut root.children {
        if child.base_id.is_none() {
            child.base_id = Some(source_base_id);
        }
    }
    lance_table::fragment_metadata::store::preserve_inherited_refs(
        &mut root.buffer,
        lance_table::fragment_metadata::store::SourceStore {
            object_store: source_store.as_ref(),
            base: &source_base,
            base_id: source_base_id,
        },
    )
    .await?;
    let dest_nodes = NodeStore::new(
        dest_store.clone(),
        dest_base,
        scheduler_for(&dest_store),
        Arc::new(LanceCache::with_capacity(0)),
    );
    let (path, _) = dest_nodes.write_root_base(&root).await?;
    manifest.fragment_metadata = Some(Arc::new(pb::FragmentMetadataTree {
        root: Some(pb::fragment_metadata_tree::Root::RootPath(path)),
        mutations_since_root: Vec::new(),
        next_action_sequence: snapshot.next_action_sequence,
    }));
    Ok(())
}

/// Bootstrap dest-owned metadata from a complete fragment list. Used by deep
/// clone after files have been copied into the destination dataset.
pub(crate) async fn rebuild_fragment_metadata(
    object_store: Arc<ObjectStore>,
    base: Path,
    manifest: &mut Manifest,
) -> Result<()> {
    let config = tree_config_from(&manifest.config)?;
    let policy = publication::policy(manifest)?;
    let fragments = std::mem::replace(&mut manifest.fragments, Arc::new(Vec::new()));
    manifest.fragment_metadata = None;
    let (tree, snapshot, _) = FragmentMetadataTree::bootstrap_snapshot(
        object_store.clone(),
        base,
        scheduler_for(&object_store),
        Arc::new(LanceCache::with_capacity(0)),
        config,
        Arc::unwrap_or_clone(fragments),
        manifest.version,
        policy,
    )
    .await?;
    if tree.height() > 1
        && manifest
            .config
            .get("lance.fragment_metadata.allow_deep_writer")
            .map(String::as_str)
            != Some("true")
    {
        return Err(Error::not_supported_source(
            "cloned fragment metadata requires lance.fragment_metadata.allow_deep_writer=true"
                .into(),
        ));
    }
    manifest.fragment_metadata = Some(Arc::new(snapshot));
    Ok(())
}

/// Copy external lineage slices into the destination-owned fragment record.
/// These references have no base ID, so leaving a source-relative path in a
/// clone would bind it to the wrong dataset.
pub(crate) async fn inline_clone_lineage(
    store: &ObjectStore,
    base: &Path,
    fragments: &mut [Fragment],
) -> Result<()> {
    lance_table::fragment_metadata::store::inline_external_lineage(store, base, fragments).await
}

/// Production DataReplacement, one group at a time against the touched
/// state, after the transaction-wide rule that every replacement file in
/// one transaction carries the same field set.
fn replacement_actions(
    replacements: &[DataReplacementGroup],
    touched: &TouchedFragments,
) -> Result<Vec<pb::FragmentAction>> {
    let field_sets: HashSet<Vec<i32>> = replacements
        .iter()
        .map(|DataReplacementGroup(_, file)| file.fields.to_vec())
        .collect();
    if field_sets.len() > 1 {
        return Err(Error::invalid_input(format!(
            "All new data files must have the same fields, but found different \
             fields: {field_sets:?}"
        )));
    }
    let mut actions = Vec::with_capacity(replacements.len());
    for DataReplacementGroup(fragment_id, file) in replacements {
        actions.extend(data_replacement(
            touched.get(*fragment_id),
            *fragment_id,
            file,
        )?);
    }
    Ok(actions)
}

fn tree_config_from(config: &HashMap<String, String>) -> Result<FragmentMetadataTreeConfig> {
    for (key, choices) in [
        (
            "lance.fragment_metadata.materialization",
            ["buffered", "bulk"],
        ),
        (
            "lance.fragment_metadata.allow_deep_writer",
            ["false", "true"],
        ),
    ] {
        if let Some(value) = config.get(key)
            && !choices.contains(&value.as_str())
        {
            return Err(Error::invalid_input(format!(
                "{key} must be one of {choices:?}, got {value:?}"
            )));
        }
    }

    for key in config
        .keys()
        .filter(|key| key.starts_with("lance.fragment_metadata."))
    {
        if !matches!(
            key.as_str(),
            MAX_NODE_BYTES_KEY
                | MAX_LEAF_BYTES_KEY
                | "lance.fragment_metadata.semantic_buffer_bytes"
                | "lance.fragment_metadata.hard_capacity_bytes"
                | "lance.fragment_metadata.inline_manifest_budget"
                | "lance.fragment_metadata.publication_suffix_budget"
                | "lance.fragment_metadata.materialization"
                | "lance.fragment_metadata.allow_deep_writer"
        ) {
            return Err(Error::invalid_input(format!(
                "Unknown fragment metadata setting {key}"
            )));
        }
    }
    fn parse(config: &HashMap<String, String>, key: &str) -> Result<Option<u64>> {
        config
            .get(key)
            .map(|raw| {
                raw.parse::<u64>().map_err(|_| {
                    Error::invalid_input(format!("{key} must be an integer, got {raw:?}"))
                })
            })
            .transpose()
    }
    // Opt-in policy defaults. Routing capacity, mutation batching, and encoded
    // leaf locality are separate knobs; the byte limits determine growth.
    let mut tree_config = FragmentMetadataTreeConfig::default();
    if let Some(max_node_bytes) = parse(config, MAX_NODE_BYTES_KEY)? {
        tree_config.max_node_bytes = max_node_bytes;
    }
    if let Some(max_leaf_bytes) = parse(config, MAX_LEAF_BYTES_KEY)? {
        tree_config.max_leaf_bytes = max_leaf_bytes;
    }
    if let Some(bytes) = parse(config, "lance.fragment_metadata.semantic_buffer_bytes")? {
        tree_config.semantic_buffer_bytes = bytes;
    }
    if let Some(bytes) = parse(config, "lance.fragment_metadata.hard_capacity_bytes")? {
        tree_config.hard_capacity_bytes = bytes;
    }
    tree_config.validate()?;
    Ok(tree_config)
}

fn scheduler_for(object_store: &Arc<ObjectStore>) -> Arc<ScanScheduler> {
    ScanScheduler::new(
        object_store.clone(),
        SchedulerConfig::max_bandwidth(object_store),
    )
}

#[cfg(test)]
mod differential;
#[cfg(test)]
mod gate_tests;
#[cfg(test)]
mod publication_tests;

#[cfg(test)]
mod tests {
    use super::test_support::Reader;
    use super::*;
    use crate::dataset::write::CommitBuilder;
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::future::join_all;
    use lance_core::datatypes::Schema;
    use lance_table::format::pb as table_pb;
    use lance_table::fragment_metadata::MANIFEST_LAYOUT_KEY;
    use lance_table::fragment_metadata::support::{
        make_backfill_data_file, make_fragment, make_replacement_data_file,
    };

    fn table_schema() -> Schema {
        Schema::try_from(&ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, false),
        ]))
        .unwrap()
    }

    fn fragment_metadata_config_upsert() -> HashMap<String, String> {
        HashMap::from([
            (
                MANIFEST_LAYOUT_KEY.to_string(),
                lance_table::fragment_metadata::MANIFEST_LAYOUT_TREE.to_string(),
            ),
            (MAX_NODE_BYTES_KEY.to_string(), (16 * 1024).to_string()),
            (
                "lance.fragment_metadata.allow_deep_writer".to_string(),
                "true".to_string(),
            ),
        ])
    }

    fn create_transaction(n: u64, config: HashMap<String, String>) -> Transaction {
        Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: (0..n).map(make_fragment).collect(),
                schema: table_schema(),
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        )
    }

    async fn create_fragment_metadata_dataset(uri: &str, n: u64) -> Dataset {
        CommitBuilder::new(uri)
            .execute(create_transaction(n, fragment_metadata_config_upsert()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn create_reopen_and_resolve_through_commit_builder() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        let dataset = create_fragment_metadata_dataset(uri, 500).await;
        assert_eq!(dataset.manifest.version, 1);
        assert!(dataset_uses_fragment_metadata(&dataset.manifest));
        assert_eq!(dataset.schema().fields.len(), 2);

        let reader = Reader::open_uri(uri).await.unwrap();
        assert_eq!(reader.version(), 1);
        assert_eq!(reader.count_fragments(), 500);
        assert_eq!(reader.resolve_fragment(123).await.unwrap().unwrap().id, 123);

        let opened = crate::dataset::builder::DatasetBuilder::from_uri(uri)
            .load()
            .await
            .unwrap();
        assert!(!opened.fragment_source().is_lazy());
        assert_eq!(opened.fragments().len(), 500);
        assert_eq!(opened.get_fragments().len(), 500);
        assert_eq!(opened.get_fragment(123).unwrap().id(), 123);
        let lazy = crate::dataset::builder::DatasetBuilder::from_uri(uri)
            .load_lazy()
            .await
            .unwrap();
        assert_eq!(lazy.count_fragments(), 500);
        assert_eq!(lazy.get_fragment(123).await.unwrap().unwrap().id, 123);
        assert_eq!(lazy.into_dataset().await.unwrap().fragments().len(), 500);
        assert_eq!(opened.manifest.version, 1);
    }

    #[tokio::test]
    async fn append_add_column_and_replace_commit_through_dataset_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        let dataset = create_fragment_metadata_dataset(uri, 500).await;

        let appended = CommitBuilder::new(Arc::new(dataset))
            .execute(Transaction::new_from_version(
                1,
                Operation::Append {
                    fragments: vec![make_fragment(500)],
                },
            ))
            .await
            .unwrap();
        assert_eq!(appended.manifest.version, 2);

        let add_column = CommitBuilder::new(Arc::new(appended))
            .execute(Transaction::new_from_version(
                2,
                Operation::DataReplacement {
                    replacements: (0..10)
                        .map(|id| DataReplacementGroup(id, make_backfill_data_file(id, 0)))
                        .collect(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(add_column.manifest.version, 3);

        let replaced = CommitBuilder::new(Arc::new(add_column))
            .execute(Transaction::new_from_version(
                3,
                Operation::DataReplacement {
                    replacements: (0..10)
                        .map(|id| DataReplacementGroup(id, make_replacement_data_file(id, 0)))
                        .collect(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(replaced.manifest.version, 4);

        let reader = Reader::open_uri(uri).await.unwrap();
        assert_eq!(reader.count_fragments(), 501);
        let touched = reader.resolve_fragment(3).await.unwrap().unwrap();
        assert_eq!(touched.files.len(), 2);
        assert_eq!(touched.files[0].path, make_replacement_data_file(3, 0).path);
        assert_eq!(touched.files[1].path, make_backfill_data_file(3, 0).path);
        let untouched = reader.resolve_fragment(400).await.unwrap().unwrap();
        assert_eq!(untouched.files.len(), 1);
    }

    #[tokio::test]
    async fn production_transaction_file_stays_action_scale() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        let dataset = create_fragment_metadata_dataset(uri, 2_000).await;

        CommitBuilder::new(Arc::new(dataset))
            .execute(Transaction::new_from_version(
                1,
                Operation::DataReplacement {
                    replacements: (0..10)
                        .map(|id| DataReplacementGroup(id, make_backfill_data_file(id, 0)))
                        .collect(),
                },
            ))
            .await
            .unwrap();

        let transactions_dir = std::fs::read_dir(tempdir.path().join("_transactions")).unwrap();
        let mut backfill_transaction = None;
        for entry in transactions_dir {
            let entry = entry.unwrap();
            let bytes = std::fs::read(entry.path()).unwrap();
            let decoded = table_pb::Transaction::decode(bytes.as_slice()).unwrap();
            if let Some(table_pb::transaction::Operation::DataReplacement(replacement)) =
                decoded.operation
            {
                backfill_transaction = Some((bytes.len(), replacement.replacements.len()));
            }
        }
        let (transaction_bytes, group_count) = backfill_transaction.unwrap();
        assert_eq!(group_count, 10);
        assert!(
            transaction_bytes < 4 * 1024,
            "10-group DataReplacement txn must stay action-scale at N=2000, \
             got {transaction_bytes} bytes"
        );
    }

    #[tokio::test]
    async fn default_layout_still_flat_and_unknown_layout_rejected() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        CommitBuilder::new(uri)
            .execute(create_transaction(5, HashMap::new()))
            .await
            .unwrap();
        assert!(
            tempdir.path().join("_versions").exists(),
            "default create must keep writing flat manifests"
        );
        assert!(!tempdir.path().join("_bt").exists());

        let unknown_dir = tempfile::tempdir().unwrap();
        let unknown_uri = unknown_dir.path().to_str().unwrap();
        let error = CommitBuilder::new(unknown_uri)
            .execute(create_transaction(
                5,
                HashMap::from([(MANIFEST_LAYOUT_KEY.to_string(), "tiered".to_string())]),
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("layout=\"tiered\""), "{error}");
    }

    #[tokio::test]
    async fn concurrent_appends_leave_a_sane_tip() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        create_fragment_metadata_dataset(uri, 100).await;

        let outcomes = join_all((0..4u64).map(|writer| {
            let uri = uri.to_string();
            async move {
                CommitBuilder::new(uri.as_str())
                    .execute(Transaction::new_from_version(
                        1,
                        Operation::Append {
                            fragments: vec![make_fragment(100 + writer)],
                        },
                    ))
                    .await
            }
        }))
        .await;

        let successes = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
        assert!(successes >= 1, "at least one concurrent append must land");

        let reader = Reader::open_uri(uri).await.unwrap();
        assert_eq!(reader.version(), 1 + successes as u64);
        assert_eq!(reader.count_fragments(), 100 + successes as u64);
        for outcome in outcomes.into_iter().flatten() {
            assert!(dataset_uses_fragment_metadata(&outcome.manifest));
        }
    }
}
