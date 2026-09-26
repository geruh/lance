// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The Version Manifest is the only publication authority. Tree objects are
//! immutable dependencies; raw transaction records are used only for conflicts.

use super::*;
use crate::io::commit::{cleanup_transaction_file, write_transaction_file};
use lance_io::object_store::ObjectStoreRegistry;
use lance_table::fragment_metadata::SnapshotPolicy;
use lance_table::io::commit::CommitError;
use lance_table::io::manifest::read_manifest;
use prost::Message;

pub(super) const INLINE_ROOT_BYTES_KEY: &str = "lance.fragment_metadata.inline_manifest_budget";
pub(super) const SUFFIX_BYTES_KEY: &str = "lance.fragment_metadata.publication_suffix_budget";

pub(super) async fn publish(
    target: CommitTarget,
    mut tree: FragmentTree,
    mut manifest: Manifest,
    snapshot: pb::FragmentTree,
    indices: Vec<lance_table::format::IndexMetadata>,
    write_config: &super::super::ManifestWriteConfig,
    transaction: &Transaction,
) -> Result<Dataset> {
    // Resolve dataset-root stores before writing the Version Manifest.
    // A later failure would report an error after the version is already visible.
    let foreign_bases = Dataset::resolve_tree_foreign_bases(
        target.session.store_registry(),
        &target.uri,
        &target.object_store,
        &manifest,
        target.context.as_deref(),
    )
    .await?;
    manifest.version = tree.version();
    if tree.count_visible_rows() < tree.count_rows() {
        manifest.reader_feature_flags |= lance_table::feature_flags::FLAG_DELETION_FILES;
    }
    manifest.max_fragment_id = tree
        .next_fragment_id()
        .checked_sub(1)
        .map(|id| {
            u32::try_from(id).map_err(|_| {
                Error::invalid_input(format!(
                    "Fragment ID {id} exceeds Lance's u32 address space"
                ))
            })
        })
        .transpose()?;
    manifest.fragment_tree = Some(Arc::new(snapshot));
    let encoded = pb::Transaction::from(transaction);
    manifest.transaction_file = if write_config.disable_transaction_file() {
        None
    } else {
        Some(write_transaction_file(&target.object_store, &target.base_path, &encoded).await?)
    };
    manifest.transaction_section = None;
    let may_change_schema = lance_table::format::operation_may_change_schema(&encoded);
    let location = super::super::write_manifest_file(
        &target.object_store,
        target.commit_handler.as_ref(),
        &target.base_path,
        &mut manifest,
        (!indices.is_empty()).then_some(indices),
        write_config,
        ManifestNamingScheme::V2,
        (encoded.encoded_len() <= crate::io::commit::MAX_INLINE_TRANSACTION_BYTES)
            .then(|| lance_table::format::Transaction::from(encoded)),
        may_change_schema,
    )
    .await;
    let location = match location {
        Ok(location) => location,
        Err(error) => {
            // Use the same outcome verification and handler policy as flat
            // commits. An unavailable readback must not look safe to retry.
            let was_conflict = matches!(error, CommitError::CommitConflict);
            let error = match error {
                CommitError::CommitConflict => Error::commit_conflict_source(
                    manifest.version,
                    "Another writer published this Version Manifest".into(),
                ),
                CommitError::OtherError(error) => error,
            };
            let outcome = crate::io::commit::verify_commit_outcome(
                &target.object_store,
                target.commit_handler.as_ref(),
                &target.base_path,
                manifest.version,
                transaction,
            )
            .await;
            match outcome {
                crate::io::commit::CommitOutcome::Ours {
                    manifest: published,
                    location,
                } if published.fragment_tree == manifest.fragment_tree => {
                    if !was_conflict && target.commit_handler.propagate_commit_error_after_success()
                    {
                        return Err(error);
                    }
                    manifest = *published;
                    location
                }
                crate::io::commit::CommitOutcome::Unknown => {
                    return Err(Error::commit_status_unknown_source(
                        manifest.version,
                        Box::new(error),
                    ));
                }
                crate::io::commit::CommitOutcome::Foreign
                | crate::io::commit::CommitOutcome::Absent => {
                    if let Some(transaction_file) = manifest.transaction_file.as_deref() {
                        cleanup_transaction_file(
                            &target.object_store,
                            &target.base_path,
                            transaction_file,
                        )
                        .await;
                    }
                    return Err(error);
                }
                _ => return Err(error),
            }
        }
    };
    let manifest = Arc::new(manifest);
    // Cache the published manifest for subsequent commits in this session.
    target
        .session
        .metadata_cache
        .for_dataset(&target.uri)
        .insert_with_key(
            &crate::session::caches::ManifestKey {
                version: location.version,
                e_tag: location.e_tag.as_deref(),
            },
            manifest.clone(),
        )
        .await;
    let dataset = Dataset::checkout_manifest(
        target.object_store,
        target.base_path,
        target.uri,
        manifest,
        location,
        target.session,
        target.commit_handler,
        target
            .context
            .as_ref()
            .and_then(|dataset| dataset.file_reader_options.clone()),
        target
            .context
            .as_ref()
            .and_then(|dataset| dataset.store_params.as_deref().cloned()),
        target
            .context
            .as_ref()
            .and_then(|dataset| dataset.base_store_params.clone()),
    )?;
    tree.set_foreign_bases(foreign_bases);
    Ok(dataset.with_prepared_tree(tree))
}

#[cfg(test)]
pub(super) async fn open_dataset(target: &CommitTarget, version: Option<u64>) -> Result<Dataset> {
    let (manifest, location) = read_version(
        &target.object_store,
        &target.base_path,
        target.commit_handler.as_ref(),
        version,
    )
    .await?;
    checkout(target, Arc::new(manifest), location).await
}

/// Resolve the latest stored manifest for this attempt.
pub(super) async fn open_latest(target: &CommitTarget) -> Result<Dataset> {
    let location = target
        .commit_handler
        .resolve_latest_location(&target.base_path, &target.object_store)
        .await?;
    let manifest = Dataset::get_manifest(
        &target.object_store,
        &location,
        &target.uri,
        target.session.as_ref(),
    )
    .await?;
    checkout(target, manifest, location).await
}

async fn checkout(
    target: &CommitTarget,
    manifest: Arc<Manifest>,
    location: ManifestLocation,
) -> Result<Dataset> {
    Dataset::checkout_manifest(
        target.object_store.clone(),
        target.base_path.clone(),
        target.uri.clone(),
        manifest,
        location,
        target.session.clone(),
        target.commit_handler.clone(),
        target
            .context
            .as_ref()
            .and_then(|dataset| dataset.file_reader_options.clone()),
        target
            .context
            .as_ref()
            .and_then(|dataset| dataset.store_params.as_deref().cloned()),
        target
            .context
            .as_ref()
            .and_then(|dataset| dataset.base_store_params.clone()),
    )?
    .attach_fragment_source()
    .await
}

/// Open `manifest`'s tree with its dataset-root bases parsed by `registry`,
/// which must know every scheme the dataset's session knows.
pub async fn tree_from_manifest(
    store: Arc<ObjectStore>,
    registry: Arc<ObjectStoreRegistry>,
    base: Path,
    manifest: &Manifest,
) -> Result<FragmentTree> {
    let snapshot = descriptor(manifest)?;
    let mut tree = FragmentTree::open_snapshot(
        store.clone(),
        base,
        scheduler_for(&store),
        Arc::new(LanceCache::with_capacity(0)),
        &snapshot,
        manifest.version,
        super::tree_config_from(&manifest.config)?,
        next_fragment_id(manifest),
    )
    .await?;
    tree.set_foreign_bases(foreign_bases_from_manifest(&store, registry, manifest)?);
    Ok(tree)
}

pub(super) async fn history(
    target: &CommitTarget,
    since: u64,
    through: u64,
) -> Result<Vec<(u64, Transaction)>> {
    let mut history = Vec::new();
    for version in since + 1..=through {
        let (manifest, location) = read_version(
            &target.object_store,
            &target.base_path,
            target.commit_handler.as_ref(),
            Some(version),
        )
        .await?;
        let transaction = crate::io::commit::read_manifest_transaction(
            &target.object_store,
            &target.base_path,
            &manifest,
            &location,
        )
        .await?
        .ok_or_else(|| {
            Error::invalid_input(format!(
                "Version {version} has no transaction history; cannot rebase"
            ))
        })?;
        history.push((version, transaction));
    }
    Ok(history)
}

pub(super) async fn read_version(
    store: &ObjectStore,
    base: &Path,
    handler: &dyn CommitHandler,
    version: Option<u64>,
) -> Result<(Manifest, ManifestLocation)> {
    let location = match version {
        Some(version) => {
            handler
                .resolve_version_location(base, version, &store.inner)
                .await?
        }
        None => handler.resolve_latest_location(base, store).await?,
    };
    let manifest = read_manifest(store, &location.path, location.size).await?;
    Ok((manifest, location))
}

pub(super) fn policy(manifest: &Manifest) -> Result<SnapshotPolicy> {
    let defaults = SnapshotPolicy::default();
    let parse = |key: &str, default| {
        manifest.config.get(key).map_or(Ok(default), |value| {
            value.parse::<usize>().map_err(|_| {
                Error::invalid_input(format!("{key} must be a byte count, got {value:?}"))
            })
        })
    };
    Ok(SnapshotPolicy {
        inline_root_bytes: parse(INLINE_ROOT_BYTES_KEY, defaults.inline_root_bytes)?,
        max_suffix_bytes: parse(SUFFIX_BYTES_KEY, defaults.max_suffix_bytes)?,
    })
}

pub(super) fn descriptor(manifest: &Manifest) -> Result<pb::FragmentTree> {
    let snapshot = manifest.fragment_tree.as_ref().ok_or_else(|| {
        Error::invalid_input(format!(
            "fragment metadata tree version {} has no descriptor",
            manifest.version
        ))
    })?;
    Ok(snapshot.as_ref().clone())
}

/// The next allocatable fragment id, from the manifest's high-water mark.
pub(super) fn next_fragment_id(manifest: &Manifest) -> u64 {
    manifest.max_fragment_id.map_or(0, |id| u64::from(id) + 1)
}

fn foreign_bases_from_manifest(
    store: &Arc<ObjectStore>,
    registry: Arc<ObjectStoreRegistry>,
    manifest: &Manifest,
) -> Result<HashMap<u32, (Arc<ObjectStore>, Path)>> {
    let mut foreign = HashMap::new();
    for (id, base_path) in &manifest.base_paths {
        if !base_path.is_dataset_root {
            continue;
        }
        let path = base_path.extract_path(registry.clone())?;
        foreign.insert(*id, (store.clone(), path));
    }
    Ok(foreign)
}
