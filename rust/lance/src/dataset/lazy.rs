// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use futures::{StreamExt, stream::BoxStream};
use lance_core::{Result, datatypes::Schema};
use lance_table::format::Fragment;

use super::{Dataset, refs::Ref, scanner::Scanner, transaction::Transaction, write::CommitBuilder};

/// A dataset whose fragment metadata is read on demand.
///
/// This handle provides streaming metadata, scans without indices, and native
/// transactions. Convert with [`Self::into_dataset`] for APIs that need resident
/// fragments, including index construction and synchronous fragment access.
///
/// ```
/// # use lance::{dataset::builder::DatasetBuilder, Result};
/// # use futures::TryStreamExt;
/// # async fn example(uri: &str) -> Result<()> {
/// let dataset = DatasetBuilder::from_uri(uri).load_lazy().await?;
/// let mut fragments = dataset.fragments_from(100);
/// while let Some(fragment) = fragments.try_next().await? {
///     assert!(fragment.id >= 100);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct LazyDataset {
    dataset: Dataset,
}

impl LazyDataset {
    pub(crate) fn new(dataset: Dataset) -> Self {
        Self { dataset }
    }

    /// Schema of this snapshot; no metadata objects are read.
    pub fn schema(&self) -> &Schema {
        self.dataset.schema()
    }

    /// Published version number of this snapshot.
    pub fn version_id(&self) -> u64 {
        self.dataset.version_id()
    }

    /// Number of live fragments, including buffered changes.
    pub fn count_fragments(&self) -> usize {
        self.dataset.count_fragments()
    }

    /// Count visible rows; an absent filter uses metadata aggregates.
    pub async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        if let Some(filter) = filter {
            let mut scanner = self.scan();
            scanner
                .filter(&filter)?
                .project::<String>(&[])?
                .with_row_id();
            Ok(scanner.count_rows().await? as usize)
        } else {
            self.dataset.count_all_rows().await
        }
    }

    /// Resolve one fragment. Deleted and unallocated IDs return `None`.
    pub async fn get_fragment(&self, id: u64) -> Result<Option<Fragment>> {
        if let Some(lazy) = &self.dataset.lazy_fragments {
            lazy.tree().resolve_fragment(id).await
        } else {
            Ok(self
                .dataset
                .manifest
                .fragments
                .iter()
                .find(|f| f.id == id)
                .cloned())
        }
    }

    /// Resolve distinct live IDs in ascending order, sharing leaf reads.
    pub async fn get_fragments(&self, ids: &[u64]) -> Result<Vec<Fragment>> {
        Ok(self
            .dataset
            .file_fragments_for_ids(ids)
            .await?
            .into_iter()
            .map(|fragment| fragment.metadata().clone())
            .collect())
    }

    /// Stream fragments in ID order, starting at an inclusive lower bound.
    pub fn fragments_from(&self, id: u64) -> BoxStream<'static, Result<Fragment>> {
        if let Some(lazy) = &self.dataset.lazy_fragments {
            lazy.tree().clone().fragment_stream_from(id)
        } else {
            let fragments = self.dataset.manifest.fragments.clone();
            let start = fragments.partition_point(|fragment| fragment.id < id);
            futures::stream::iter(start..fragments.len())
                .map(move |index| Ok(fragments[index].clone()))
                .boxed()
        }
    }

    /// Start a streaming scan without indices. Indexed plans require
    /// [`Self::into_dataset`] to prepare the complete live-fragment bitmap.
    pub fn scan(&self) -> Scanner {
        let mut scanner = self.dataset.scan();
        scanner.use_index(false).use_scalar_index(false);
        scanner
    }

    /// Open another published snapshot with metadata loaded on demand.
    pub async fn checkout_version(&self, version: impl Into<Ref>) -> Result<Self> {
        Ok(Self::new(self.dataset.checkout_version(version).await?))
    }

    /// Commit a native transaction with the standard retry policy.
    ///
    /// The returned snapshot keeps fragment metadata lazy. The original handle
    /// remains pinned to its version if publication succeeds or fails.
    pub async fn commit(&self, transaction: Transaction) -> Result<Self> {
        CommitBuilder::new(Arc::new(self.dataset.clone()))
            .execute_lazy(transaction)
            .await
    }

    /// Read all fragment metadata and return a regular dataset. A failed read
    /// leaves other handles to this snapshot usable.
    pub async fn into_dataset(mut self) -> Result<Dataset> {
        self.dataset.hydrate_fragments_for_maintenance().await?;
        Ok(self.dataset)
    }
}
