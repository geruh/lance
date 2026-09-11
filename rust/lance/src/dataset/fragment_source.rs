// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Where a scan gets its fragments.
//!
//! A flat manifest holds the whole fragment list in memory. A fragment
//! metadata tree yields fragments leaf by leaf, so a scan can start before
//! the metadata for the last fragment has been read. Plan nodes that can
//! consume a stream take a [`FragmentSource`]; plan nodes that need the
//! complete list ask for [`FragmentSource::materialized`] and refuse a lazy
//! source rather than materializing it behind the caller's back.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use lance_core::{Error, Result};
use lance_table::format::Fragment;
use lance_table::fragment_metadata::FragmentMetadataTree;

/// Fragments served from a fragment metadata tree, in id order. Hydration
/// prefetches up to the store's I/O parallelism in leaves. A streaming scan
/// keeps a bounded number of leaf reads in flight.
pub struct LazyFragments {
    tree: Arc<FragmentMetadataTree>,
}

impl std::fmt::Debug for LazyFragments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LazyFragments({:?})",
            FragmentMetadataTreeDebug(&self.tree)
        )
    }
}

impl LazyFragments {
    pub fn new(tree: Arc<FragmentMetadataTree>) -> Self {
        Self { tree }
    }

    pub fn tree(&self) -> &Arc<FragmentMetadataTree> {
        &self.tree
    }
}

impl std::fmt::Debug for FragmentMetadataTreeDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FragmentMetadataTree(version={})", self.0.version())
    }
}
struct FragmentMetadataTreeDebug<'a>(&'a FragmentMetadataTree);

#[derive(Clone)]
pub enum FragmentSource {
    /// The manifest's fragment list, already in memory.
    Manifest(Arc<Vec<Fragment>>),
    /// A stream from the fragment metadata tree.
    Lazy(Arc<LazyFragments>),
}

impl std::fmt::Debug for FragmentSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manifest(fragments) => write!(f, "Manifest({} fragments)", fragments.len()),
            Self::Lazy(lazy) => write!(f, "Lazy({:?})", FragmentMetadataTreeDebug(lazy.tree())),
        }
    }
}

impl From<Arc<Vec<Fragment>>> for FragmentSource {
    fn from(fragments: Arc<Vec<Fragment>>) -> Self {
        Self::Manifest(fragments)
    }
}

impl From<Vec<Fragment>> for FragmentSource {
    fn from(fragments: Vec<Fragment>) -> Self {
        Self::Manifest(Arc::new(fragments))
    }
}

impl FragmentSource {
    pub fn is_lazy(&self) -> bool {
        matches!(self, Self::Lazy(_))
    }

    /// Fragments in id order. Manifest clones one fragment per yield from the
    /// shared `Arc`, not a second full `Vec` before the first item.
    pub fn stream(&self) -> BoxStream<'static, Result<Fragment>> {
        match self {
            Self::Manifest(fragments) => {
                let fragments = Arc::clone(fragments);
                futures::stream::iter(0..fragments.len())
                    .map(move |idx| Ok(fragments[idx].clone()))
                    .boxed()
            }
            Self::Lazy(lazy) => lazy.tree.clone().fragment_stream(),
        }
    }

    /// The complete list, for plan nodes that need random access. A lazy
    /// source refuses instead of collecting: the caller must plan through the
    /// streaming scan or gain an explicit full-list path.
    pub fn materialized(&self, purpose: &str) -> Result<Arc<Vec<Fragment>>> {
        match self {
            Self::Manifest(fragments) => Ok(fragments.clone()),
            Self::Lazy(_) => Err(Error::not_supported_source(
                format!("{purpose} needs the complete fragment list, which a lazily loaded fragment metadata dataset does not hold in memory yet")
                    .into(),
            )),
        }
    }

    /// Visible row count. A flat source may lack legacy statistics; a tree
    /// admits only known counts and maintains exact visible-row aggregates.
    pub fn row_count(&self) -> Option<FragmentRowCount> {
        match self {
            Self::Manifest(fragments) => {
                let sum: Option<usize> = fragments.iter().map(Fragment::num_rows).sum();
                sum.map(FragmentRowCount::Exact)
            }
            Self::Lazy(lazy) => usize::try_from(lazy.tree.count_visible_rows())
                .ok()
                .map(FragmentRowCount::Exact),
        }
    }
}

/// Row count from a fragment source, with how precise the value is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FragmentRowCount {
    Exact(usize),
    Inexact(usize),
}

impl FragmentRowCount {
    pub fn get(self) -> usize {
        match self {
            Self::Exact(n) | Self::Inexact(n) => n,
        }
    }
}
