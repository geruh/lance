// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Scan metadata from a resident fragment list or a tree stream.
//! Operations requiring the complete list reject a lazy source.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use lance_core::{Error, Result};
use lance_table::format::Fragment;
use lance_table::fragment_metadata::FragmentTree;

/// Fragment metadata read from a tree on demand, in ID order.
pub struct LazyFragments {
    tree: Arc<FragmentTree>,
}

impl std::fmt::Debug for LazyFragments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LazyFragments({:?})", FragmentTreeDebug(&self.tree))
    }
}

impl LazyFragments {
    pub fn new(tree: Arc<FragmentTree>) -> Self {
        Self { tree }
    }

    pub fn tree(&self) -> &Arc<FragmentTree> {
        &self.tree
    }
}

impl std::fmt::Debug for FragmentTreeDebug<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FragmentTree(version={})", self.0.version())
    }
}
struct FragmentTreeDebug<'a>(&'a FragmentTree);

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
            Self::Lazy(lazy) => write!(f, "Lazy({:?})", FragmentTreeDebug(lazy.tree())),
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

    /// Stream fragments without copying the complete list.
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

    /// Return the resident list, or an error if metadata is loaded on demand.
    pub fn materialized(&self, purpose: &str) -> Result<Arc<Vec<Fragment>>> {
        match self {
            Self::Manifest(fragments) => Ok(fragments.clone()),
            Self::Lazy(_) => Err(Error::not_supported_source(
                format!("{purpose} needs the complete fragment list, which a lazily loaded fragment metadata dataset does not hold in memory yet")
                    .into(),
            )),
        }
    }

    /// Visible row count when all fragment counts are available.
    pub fn row_count(&self) -> Option<usize> {
        match self {
            Self::Manifest(fragments) => fragments.iter().map(Fragment::num_rows).sum(),
            Self::Lazy(lazy) => usize::try_from(lazy.tree.count_visible_rows()).ok(),
        }
    }
}
