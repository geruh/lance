// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Tiered manifest layout (discussion #5947).
//!
//! Root holds child refs plus a bounded buffer; overflow seals into immutable
//! files under [`MANIFEST_CHILDREN_DIR`]. Pure IO-free primitives here; object
//! store paths live in `lance-table::io` and commit sealing in `lance::dataset::tiered`.

use deepsize::DeepSizeOf;
use uuid::Uuid;

use super::Fragment;
use crate::format::pb;

pub const MANIFEST_CHILDREN_DIR: &str = "_manifest_children";
pub const MANIFEST_LAYOUT_KEY: &str = "lance.manifest.layout";
pub const MANIFEST_BUFFER_CAP_KEY: &str = "lance.manifest.buffer_cap";
pub const MANIFEST_LAYOUT_TIERED: &str = "tiered";
pub const MANIFEST_LAYOUT_FLAT: &str = "flat";
pub const DEFAULT_MANIFEST_BUFFER_CAP: usize = 100_000;

#[derive(Debug, Clone, PartialEq, Eq, DeepSizeOf)]
pub struct FragmentManifestRef {
    pub path: String,
    pub min_fragment_id: u64,
    pub max_fragment_id: u64,
    pub row_offset_start: u64,
    pub total_rows: u64,
    pub fragment_count: u32,
    pub byte_size: u64,
}

impl FragmentManifestRef {
    pub fn size_hint(&self) -> Option<u64> {
        (self.byte_size > 0).then_some(self.byte_size)
    }
}

impl From<&FragmentManifestRef> for pb::FragmentManifestRef {
    fn from(r: &FragmentManifestRef) -> Self {
        Self {
            path: r.path.clone(),
            min_fragment_id: r.min_fragment_id,
            max_fragment_id: r.max_fragment_id,
            row_offset_start: r.row_offset_start,
            total_rows: r.total_rows,
            fragment_count: r.fragment_count,
            byte_size: r.byte_size,
        }
    }
}

impl From<pb::FragmentManifestRef> for FragmentManifestRef {
    fn from(p: pb::FragmentManifestRef) -> Self {
        Self {
            path: p.path,
            min_fragment_id: p.min_fragment_id,
            max_fragment_id: p.max_fragment_id,
            row_offset_start: p.row_offset_start,
            total_rows: p.total_rows,
            fragment_count: p.fragment_count,
            byte_size: p.byte_size,
        }
    }
}

pub fn spilled_rows(children: &[FragmentManifestRef]) -> u64 {
    children
        .last()
        .map_or(0, |c| c.row_offset_start + c.total_rows)
}

pub fn seal_run(
    fragments: &[Fragment],
    path: String,
    row_offset_start: u64,
) -> FragmentManifestRef {
    debug_assert!(
        !fragments.is_empty(),
        "cannot seal an empty run of fragments"
    );
    let total_rows = run_rows(fragments);
    FragmentManifestRef {
        path,
        min_fragment_id: fragments.first().map_or(0, |f| f.id),
        max_fragment_id: fragments.last().map_or(0, |f| f.id),
        row_offset_start,
        total_rows,
        fragment_count: fragments.len() as u32,
        byte_size: 0,
    }
}

pub fn child_path(version: u64, min_fragment_id: u64, max_fragment_id: u64) -> String {
    let unique = Uuid::new_v4().simple();
    format!(
        "{MANIFEST_CHILDREN_DIR}/v{version}-{min_fragment_id}-{max_fragment_id}-{unique}.manifest"
    )
}

fn run_rows(fragments: &[Fragment]) -> u64 {
    fragments
        .iter()
        .map(|f| f.num_rows().unwrap_or_default() as u64)
        .sum()
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLocation {
    Buffer { local_row: u64 },
    Child { index: usize, local_row: u64 },
}

#[cfg(test)]
pub fn locate_row(
    children: &[FragmentManifestRef],
    buffer_rows: u64,
    row: u64,
) -> Option<RowLocation> {
    let spilled = spilled_rows(children);
    if row < spilled {
        let index = children.partition_point(|c| c.row_offset_start <= row) - 1;
        Some(RowLocation::Child {
            index,
            local_row: row - children[index].row_offset_start,
        })
    } else if row < spilled + buffer_rows {
        Some(RowLocation::Buffer {
            local_row: row - spilled,
        })
    } else {
        None
    }
}

#[cfg(test)]
pub fn fragment_at_local_row(fragments: &[Fragment], mut local_row: u64) -> Option<u64> {
    for fragment in fragments {
        let rows = fragment.num_rows().unwrap_or_default() as u64;
        if local_row < rows {
            return Some(fragment.id);
        }
        local_row -= rows;
    }
    None
}

#[cfg(test)]
#[derive(Debug)]
pub struct TieredLayout {
    buffer_cap: usize,
    version: u64,
    children: Vec<SealedChild>,
    buffer: Vec<Fragment>,
    spilled_rows: u64,
}

#[cfg(test)]
#[derive(Debug)]
pub struct SealedChild {
    pub reference: FragmentManifestRef,
    pub fragments: Vec<Fragment>,
}

#[cfg(test)]
impl TieredLayout {
    pub fn new(buffer_cap: usize, version: u64) -> Self {
        assert!(buffer_cap >= 1, "buffer_cap must be at least 1");
        Self {
            buffer_cap,
            version,
            children: Vec::new(),
            buffer: Vec::new(),
            spilled_rows: 0,
        }
    }

    pub fn seal(
        fragments: impl IntoIterator<Item = Fragment>,
        buffer_cap: usize,
        version: u64,
    ) -> Self {
        let mut layout = Self::new(buffer_cap, version);
        for fragment in fragments {
            layout.push(fragment);
        }
        layout
    }

    pub fn push(&mut self, fragment: Fragment) {
        self.buffer.push(fragment);
        while self.buffer.len() > self.buffer_cap {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let take = self.buffer_cap.min(self.buffer.len());
        let run: Vec<Fragment> = self.buffer.drain(0..take).collect();
        let reference = seal_run(
            &run,
            child_path(
                self.version,
                run.first().map_or(0, |f| f.id),
                run.last().map_or(0, |f| f.id),
            ),
            self.spilled_rows,
        );
        self.spilled_rows += reference.total_rows;
        self.children.push(SealedChild {
            reference,
            fragments: run,
        });
    }

    pub fn children(&self) -> &[SealedChild] {
        &self.children
    }

    pub fn buffer(&self) -> &[Fragment] {
        &self.buffer
    }

    pub fn child_refs(&self) -> Vec<FragmentManifestRef> {
        self.children.iter().map(|c| c.reference.clone()).collect()
    }

    pub fn enumerate(&self) -> Vec<Fragment> {
        let mut all = Vec::with_capacity(self.spilled_rows as usize + self.buffer.len());
        for child in &self.children {
            all.extend_from_slice(&child.fragments);
        }
        all.extend_from_slice(&self.buffer);
        all
    }

    pub fn resolve_fragment(&self, row: u64) -> Option<u64> {
        let buffer_rows = run_rows(&self.buffer);
        match locate_row(&self.child_refs(), buffer_rows, row)? {
            RowLocation::Buffer { local_row } => fragment_at_local_row(&self.buffer, local_row),
            RowLocation::Child { index, local_row } => {
                fragment_at_local_row(&self.children[index].fragments, local_row)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic splitmix64.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn range(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn fragment(id: u64, num_rows: usize) -> Fragment {
        Fragment::new(id).with_physical_rows(num_rows)
    }

    fn flat_resolve(fragments: &[Fragment], row: u64) -> u64 {
        let mut offset = 0u64;
        for f in fragments {
            let rows = f.num_rows().unwrap_or_default() as u64;
            if row < offset + rows {
                return f.id;
            }
            offset += rows;
        }
        panic!("row {row} out of range");
    }

    #[test]
    fn tiered_matches_flat_oracle() {
        const SEEDS: u64 = 40;
        for seed in 0..SEEDS {
            let mut rng = Rng(0xBE_5947 ^ seed);
            let buffer_cap = (1 + rng.range(20)) as usize;
            let appends = 50 + rng.range(450);

            let mut flat = Vec::new();
            let mut tiered = TieredLayout::new(buffer_cap, 1);

            for frag_id in 0..appends {
                let num_rows = (1 + rng.range(1000)) as usize;
                let f = fragment(frag_id, num_rows);
                flat.push(f.clone());
                tiered.push(f);

                assert_eq!(
                    tiered.enumerate(),
                    flat,
                    "enumerate mismatch (seed {seed}, cap {buffer_cap}, after frag {frag_id})"
                );
            }

            for child in tiered.children() {
                assert_eq!(child.fragments.len(), buffer_cap);
                assert_eq!(child.reference.fragment_count as usize, buffer_cap);
            }
            assert_eq!(
                tiered.children().len() * buffer_cap + tiered.buffer().len(),
                flat.len()
            );

            let mut expected_offset = 0u64;
            for child in tiered.children() {
                assert_eq!(child.reference.row_offset_start, expected_offset);
                expected_offset += child.reference.total_rows;
            }
            assert_eq!(tiered.spilled_rows, expected_offset);

            let total_rows: u64 = flat.iter().map(|f| f.num_rows().unwrap() as u64).sum();
            for _ in 0..2_000 {
                let row = rng.range(total_rows);
                assert_eq!(
                    tiered.resolve_fragment(row),
                    Some(flat_resolve(&flat, row)),
                    "row route mismatch (seed {seed}, cap {buffer_cap}, row {row})"
                );
            }
        }
    }

    #[test]
    fn seal_is_equivalent_to_incremental_push() {
        let fragments: Vec<Fragment> = (0..250).map(|id| fragment(id, 7)).collect();
        let sealed = TieredLayout::seal(fragments.clone(), 100, 3);

        let mut pushed = TieredLayout::new(100, 3);
        for f in &fragments {
            pushed.push(f.clone());
        }

        assert_eq!(sealed.enumerate(), pushed.enumerate());
        for (a, b) in sealed.child_refs().iter().zip(pushed.child_refs().iter()) {
            assert_eq!(a.min_fragment_id, b.min_fragment_id);
            assert_eq!(a.max_fragment_id, b.max_fragment_id);
            assert_eq!(a.row_offset_start, b.row_offset_start);
            assert_eq!(a.total_rows, b.total_rows);
            assert_eq!(a.fragment_count, b.fragment_count);
        }
        assert_eq!(sealed.children().len(), 2);
        assert_eq!(sealed.buffer().len(), 50);
    }

    #[test]
    fn locate_row_reports_out_of_range() {
        let mut layout = TieredLayout::new(2, 1);
        for id in 0..5 {
            layout.push(fragment(id, 10));
        }
        assert!(layout.resolve_fragment(49).is_some());
        assert_eq!(layout.resolve_fragment(50), None);
        assert_eq!(locate_row(&layout.child_refs(), 10, 1_000), None);
    }

    #[test]
    fn child_paths_are_readable_and_collision_proof() {
        let first = child_path(7, 100, 199);
        assert!(first.starts_with("_manifest_children/v7-100-199-"));
        assert!(first.ends_with(".manifest"));
        assert_ne!(first, child_path(7, 100, 199));
    }
}
