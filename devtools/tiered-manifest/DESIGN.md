# Tiered manifest layout

Proto field details in [FORMAT_DECISIONS.md](./FORMAT_DECISIONS.md).

---

## The problem

Every version is one protobuf manifest with every fragment inline. Each append writes a new file with the full list again. Cost tracks fragment count, not append size.

Jack measured about 82 bytes per fragment on S3. At 1M fragments that is roughly 82 MiB rewritten every commit. If compaction does not keep up, fragment count can run away and the manifest gets huge.

---

## What we built

Two levels only. A bounded buffer in the root plus sealed child files on disk. Same logical fragment list after you materialize on open. Bε-tree shaped, not a full Bε-tree.

Opt in. Flat stays the default.

Table config

- `lance.manifest.layout` = `tiered` or `flat`
- `lance.manifest.buffer_cap` = fragment count before seal (default `100000`, about 8 MiB root at Jack's slope)

Reader flag bit 64. Old readers refuse tiered tables. Same pattern as other format flags.

```text
FLAT today                         TIERED (opt in)

every commit                         every commit
    │                                    │
    ▼                                    ▼
┌─────────────────┐                  ┌─────────────────┐
│ ROOT MANIFEST   │                  │ ROOT ~8 MiB cap │
│ all N fragments │                  │ refs + buffer   │
│ grows with N    │                  └────────┬────────┘
└─────────────────┘                           │
                                         sealed children
                                         (never rewritten)
```

---

## When to bother

Below ~100K fragments, tiered should match flat on the wire. Nothing seals until you pass the buffer cap.

Tiered helps when fragment count is already high and you mostly append. It caps **root rewrite size**. It does not stop you from minting fragments. Compaction still matters.

Jack's 10K metadata benchmark is not the target. Flat Lance already wins there. We care about the high fragment-count case.

---

## On disk

```text
{dataset}/
  _versions/{manifest}.manifest     root (schema, refs, buffer)
  _manifest_children/
    v{version}-{min}-{max}-{uuid}.manifest    sealed once
  data/*.lance
```

Root when tiered

- `child_manifests[]` oldest first. Each ref has path, id range, row offsets, byte_size.
- `buffer_fragments[]` newest fragments still in the root.
- `fragments[]` empty once children exist. Before first seal the file matches flat.

Each child is a `FragmentManifest` with the same tail envelope as the root manifest.

Open loads children, concatenates with the buffer, recomputes offsets. Same list flat would have given you.

---

## How ops behave in v1

**Append**

Build the full in-memory list like today. If tiered config is on, seal overflow into children, then write a small root. Pure append keeps existing children. Overflow adds one new child PUT plus the root PUT.

**Open warm**

Children are immutable and cached. After a small append you mostly re-read the root.

**Open cold and enumerate**

Same total metadata bytes as flat. More GETs because children are separate files.

**Delete, update, compact**

Not a pure append, so v1 re-seals from scratch. Correct but expensive at high child count. Delete vectors are the v2 fix.

**Flip flat to tiered**

First commit after opt-in may seal everything in one shot. After that appends seal incrementally.

**Go back to flat**

Set layout flat and inline everything in a maintenance commit.

---

## Wins and tradeoffs

| | flat | tiered |
|--|------|--------|
| steady append when N >> buffer cap | rewrite full manifest | rewrite root only |
| warm reopen while appending | re-read full manifest | re-read root |
| cold open | one manifest | root + each child |
| total metadata bytes | baseline | same, split across files |
| fragment explosion | bad | still bad |
| Jack 10K bench | baseline | tie |

Smaller buffer cap means smaller root PUT and more child files. Default 100K is the compromise.

---

## v1 limits

- Materialize all children on every open (RAM still O(N))
- `build_manifest` still clones the full fragment list on commit
- No `_manifest_children` cleanup in `cleanup_old_versions` yet (orphans leak safely)
- No lazy child load, no delete vectors, no background child merge
- Only two levels

---

## Code map

| Piece | Where |
|-------|-------|
| Proto | `protos/table.proto` |
| Ref types and seal helpers | `rust/lance-table/src/format/fragment_catalog.rs` |
| Read and write child files | `rust/lance-table/src/io/manifest.rs` |
| Commit sealing | `rust/lance/src/dataset/tiered.rs` |
| Open materialize + cache | `builder.rs`, `tiered.rs`, `session/caches.rs` |
| Flag 64 | `rust/lance-table/src/feature_flags.rs` |
| Bench | `rust/lance/examples/tiered_manifest_bench.rs` |

---

## Later

Manifest delete vectors. Lazy open. `optimize_manifest` to merge adjacent children. Byte-based buffer cap. Deeper trees or partition by fragment id if we outgrow two levels.

---

## References

- [Jack metadata benchmark](https://www.lancedb.com/blog/a-metadata-benchmark-of-lance-delta-lake-and-iceberg-on-s3)
- `docs/src/format/table/layout.md`
