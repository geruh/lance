# Format decisions (tiered manifest)

Companion to [DESIGN.md](./DESIGN.md).

This is the on-disk format we landed on and why. Working example throughout is **150K fragments, buffer cap 100K** (one sealed child of 100K, 50K left in the root buffer).

Fragment entry size depends on metadata richness. Jack measured about **82 bytes** per fragment on S3. Our bench harness uses minimal fabricated entries at about **65 bytes** each. Both sets of 150K numbers are below.

---

## Root manifest shape

We kept the full `pb::Manifest` at the root.

When tiered and something has sealed

- `fragments[]` is empty
- `child_manifests[]` holds refs, oldest first
- `buffer_fragments[]` holds the newest fragments still in the root

When nothing has sealed yet, the buffer goes in `fragments[]` like today. **Wire-identical to flat until the first seal.** Jack's 10K case cannot regress on bytes.

**Why not a slim new root message?** Empty protobuf fields cost zero bytes, so a smaller message type does not shrink the root in practice. You still need schema, config, transaction, index pointer. But a new top-level type breaks every tool that assumes manifest tails decode as `pb::Manifest`.

**Why not put the buffer in `fragments[]` when tiered?** Then `fragments[]` means different things depending on context. A parser that ignores flags might see a partial list and think it is complete. Empty `fragments[]` is obviously wrong instead of subtly wrong.

---

## Child manifest shape

Each sealed file is `pb::FragmentManifest { repeated DataFragment fragments }`.

Same tail envelope as the root (`len`, proto, magics). Same `DataFragment` bytes as flat would write.

We rejected stuffing a full `Manifest` into each child (wrong invariants, fake reuse) and rejected raw concatenated fragments (custom parser, tiny byte savings).

---

## What each child ref carries

| Field | Purpose |
|-------|-----------|
| `path` | Dataset-root relative path to the child file |
| `min_fragment_id`, `max_fragment_id` | u64, same width as `DataFragment.id` |
| `row_offset_start` | Prefix sum so you can route a row without opening every child |
| `total_rows` | Row count in that child |
| `fragment_count` | Sanity check on load |
| `byte_size` | Sized GET without HEAD on cold open |

No content hash in v1. Seal version is already in the path. Hash can be a new field later if we want it.

---

## Child file names

Pattern

```text
_manifest_children/v{version}-{min_id}-{max_id}-{uuid}.manifest
```

Readers use `ref.path`, never parse the filename.

The UUID is there because two writers can race on the same version and fragment id range. Without it, one PUT overwrites the other and the root can point at the wrong bytes. Same reason data files use UUIDs.

Once written, a child is never overwritten. Re-seal on delete or compact writes new UUIDs. Old files become unreferenced.

v1 gap. `cleanup_old_versions` does not scan `_manifest_children/` yet, so orphans leak until cleanup learns that directory. Safe direction, still a follow-up before GA.

Shallow clone inlines fragments flat under the new root. Cross-root child refs are v2.

---

## How you know a table is tiered

Two things work together.

**Flag 64** on the manifest. Old readers refuse the dataset. Fail closed, same as other format flags.

**Non-empty `child_manifests[]`** is what new code dispatches on.

Flag 64 is set only when children actually exist, not when you flip config with an empty buffer. Tiered config with nothing sealed yet stays readable by old readers.

---

## When we seal

Buffer cap is in **fragments**, config key `lance.manifest.buffer_cap`, default `100000`.

At Jack's ~82 B per fragment that is about 8 MiB of root. Tables with fat fragment metadata should lower the cap.

Byte-based cap is possible later as writer policy. It does not change the wire format.

Smaller cap means smaller root PUTs and more child files. More children means more GETs on cold enumerate. 100K is the default compromise (~10 children at 1M fragments).

---

## Bytes at 150K fragments (cap 100K)

**Measured** from `tiered_manifest_bench` (fabricated metadata, real commit path). Use these when comparing to the discussion post and charts.

```text
flat root at 150K                     9.28 MiB every commit
tiered root (1 ref + 50K buffer)      3.15 MiB every commit
_manifest_children/...                6.18 MiB, written once
```

**Jack's ~82 B per fragment** back-of-envelope for the same layout on richer S3 metadata.

```text
flat root at 150K                     ~12.3 MiB every commit
sealed child (100K fragments)         ~8.2 MiB, written once
tiered root (50K buffer + one ref)    ~4.2 MiB every commit
```

Same shape, different bytes per entry. Tables with fat fragment metadata should expect sizes closer to the second block. The root PUT win at 150K is still roughly 3x in either case.

---

## Migration

**Flat to tiered.** Config flip alone changes nothing. Next commit that pushes past the cap seals full runs into children and rewrites a small root. Old readers lose access on that commit. Earlier versions stay as they were.

**Tiered to flat.** Set layout flat. Next commit inlines everything into one root, no flag 64. Orphan children wait for cleanup.

---

## Branch checklist (already in the commit)

- Fragment id fields on refs are u64
- `byte_size` on refs for sized GETs
- UUID in child paths
- Root shape, child shape, envelope, flag-when-children, fragment-count cap as above
