# Fragment Metadata Tree

!!! warning "Experimental"

    This layout is unstable. Readers and writers may change it without
    keeping compatibility with earlier unstable revisions. Flat manifests
    remain the default.

**This page specifies a proposed on disk format for storing fragment state. Support for creating or reading tree
tables is not yet available in released Lance versions.**

!!! note "Tree tables require feature flag 512, `FLAG_FRAGMENT_METADATA`"

    A reader or writer that does not understand this layout must refuse the
    dataset. The flat `fragments` list is empty on a tree table, so a reader
    that ignored the flag would see an empty table.

In the flat format, each Version Manifest carries the complete fragment list.
Committing a change and opening a dataset both process that whole list, even
when the change touches only a few fragments.

The fragment metadata tree moves those records into immutable Lance leaves
keyed by fragment ID. Small validated changes can stay above the leaves as
pending operations until enough work accumulates to rewrite them efficiently.

The Version Manifest still defines the state of a table version, and committing it makes that version visible.

![Fragment metadata tree](fragment_metadata_tree.svg)

*A shallow tree is shown. Larger trees may insert interior routing nodes between
the root and leaves.*

### At a glance

| |                                                                                       |
|---|---------------------------------------------------------------------------------------|
| Read | Follow the fragment ID range to a leaf, then apply newer mutations for that fragment. |
| Write | Native Lance validates the change. The tree stores the resulting mutation.            |
| Publish | Write immutable objects first, then publish the ordinary Version Manifest.            |

## Format contract

Protobuf messages are in `protos/table.proto`. Operation semantics, validation,
and conflict rules stay with [transactions](transaction.md). A storage action
is the result of a successful native transaction, never a second implementation
of its rules.

Violating any required invariant on this page makes the snapshot corrupt.

## Snapshot layout

A tree table sets `Manifest.fragment_metadata`, leaves `Manifest.fragments`
empty, and sets flag 512 in both reader_feature_flags and writer_feature_flags.
These requirements must agree, otherwise the manifest is invalid.

A tree descriptor contains exactly one of an inline root or an external
`root_path`.

An inline root carries the complete `FragmentMetadataRoot` in the Version
Manifest. `mutations_since_root` is empty.

An external root is named by `root_path`. Each Version Manifest contains the
complete set of mutations needed after that root. Readers never follow a
mutation chain.

```
Version Manifest N
├─ root_path ──────────► Root R
└─ mutations_since_root

snapshot N = Root R + mutations_since_root
```

Opening a version reads its Version Manifest and at most one external root.
It does not walk version history. Fragment lookup then follows the tree
from root to leaf.

`buffer` on a root or interior node holds the pending mutations already stored in the tree 
but not yet pushed to that node’s children. A later commit may push them downward or materialize
them into leaves. It may also publish a new external root and reset `mutations_since_root`.

## Objects

All tree objects are immutable and stored under `_bt/` in the dataset root. Paths are 
relative to the dataset root and must begin with _bt/. Absolute paths and paths 
outside `_bt/` are invalid.

```
{dataset_root}/
    _bt/
        base/{uuid}.root     FragmentMetadataRoot protobuf
        node/{uuid}.node     FragmentMetadataNode protobuf
        leaf/{uuid}.lance    Lance file of complete fragment records
```

`.root` files contain a `FragmentMetadataRoot` protobuf. `.node` file contain a `FragmentMetadataNode`
protobuf. Both are stored as the raw protobuf message, with no extra header or footer.

`FragmentMetadataRoot` contains its child references and mutation `buffer`, along 
with `next_action_sequence`, `hard_capacity_bytes`, and `version`. version must be between 1 and
the Version Manifest version that references the root.

`FragmentMetadataNode` holds the child list and a mutation buffer for that
subtree. Child paths are unique and non-empty.

A leaf is a Lance file that stores complete fragment records for a single fragment ID range.

## Leaf representation

A leaf is a Lance file, format version 2.1, with this schema.

```python
import pyarrow as pa

leaf_schema = pa.schema([
    pa.field("row_kind", pa.uint8(), nullable=False),
    pa.field("frag_id", pa.uint64(), nullable=False),
    pa.field("fragment_meta", pa.binary(), nullable=True),
    pa.field("path", pa.utf8(), nullable=False),
    pa.field("field_ids", pa.list_(pa.field("item", pa.int32(), nullable=False)), nullable=False),
    pa.field("column_indices", pa.list_(pa.field("item", pa.int32(), nullable=False)), nullable=False),
    pa.field("major_version", pa.uint32(), nullable=False),
    pa.field("minor_version", pa.uint32(), nullable=False),
    pa.field("file_size_bytes", pa.uint64(), nullable=False),
    pa.field("base_id", pa.uint32(), nullable=True),
])
```

The leaf schema is exact. Different fields, types, or nullability are invalid.
Non-nullable columns and list elements do not hold nulls.

`row_kind` is 0 for a FRAGMENT row and 1 for a DATA_FILE row. Unknown values
are invalid.

Rows are grouped by fragment. A group is one FRAGMENT row followed by its
DATA_FILE rows, in file order. Every row in the group carries that
`frag_id`. The FRAGMENT row's `frag_id` and the `id` inside `fragment_meta`
are equal. A fragment with no files is a group of one FRAGMENT row. Groups
are ordered by strictly increasing `frag_id`. A fragment is never split
across leaves.

A FRAGMENT row carries `fragment_meta`, the `DataFragment` protobuf with
`files` cleared. Its remaining columns are sentinels: empty `path`, empty
`field_ids` and `column_indices`, version and size 0, and a null `base_id`.
A DATA_FILE row carries one `DataFile` across those columns and a null
`fragment_meta`. `file_size_bytes` of 0 means unknown. A null `base_id` means
the file lives under the dataset root. Otherwise it indexes
`Manifest.base_paths`.

The encoded size equals the parent's `object_size`. After decode, the
recomputed child summary equals the parent's `FragmentMetadataChild`.

Counts in a leaf are always known. `physical_rows` of 0 is zero rows, not
unknown. A deletion file carries `num_deleted_rows`, which may not exceed
`physical_rows`.

## Routing

Children of a root or interior are sorted by `min_key`. `min_key` is the
inclusive start of the range that child owns. The next sibling's `min_key` is
the exclusive end. The root's first child has `min_key` 0. An interior's
first child has the `min_key` its parent assigned to that node. Remaining
children partition that range by increasing `min_key`. Every fragment id
routes to exactly one leaf. Routing a key picks the rightmost child whose
`min_key` is at or below it.

`max_key` is the largest fragment id stored in the subtree. It is a summary
and never routes or prunes, because a buffered insert may sit past it.

Ranges never overlap. Each buffered mutation belongs to exactly one child
range. Every child in one list has the same `height`. A leaf has `height` 0,
`num_children` 0, and `num_keys` above 0. An interior has at least two children
and a `height` one above its children. An empty child is removed from its
parent rather than kept.

`num_keys`, `total_rows`, and `visible_rows` on a child include every mutation
buffered inside that subtree. A leaf summary is recomputed from the leaf's
records on read. An interior or root summary is the sum of its children plus
its own buffered deltas. `visible_rows` never exceeds `total_rows`.

`object_size` is the stored byte length of the child object and must be
nonzero. Split and merge targets are writer policy and are not stored on the
child.

A child's inherited range is `min_key` inclusive to the next sibling's
`min_key` exclusive. The last child of the root uses `2^32` as that
exclusive end. The last child of an interior uses the exclusive end the parent
assigned to that node. `max_key` is a stored-id summary and is not the
exclusive end. A buffered insert may sit past `max_key` and must still lie
in the inherited range. `min_key` is at most `max_key`, and at most
`2^32 - 1`.

Every fragment id decoded from a leaf lies in that leaf's inherited range.
Every mutation target decoded from a root or interior buffer lies in that
node's inherited range. Every descendant child range is contained in its
parent's range. These checks apply to each object as it is decoded. They do
not require opening the rest of the tree. They are separate from the rule
that no id may exceed `Manifest.max_fragment_id`.

## Mutations and replay

`FragmentMetadataMutation` wraps one `FragmentAction` with a sequence number
and three count deltas. The deltas are exact. `fragment_count_delta` is 1
when the action creates a record, minus 1 when it removes one, and 0
otherwise. `total_rows_delta` and `visible_rows_delta` are the record after
the action minus the record before it, with an absent record counting as
zero.

Resolving a fragment has the record before the action and the action itself.
Stored deltas that disagree with that recomputation are invalid. Summaries
above a leaf are derived from these deltas. Materialization recomputes leaf
summaries from records. A published materialization whose recomputed
summaries disagree with the snapshot totals is invalid.

| Action | Precondition | Effect |
|---|---|---|
| `add_fragment` | None | Install the complete record, replacing anything at that id |
| `remove_fragment` | None | Remove the record at that id. An absent id is a no-op |
| `add_data_file` | Record present, else invalid | Append the file to the end of the ordered file list |
| `remove_data_file` | Record present, else invalid | Remove every file whose path matches, keeping survivor order. No match is a no-op |
| `add_deletion_file` | Record present, else invalid | Set the deletion file, replacing any existing one |
| `clear_deletion_file` | Record present, else invalid | Clear the deletion file. No deletion file is a no-op |
| `replace_data_file` | Record present and a file whose path equals `expected_path`, else invalid | On the first such file set `path`, `file_size_bytes`, and `base_id` from `file`. Keep its field ids, column indices, and file version |

`Fragment.files` is an ordered list and paths may repeat. `replace_data_file`
edits a slot, not a path. Two replacements that chain through a renamed path
must not be folded into one. Starting from files `[A, B]`, renaming B to A
and then replacing the first A with C yields `[C, A]`. Folding them into one
replacement of B with C yields `[A, C]` and is wrong.

To resolve a fragment, collect every mutation for its id from
`mutations_since_root`, the root buffer, and each interior buffer on its routing
path. Sort by sequence and apply in order to the leaf record, or to nothing if
the leaf has no record.

`total_fragments`, `total_rows`, and `visible_rows` on the descriptor equal the
root's derived totals plus the `mutations_since_root` deltas.

## Sequences and materialization watermark

Mutation sequences are nonzero, unique, and within their owner's range. Each
tree has one sequence namespace. A writer never reuses a number anywhere in
the tree.

- `FragmentMetadataRoot.next_action_sequence` is at least 1. Every sequence in
  the root buffer, in every interior buffer below it, and every leaf watermark
  below it is less than this value.
- `FragmentMetadataTree.next_action_sequence` is at least the root's value.
  Every `mutations_since_root` sequence is at or above the root's value and
  below the descriptor's value.
- A leaf watermark of 0 means the leaf has applied nothing, so every mutation
  routed to it replays.

Uniqueness is required across every mutation source decoded for the
operation. Uniqueness across unread subtrees is a writer invariant.

A leaf watermark `N` means every mutation for that leaf's range with a
sequence at or below `N` has been incorporated into the leaf's records, with
no holes, and no buffer above the leaf holds a mutation for its range with a
sequence at or below `N`. A collected mutation whose sequence is at or below
the owning leaf's `materialized_through_action_sequence` is invalid.

A writer establishes the watermark by moving whole per-child batches from a
buffer into the child below, never part of a fragment's pending sequence, and
by setting the watermark to the highest sequence incorporated. Splits and
coalesces move messages with their ranges and preserve it.

## Fragment ids

Fragment id allocation is not tree state. `Manifest.max_fragment_id` is the
fragment id allocation high-water mark for flat and tree layouts alike, and
it includes reservations.

- Absent means nothing has been allocated or reserved, and the next id is 0.
- Otherwise the next id is `max_fragment_id` plus one.
- `u32` max means the id space is exhausted, and any further allocation fails
  before publication.

A reservation advances `max_fragment_id` without creating a fragment. Reserved
but unused ids stay consumed. Deleting a fragment never recycles its id. The
value never decreases within a lineage, restore included.

An external root may be reused by later versions whose `max_fragment_id` has
moved on, so a root holds no allocation state. No fragment id stored in the
tree, whether in a leaf, a child summary, a buffer, or
`mutations_since_root`, may exceed the manifest's `max_fragment_id`. If
`max_fragment_id` is absent, the tree contains no fragments and no mutation
that targets a fragment.

## Limits and validation

A reader validates every structure it decodes before using it. The snapshot is
never treated as empty.

`hard_capacity_bytes` in the root is the one size rule. No object under that
root, the root included, may exceed it. A single fragment that cannot fit is
an error before publication. Each external root records the cap its objects
satisfy.

Node, leaf, and buffer byte targets are writer policy. They live in table
configuration, not in the tree. A writer may rebalance an existing tree under
different targets.

| Object | Additional checks |
|---|---|
| Manifest | Feature flags set, `fragments` empty, descriptor present |
| Descriptor | Valid base, totals match derived values plus `mutations_since_root`, sequence range valid, no target above `max_fragment_id` |
| Root | Encoded size within `hard_capacity_bytes`, children and buffer valid, derived visible rows at most derived total rows |
| Node / leaf | Encoded size equals parent `object_size` and is within `hard_capacity_bytes`, recomputed summary equals the parent child |

## Publication and cleanup

Every `_bt/` object a version depends on is written before the Version Manifest
that names it is committed. That manifest commit through the dataset's commit
handler is the visibility boundary. There is no other publication step.

A failed attempt leaves unreachable objects that are eligible for cleanup. A retry
validates again against the winning version and does not reuse actions
validated against a different descriptor. A lost publish response is resolved
by reading the published manifest and comparing descriptors.

Whether to inline the root or publish a new external root is writer policy and
may change between versions.

Cleanup computes reachability from the logical fragment state of every
retained manifest, applying buffers and `mutations_since_root`. The reachable
set is the `root_path` if any, every `.node` and `.lance` reachable from that
root, and every data file, deletion file, and related object that state names.
A file named only by a pending mutation is reachable through that resolved
state. A file named only by a mutation that a later mutation in the same
snapshot supersedes is not. Objects under `_bt/` outside the set follow the
same age and in-progress rules as data files.

An object reachable from any retained manifest is never removed. A clone
must preserve the lifetime of every tree object reachable from its retained
manifests. It may copy immutable objects or share them.
