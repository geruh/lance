# Nested vs flat leaf layout

Local in-memory comparison on `fragment-metadata-prototype`. Nested files are
not a voted leaf format. This run sizes a homogeneous-add workload to the default
1 MiB leaf budget and projects the same fields in both layouts: fragment ID and
file field IDs.

## Command

```text
cargo test -p lance-table --profile release-with-debug --lib \
  leaf_layouts_at_default_leaf_budget -- --ignored --nocapture
```

Harness: `leaf_layout_compare.rs` in this commit. Parent of the run:
`dcb92b427`. Raw `LEAF_JSON` lines: `leaf_layout_compare.jsonl`.

## Machine

- Darwin 25.6.0 arm64
- rustc 1.97.0 (2d8144b78 2026-07-07)
- profile: `release-with-debug`
- storage: `memory://`

## Workload

2089 fragments, 100 columns, 5 add-columns files each (12534 files,
6 unique field-id lists). `after_adds` is the comparison below.
`timestamp_bump` is the same rows with inline last-updated metadata.

Projection columns:

- flat: `frag_id`, `field_ids`
- nested: `id`, `files.item.field_ids`

The earlier comparison selected the whole nested `files` struct. This one
does not.

## Results (`after_adds`, list mapping, physical dictionary on)

| layout | encoded | SmallReader GET | range GET | range IOPS | projected Arrow | project ns | range ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| flat | 1,050,172 | 1,050,172 | 291,334 | 16 | 1,880,408 | 2,465,292 | 959,584 |
| nested | 986,045 | 986,045 | 263,364 | 6 | 1,949,236 | 741,250 | 641,583 |

Times are one-shot in-memory and include a cold first arm; do not treat the
nanosecond columns as a format-switch argument.

What this does settle:

- Production leaves use `SmallReader`. Bytes read equal the encoded leaf.
  Nested is ~6% smaller on this synthetic file, so its GET is ~6% smaller.
- If the generic file opener issued range GETs instead, nested still reads
  the whole `id` and `files.item.field_ids` pages: 263 KiB vs 291 KiB, 6 vs
  16 IOPS. That is not a large IO win once the dummy FRAGMENT rows are no
  longer in the projected nested columns.
- Decoded Arrow memory for the matched projection is slightly *higher* for
  nested (list-of-struct offsets around `field_ids`) than for flat
  `frag_id` + `field_ids`.
- Dictionary-of-lists is still rejected as a Lance schema.
