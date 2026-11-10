# Format Versioning

## Feature Flags

As the table format evolves, new feature flags are added to the format.
There are two separate fields for checking for feature flags,
depending on whether you are trying to read or write the table.
Readers should check the `reader_feature_flags` to see if there are any flag it is not aware of.
Writers should check `writer_feature_flags`. If either sees a flag they don't know,
they should return an "unsupported" error on any read or write operation.

## Current Feature Flags

<style>
.feature-flags-table th:nth-child(2),
.feature-flags-table td:nth-child(2) {
  white-space: nowrap;
  min-width: 250px;
}
</style>

<div class="feature-flags-table" markdown="1">

| Flag Bit | Flag Name                       | Reader Required | Writer Required | Description                                                                                                 |
|----------|---------------------------------|-----------------|-----------------|-------------------------------------------------------------------------------------------------------------|
| 1        | `FLAG_DELETION_FILES`           | Yes             | Yes             | Fragments may contain deletion files, which record the tombstones of soft-deleted rows.                     |
| 2        | `FLAG_STABLE_ROW_IDS`           | Yes             | Yes             | Row IDs are stable for both moves and updates. Fragments contain an index mapping row IDs to row addresses. |
| 4        | `FLAG_USE_V2_FORMAT_DEPRECATED` | No              | No              | Files are written with the new v2 format. This flag is deprecated and no longer used.                       |
| 8        | `FLAG_TABLE_CONFIG`             | No              | Yes             | Table config is present in the manifest.                                                                    |
| 16       | `FLAG_BASE_PATHS`               | Yes             | Yes             | Dataset uses multiple base paths (for shallow clones or multi-base datasets).                               |
| 32       | `FLAG_DISABLE_TRANSACTION_FILE` | No              | Yes             | Transactions are recorded in the manifest rather than in a separate transaction file.                       |
| 64       | `FLAG_UNSTABLE_DATA_OVERLAY_FILES` | Yes          | Yes             | Fragments may carry data overlay files. Unstable: release builds reject it unless explicitly opted in.      |
| 128      | `FLAG_COVERED_INDEX_METADATA`   | Yes             | Yes             | Some index declares covering columns (`IndexMetadata.covering_fields`), so `fields` means keyed columns followed by carried ones. An implementation without this flag selects an index by membership of `fields` and would answer a query on a merely-carried column with an index keyed on a different one. |
| 256      | `FLAG_MIXED_DATA_FILE_VERSIONS` | Yes             | Yes             | The snapshot may reference recognized V2 data files with different exact versions. Both bits must be set and remain set on later versions. |
| 512      | `FLAG_FRAG_REUSE_WITH_STABLE_ROW_IDS` | Yes       | Yes             | The table uses stable row IDs and carries a [Fragment Reuse Index](../index/system/frag_reuse.md). |
| 1024     | `FLAG_FRAGMENT_REUSE_INDEX`     | Yes             | Yes             | The fragment reuse index records tagged transitions (`IndexMetadata.index_version >= 1`). Readers must translate row addresses through them; writers must preserve them. An implementation without this flag would decode the details as the legacy format and silently drop the transitions when it next rewrites the fragment reuse index. See [FRI index versions](../index/system/frag_reuse.md#fri-index-versions). |
| 2048     | `FLAG_UNSTABLE_SPILLED_ROW_LINEAGE` | Yes         | Yes             | Some fragment stores its row ids or row version sequences as hidden columns of a data file rather than inline. A reader without this flag would see the fragment as having no row ids. Unstable: release builds reject it unless explicitly opted in. |
| 4096     | `FLAG_FRAGMENT_TREE`            | Yes             | Yes             | Fragment records live in a [fragment tree](fragment_metadata.md). `Manifest.fragments` is empty. |
| 1048576  | `FLAG_UNSTABLE_DELETION_FILE_PATHS` | Yes         | Yes             | Some fragment's deletion file records its own location in `DeletionFile.path` rather than deriving it from the fragment id (see [Deletion Files](index.md#deletion-files)). A reader without this flag would look for a file that does not exist. Once set, it stays set on every later version, including through restore, because earlier versions that reference such files may still be read and cleaned up. Unstable: release builds reject it unless explicitly opted in. |

</div>

Bit values 8192 through 524288 belong to features that this version of the specification does not include; an implementation of this version rejects them like unknown flags. Flags with bit values 2097152 and above are unknown; unknown flags cause implementations to reject the dataset with an "unsupported" error. The paired mixed-version and deletion file paths reader and writer bits must each either both be set or both be clear; a half-set manifest is invalid.

Cleanup reads every retained version. Implementations of Lance before 14.0 do not check the flags of each version they clean up, only those of the version they opened. Such an implementation that holds a version opened before the first explicit deletion file path can delete files that later versions reference, so every client that cleans up a table must be upgraded before explicit deletion file paths are written to it.
