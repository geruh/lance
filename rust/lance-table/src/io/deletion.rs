// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashSet, sync::Arc, sync::LazyLock};

use arrow_array::{RecordBatch, UInt32Array};
use arrow_ipc::CompressionType;
use arrow_ipc::reader::FileReader as ArrowFileReader;
use arrow_ipc::writer::{FileWriter as ArrowFileWriter, IpcWriteOptions};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use bytes::Buf;
use lance_core::error::{CorruptFileSnafu, box_error};
use lance_core::utils::deletion::DeletionVector;
use lance_core::utils::tracing::{AUDIT_MODE_CREATE, AUDIT_TYPE_DELETION, TRACE_FILE_AUDIT};
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use rand::Rng;
use roaring::bitmap::RoaringBitmap;
use snafu::ResultExt;
use tracing::{info, instrument};

use crate::feature_flags::ensure_deletion_file_paths_enabled;
use crate::format::{DeletionFile, DeletionFilePath, DeletionFileType};

pub const DELETIONS_DIR: &str = "_deletions";

/// The Arrow schema for an Arrow deletion file.
static DELETION_ARROW_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "row_id",
        DataType::UInt32,
        false,
    )]))
});

/// Get the path of a deletion file under `base`, the root of the dataset that
/// holds it (the dataset named by [`DeletionFile::base_id`]).
///
/// An explicit [`DeletionFile::path`] is the location itself, and
/// `fragment_id` plays no part in it. Otherwise this is the legacy location
/// `_deletions/{fragment_id}-{read_version}-{id}.{extension}`, derived from the
/// id of the fragment that owns the file.
pub fn deletion_file_path(base: &Path, fragment_id: u64, deletion_file: &DeletionFile) -> Path {
    base.clone()
        .join(DELETIONS_DIR)
        .join(deletion_file_name(fragment_id, deletion_file))
}

/// [`deletion_file_path`] relative to the root of the dataset that holds the
/// file.
pub fn relative_deletion_file_path(fragment_id: u64, deletion_file: &DeletionFile) -> String {
    format!(
        "{DELETIONS_DIR}/{}",
        deletion_file_name(fragment_id, deletion_file)
    )
}

/// The name of a deletion file inside `_deletions/`. This is the only place
/// that tells an explicit path from a location derived from the fragment id.
fn deletion_file_name(fragment_id: u64, deletion_file: &DeletionFile) -> String {
    // Destructured on purpose: a new field fails to compile here until someone
    // decides whether it changes where the file is.
    let DeletionFile {
        read_version,
        id,
        file_type,
        num_deleted_rows: _,
        // Selects the root the caller passes in, not the path under it.
        base_id: _,
        path,
    } = deletion_file;
    match path {
        Some(path) => path.file_name(file_type),
        None => format!("{fragment_id}-{read_version}-{id}.{}", file_type.suffix()),
    }
}

/// How [`write_deletion_file_with_naming`] names a new deletion file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionFileNaming {
    /// Derive the name from the id of the fragment that owns the file:
    /// `_deletions/{fragment_id}-{read_version}-{id}.{extension}`. Every Lance
    /// version reads it, but it is only found while the fragment keeps this id,
    /// so use it for fragments whose id is final.
    FragmentId(u64),
    /// Name the file `_deletions/{uuid}.{extension}` and record that path in
    /// [`DeletionFile::path`]. The fragment can then be given an id later -- by
    /// a commit, or by a retry of that commit -- without the file being
    /// rewritten or renamed. A manifest that references the file needs
    /// [`FLAG_UNSTABLE_DELETION_FILE_PATHS`](crate::feature_flags::FLAG_UNSTABLE_DELETION_FILE_PATHS),
    /// which commits set from the fragments they write, so this is only
    /// available where
    /// [`deletion_file_paths_enabled`](crate::feature_flags::deletion_file_paths_enabled)
    /// holds.
    Uuid,
}

/// Write a deletion file for a fragment for a given deletion vector, named
/// after `fragment_id` (see [`DeletionFileNaming::FragmentId`]).
///
/// Returns the deletion file if one was written. If no deletions were present,
/// returns `Ok(None)`.
pub async fn write_deletion_file(
    base: &Path,
    fragment_id: u64,
    read_version: u64,
    removed_rows: &DeletionVector,
    object_store: &ObjectStore,
) -> Result<Option<DeletionFile>> {
    write_deletion_file_with_naming(
        base,
        DeletionFileNaming::FragmentId(fragment_id),
        read_version,
        removed_rows,
        object_store,
    )
    .await
}

/// Write a deletion file for a given deletion vector, named as `naming` says.
///
/// Returns the deletion file if one was written. If no deletions were present,
/// returns `Ok(None)`.
pub async fn write_deletion_file_with_naming(
    base: &Path,
    naming: DeletionFileNaming,
    read_version: u64,
    removed_rows: &DeletionVector,
    object_store: &ObjectStore,
) -> Result<Option<DeletionFile>> {
    // Refused rather than written under a name derived from a fragment id the
    // caller does not have: that name would not survive the id assignment.
    if naming == DeletionFileNaming::Uuid {
        ensure_deletion_file_paths_enabled()?;
    }
    let (file_type, num_deleted_rows, out) = match removed_rows {
        DeletionVector::NoDeletions => return Ok(None),
        DeletionVector::Set(set) => {
            let array = UInt32Array::from_iter(set.iter().copied());
            let array = Arc::new(array);

            let schema = DELETION_ARROW_SCHEMA.clone();
            let batch = RecordBatch::try_new(schema.clone(), vec![array])?;

            let mut out: Vec<u8> = Vec::new();
            let write_options =
                IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;
            {
                let mut writer = ArrowFileWriter::try_new_with_options(
                    &mut out,
                    schema.as_ref(),
                    write_options,
                )?;
                writer.write(&batch)?;
                writer.finish()?;
                // Drop writer so out is no longer borrowed.
            }
            (DeletionFileType::Array, set.len(), out)
        }
        DeletionVector::Bitmap(bitmap) => {
            let mut out: Vec<u8> = Vec::new();
            bitmap.serialize_into(&mut out)?;
            (DeletionFileType::Bitmap, bitmap.len() as usize, out)
        }
    };
    let (fragment_id, path) = match naming {
        DeletionFileNaming::FragmentId(fragment_id) => (fragment_id, None),
        // An explicit path does not depend on the fragment id at all.
        DeletionFileNaming::Uuid => (0, Some(DeletionFilePath::new_random())),
    };
    let deletion_file = DeletionFile {
        read_version,
        id: rand::rng().random::<u64>(),
        file_type,
        num_deleted_rows: Some(num_deleted_rows),
        base_id: None,
        path,
    };
    let path = deletion_file_path(base, fragment_id, &deletion_file);

    object_store.put(&path, &out).await?;

    info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_CREATE, r#type=AUDIT_TYPE_DELETION, path = path.to_string());

    Ok(Some(deletion_file))
}

#[instrument(
    level = "debug",
    skip(base, object_store),
    fields(
        base = base.as_ref(),
        bytes_read = tracing::field::Empty
    )
)]
pub async fn read_deletion_file(
    fragment_id: u64,
    deletion_file: &DeletionFile,
    base: &Path,
    object_store: &ObjectStore,
) -> Result<DeletionVector> {
    let span = tracing::Span::current();
    match deletion_file.file_type {
        DeletionFileType::Array => {
            let path = deletion_file_path(base, fragment_id, deletion_file);

            let data = object_store.read_one_all(&path).await?;
            span.record("bytes_read", data.len());
            let data = std::io::Cursor::new(data);
            let mut batches: Vec<RecordBatch> = ArrowFileReader::try_new(data, None)?
                .collect::<std::result::Result<_, ArrowError>>()
                .map_err(box_error)
                .context(CorruptFileSnafu { path: path.clone() })?;

            if batches.len() != 1 {
                return Err(Error::corrupt_file(
                    path,
                    format!(
                        "Expected exactly one batch in deletion file, got {}",
                        batches.len()
                    ),
                ));
            }

            let batch = batches.pop().unwrap();
            if batch.schema().as_ref() != DELETION_ARROW_SCHEMA.as_ref() {
                return Err(Error::corrupt_file(
                    path,
                    format!(
                        "Expected schema {:?} in deletion file, got {:?}",
                        DELETION_ARROW_SCHEMA.as_ref(),
                        batch.schema()
                    ),
                ));
            }

            let array = batch.columns()[0]
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();

            let mut set = HashSet::with_capacity(array.len());
            for val in array.iter() {
                if let Some(val) = val {
                    set.insert(val);
                } else {
                    return Err(Error::corrupt_file(
                        path,
                        "Null values are not allowed in deletion files",
                    ));
                }
            }

            Ok(DeletionVector::Set(set))
        }
        DeletionFileType::Bitmap => {
            let path = deletion_file_path(base, fragment_id, deletion_file);

            let data = object_store.read_one_all(&path).await?;
            span.record("bytes_read", data.len());
            let reader = data.reader();
            let bitmap = RoaringBitmap::deserialize_from(reader)
                .map_err(box_error)
                .context(CorruptFileSnafu { path })?;

            Ok(DeletionVector::Bitmap(bitmap))
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn test_write_no_deletions() {
        let dv = DeletionVector::NoDeletions;

        let (object_store, path) = ObjectStore::from_uri("memory:///no_deletion")
            .await
            .unwrap();
        for naming in [DeletionFileNaming::FragmentId(0), DeletionFileNaming::Uuid] {
            let file = write_deletion_file_with_naming(&path, naming, 0, &dv, &object_store)
                .await
                .unwrap();
            assert!(file.is_none());
        }
    }

    #[rstest::rstest]
    #[case::array(DeletionVector::Set(HashSet::from_iter([5, 9])), "arrow")]
    #[case::bitmap(DeletionVector::Bitmap(RoaringBitmap::from_iter([5, 9])), "bin")]
    #[tokio::test]
    async fn test_uuid_naming_is_independent_of_fragment_id(
        #[case] dv: DeletionVector,
        #[case] suffix: &str,
    ) {
        let object_store = ObjectStore::memory();
        let base = Path::from("/uuid");
        let file = write_deletion_file_with_naming(
            &base,
            DeletionFileNaming::Uuid,
            12,
            &dv,
            &object_store,
        )
        .await
        .unwrap()
        .unwrap();

        let explicit = file
            .path
            .as_ref()
            .expect("uuid naming records the path")
            .relative_path(&file.file_type);
        let (name, extension) = explicit
            .strip_prefix("_deletions/")
            .unwrap()
            .rsplit_once('.')
            .unwrap();
        assert!(uuid::Uuid::parse_str(name).is_ok(), "{explicit}");
        assert_eq!(extension, suffix);
        assert_eq!(file.read_version, 12);
        assert_eq!(file.num_deleted_rows, Some(2));

        // The one stored object is the explicit path, whatever the fragment id.
        let mut stored = object_store
            .read_dir_all(&base, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        let stored = stored.pop().unwrap().location;
        assert_eq!(stored.as_ref(), format!("uuid/{explicit}"));
        for fragment_id in [0, 42, 43] {
            assert_eq!(deletion_file_path(&base, fragment_id, &file), stored);
            assert_eq!(relative_deletion_file_path(fragment_id, &file), explicit);
            let read_dv = read_deletion_file(fragment_id, &file, &base, &object_store)
                .await
                .unwrap();
            assert_eq!(read_dv, dv);
        }
    }

    #[test]
    fn test_deletion_file_path_derives_legacy_location_only_without_path() {
        let legacy = DeletionFile {
            read_version: 3,
            id: 77,
            file_type: DeletionFileType::Bitmap,
            num_deleted_rows: Some(1),
            base_id: Some(2),
            path: None,
        };
        let base = Path::from("root");
        assert_eq!(
            deletion_file_path(&base, 9, &legacy),
            Path::from("root/_deletions/9-3-77.bin")
        );
        assert_eq!(
            relative_deletion_file_path(9, &legacy),
            "_deletions/9-3-77.bin"
        );

        let uuid = uuid::Uuid::from_u128(0xabc);
        let explicit = DeletionFile {
            path: Some(uuid.into()),
            ..legacy
        };
        let expected = format!("_deletions/{}.bin", uuid.hyphenated());
        assert_eq!(
            deletion_file_path(&base, 9, &explicit),
            Path::from(format!("root/{expected}"))
        );
        assert_eq!(relative_deletion_file_path(9, &explicit), expected);
    }

    #[tokio::test]
    async fn test_write_array() {
        let dv = DeletionVector::Set(HashSet::from_iter(0..100));

        let fragment_id = 21;
        let read_version = 12;

        let object_store = ObjectStore::memory();
        let path = Path::from("/write");
        let file = write_deletion_file(&path, fragment_id, read_version, &dv, &object_store)
            .await
            .unwrap();

        assert!(matches!(
            file,
            Some(DeletionFile {
                file_type: DeletionFileType::Array,
                ..
            })
        ));

        let file = file.unwrap();
        assert_eq!(file.read_version, read_version);
        let path = deletion_file_path(&path, fragment_id, &file);
        assert_eq!(
            path,
            Path::from(format!("/write/_deletions/21-12-{}.arrow", file.id))
        );

        let data = object_store
            .inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let data = std::io::Cursor::new(data);
        let mut batches: Vec<RecordBatch> = ArrowFileReader::try_new(data, None)
            .unwrap()
            .collect::<std::result::Result<_, ArrowError>>()
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = batches.pop().unwrap();
        assert_eq!(batch.schema(), *DELETION_ARROW_SCHEMA);
        let array = batch["row_id"]
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let read_dv = DeletionVector::from_iter(array.iter().map(|v| v.unwrap()));
        assert_eq!(read_dv, dv);
    }

    #[tokio::test]
    async fn test_write_bitmap() {
        let dv = DeletionVector::Bitmap(RoaringBitmap::from_iter(0..100));

        let fragment_id = 21;
        let read_version = 12;

        let object_store = ObjectStore::memory();
        let path = Path::from("/bitmap");
        let file = write_deletion_file(&path, fragment_id, read_version, &dv, &object_store)
            .await
            .unwrap();

        assert!(matches!(
            file,
            Some(DeletionFile {
                file_type: DeletionFileType::Bitmap,
                ..
            })
        ));

        let file = file.unwrap();
        assert_eq!(file.read_version, read_version);
        let path = deletion_file_path(&path, fragment_id, &file);
        assert_eq!(
            path,
            Path::from(format!("/bitmap/_deletions/21-12-{}.bin", file.id))
        );

        let data = object_store
            .inner
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader = data.reader();
        let read_bitmap = RoaringBitmap::deserialize_from(reader).unwrap();
        assert_eq!(read_bitmap, dv.into_iter().collect::<RoaringBitmap>());
    }

    #[tokio::test]
    async fn test_roundtrip_array() {
        let dv = DeletionVector::Set(HashSet::from_iter(0..100));

        let fragment_id = 21;
        let read_version = 12;

        let object_store = ObjectStore::memory();
        let path = Path::from("/roundtrip");
        let file = write_deletion_file(&path, fragment_id, read_version, &dv, &object_store)
            .await
            .unwrap();

        let read_dv = read_deletion_file(fragment_id, &file.unwrap(), &path, &object_store)
            .await
            .unwrap();
        assert_eq!(read_dv, dv);
    }

    #[tokio::test]
    async fn test_roundtrip_bitmap() {
        let dv = DeletionVector::Bitmap(RoaringBitmap::from_iter(0..100));

        let fragment_id = 21;
        let read_version = 12;

        let object_store = ObjectStore::memory();
        let path = Path::from("/bitmap");
        let file = write_deletion_file(&path, fragment_id, read_version, &dv, &object_store)
            .await
            .unwrap();

        let read_dv = read_deletion_file(fragment_id, &file.unwrap(), &path, &object_store)
            .await
            .unwrap();
        assert_eq!(read_dv, dv);
    }
}
