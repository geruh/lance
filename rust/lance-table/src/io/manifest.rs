// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use async_trait::async_trait;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Bytes, BytesMut};
use lance_arrow::DataTypeExt;
use lance_file::{
    previous::writer::ManifestProvider as PreviousManifestProvider, version::LanceFileVersion,
};
use object_store::ObjectStoreExt;
use object_store::path::Path;
use std::collections::HashMap;
use std::{ops::Range, sync::Arc};
use tracing::instrument;

use lance_core::{Error, Result, datatypes::Schema};
use lance_io::{
    encodings::{Encoder, binary::BinaryEncoder, plain::PlainEncoder},
    object_store::ObjectStore,
    object_writer::{ObjectWriter, WriteResult},
    traits::{WriteExt, Writer},
    utils::read_message,
};

use crate::format::{
    DataFileFieldInterner, DataStorageFormat, Fragment, FragmentManifestRef, IndexMetadata, MAGIC,
    MAJOR_VERSION, MINOR_VERSION, Manifest, Transaction, pb,
};

use super::commit::ManifestLocation;

/// Read Manifest on URI.
#[instrument(level = "debug", skip(object_store))]
pub async fn read_manifest(
    object_store: &ObjectStore,
    path: &Path,
    known_size: Option<u64>,
) -> Result<Manifest> {
    let proto: pb::Manifest = read_tail_proto(object_store, path, known_size).await?;
    let mut manifest = Manifest::try_from(proto)?;
    if manifest.is_tiered() {
        materialize_child_manifests(object_store, path, &mut manifest).await?;
    }
    Ok(manifest)
}

/// Load sealed children and prepend them to the root buffer tail.
pub async fn materialize_child_manifests(
    object_store: &ObjectStore,
    manifest_path: &Path,
    manifest: &mut Manifest,
) -> Result<()> {
    if manifest.child_manifests.is_empty() {
        return Ok(());
    }
    let root = dataset_root_of_manifest(manifest_path);
    let children = manifest.child_manifests.clone();

    let loads = children.iter().map(|child| {
        let child_path = child_full_path(&root, &child.path);
        async move {
            let fragments =
                read_fragment_manifest(object_store, &child_path, child.size_hint()).await?;
            verify_child_fragment_count(&child_path, child, fragments.len())?;
            Ok::<_, Error>(fragments)
        }
    });
    let runs = futures::future::try_join_all(loads).await?;

    let sealed: usize = children.iter().map(|c| c.fragment_count as usize).sum();
    let mut all = Vec::with_capacity(sealed + manifest.fragments.len());
    all.extend(runs.into_iter().flatten());
    all.extend(manifest.fragments.iter().cloned());
    manifest.set_materialized_fragments(all);
    Ok(())
}

pub fn verify_child_fragment_count(
    child_path: &Path,
    reference: &FragmentManifestRef,
    actual: usize,
) -> Result<()> {
    if actual != reference.fragment_count as usize {
        return Err(Error::corrupt_file(
            child_path.clone(),
            format!(
                "child manifest fragment count mismatch: ref says {}, file has {}",
                reference.fragment_count, actual
            ),
        ));
    }
    Ok(())
}

pub async fn read_fragment_manifest(
    object_store: &ObjectStore,
    path: &Path,
    known_size: Option<u64>,
) -> Result<Vec<Fragment>> {
    let proto: pb::FragmentManifest = read_tail_proto(object_store, path, known_size).await?;
    let mut interner = DataFileFieldInterner::default();
    proto
        .fragments
        .into_iter()
        .map(|f| interner.intern_fragment(f))
        .collect()
}

pub async fn write_fragment_manifest_file(
    object_store: &ObjectStore,
    path: &Path,
    fragments: &[Fragment],
) -> Result<WriteResult> {
    let proto = pb::FragmentManifest {
        fragments: fragments.iter().map(pb::DataFragment::from).collect(),
    };
    let mut writer = ObjectWriter::new(object_store, path).await?;
    let pos = writer.write_protobuf(&proto).await?;
    writer
        .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
        .await?;
    Writer::shutdown(&mut writer).await
}

pub fn dataset_root_of_manifest(manifest_path: &Path) -> Path {
    let parts: Vec<String> = manifest_path
        .parts()
        .map(|p| p.as_ref().to_string())
        .collect();
    let keep = parts.len().saturating_sub(2);
    Path::from(parts[..keep].join("/"))
}

pub fn child_full_path(root: &Path, relative: &str) -> Path {
    let root = root.to_string();
    if root.is_empty() {
        Path::from(relative)
    } else {
        Path::from(format!("{root}/{relative}"))
    }
}

async fn read_tail_proto<M: prost::Message + Default>(
    object_store: &ObjectStore,
    path: &Path,
    known_size: Option<u64>,
) -> Result<M> {
    let file_size = if let Some(known_size) = known_size {
        known_size
    } else {
        object_store.inner.head(path).await?.size
    };
    const PREFETCH_SIZE: u64 = 64 * 1024;
    let initial_start = file_size.saturating_sub(PREFETCH_SIZE);
    let range = Range {
        start: initial_start,
        end: file_size,
    };
    let buf = object_store.inner.get_range(path, range).await?;

    if (buf.len() < 16 || !buf.ends_with(MAGIC)) && known_size.is_some() {
        return Box::pin(read_tail_proto::<M>(object_store, path, None)).await;
    }

    if buf.len() < 16 {
        return Err(Error::corrupt_file(
            path.clone(),
            "Invalid format: file size is smaller than 16 bytes".to_string(),
        ));
    }
    if !buf.ends_with(MAGIC) {
        return Err(Error::corrupt_file(
            path.clone(),
            "Invalid format: magic number does not match".to_string(),
        ));
    }
    let message_pos = LittleEndian::read_i64(&buf[buf.len() - 16..buf.len() - 8]) as usize;
    let message_len = file_size as usize - message_pos;

    let buf: Bytes = if message_len <= buf.len() {
        buf.slice(buf.len() - message_len..buf.len())
    } else {
        let mut buf2: BytesMut = object_store
            .inner
            .get_range(
                path,
                Range {
                    start: message_pos as u64,
                    end: file_size - PREFETCH_SIZE,
                },
            )
            .await?
            .into_iter()
            .collect();
        buf2.extend_from_slice(&buf);
        buf2.freeze()
    };

    let recorded_length = LittleEndian::read_u32(&buf[0..4]) as usize;
    let buf = buf.slice(4..buf.len() - 16);

    if buf.len() != recorded_length {
        return Err(Error::invalid_input(format!(
            "Invalid format: message length does not match. Expected {}, got {}",
            recorded_length,
            buf.len()
        )));
    }

    Ok(M::decode(buf)?)
}

#[instrument(level = "debug", skip(object_store, manifest))]
pub async fn read_manifest_indexes(
    object_store: &ObjectStore,
    location: &ManifestLocation,
    manifest: &Manifest,
) -> Result<Vec<IndexMetadata>> {
    if let Some(pos) = manifest.index_section.as_ref() {
        let reader = if let Some(size) = location.size {
            object_store
                .open_with_size(&location.path, size as usize)
                .await?
        } else {
            object_store.open(&location.path).await?
        };
        let section: pb::IndexSection = read_message(reader.as_ref(), *pos).await?;

        let indices = section
            .indices
            .into_iter()
            .map(IndexMetadata::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(indices)
    } else {
        Ok(vec![])
    }
}

async fn do_write_manifest(
    writer: &mut dyn Writer,
    manifest: &mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    mut transaction: Option<Transaction>,
) -> Result<usize> {
    // Write indices if presented.
    if let Some(indices) = indices.as_ref() {
        let section = pb::IndexSection {
            indices: indices.iter().map(|i| i.into()).collect(),
        };
        let pos = writer.write_protobuf(&section).await?;
        manifest.index_section = Some(pos);
    }

    // Write inline transaction if presented.
    if let Some(tx) = transaction.take() {
        // Convert to protobuf at the write boundary to persist inline
        let pb_tx: pb::Transaction = tx.into();
        let pos = writer.write_protobuf(&pb_tx).await?;
        manifest.transaction_section = Some(pos);
    }

    writer.write_struct(manifest).await
}

/// Write manifest to an open file.
pub async fn write_manifest(
    writer: &mut dyn Writer,
    manifest: &mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    transaction: Option<Transaction>,
) -> Result<usize> {
    // Write dictionary values.
    let max_field_id = manifest.schema.max_field_id().unwrap_or(-1);
    let is_legacy_storage = manifest.should_use_legacy_format();
    for field_id in 0..max_field_id + 1 {
        if let Some(field) = manifest.schema.mut_field_by_id(field_id)
            && field.data_type().is_dictionary()
            && is_legacy_storage
        {
            let dict_info = field.dictionary.as_mut().ok_or_else(|| {
                Error::io(format!("Lance field {} misses dictionary info", field.name))
            })?;

            let value_arr = dict_info.values.as_ref().ok_or_else(|| {
                Error::io(format!(
                    "Lance field {} is dictionary type, but misses the dictionary value array",
                    field.name
                ))
            })?;

            let data_type = value_arr.data_type();
            let pos = match data_type {
                dt if dt.is_numeric() => {
                    let mut encoder = PlainEncoder::new(writer, dt);
                    encoder.encode(&[value_arr]).await?
                }
                dt if dt.is_binary_like() => {
                    let mut encoder = BinaryEncoder::new(writer);
                    encoder.encode(&[value_arr]).await?
                }
                _ => {
                    return Err(Error::schema(format!(
                        "Does not support {} as dictionary value type",
                        value_arr.data_type()
                    )));
                }
            };
            dict_info.offset = pos;
            dict_info.length = value_arr.len();
        }
    }

    do_write_manifest(writer, manifest, indices, transaction).await
}

/// Implementation of ManifestProvider that describes a Lance file by writing
/// a manifest that contains nothing but default fields and the schema
pub struct ManifestDescribing {}

#[async_trait]
impl PreviousManifestProvider for ManifestDescribing {
    async fn store_schema(
        object_writer: &mut dyn Writer,
        schema: &Schema,
    ) -> Result<Option<usize>> {
        let mut manifest = Manifest::new(
            schema.clone(),
            Arc::new(vec![]),
            DataStorageFormat::new(LanceFileVersion::Legacy),
            HashMap::new(),
        );
        let pos = do_write_manifest(object_writer, &mut manifest, None, None).await?;
        Ok(Some(pos))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{Int32Array, RecordBatch};
    use std::collections::HashMap;

    use crate::format::SelfDescribingFileReader;
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use lance_file::format::{MAGIC, MAJOR_VERSION, MINOR_VERSION};
    use lance_file::previous::{
        reader::FileReader as PreviousFileReader, writer::FileWriter as PreviousFileWriter,
    };
    use rand::{Rng, distr::Alphanumeric};
    use tokio::io::AsyncWriteExt;

    use super::*;

    async fn test_roundtrip_manifest(prefix_size: usize, manifest_min_size: usize) {
        let store = ObjectStore::memory();
        let path = Path::from("/read_large_manifest");

        let mut writer = store.create(&path).await.unwrap();

        // Write prefix we should ignore
        let prefix: Vec<u8> = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(prefix_size)
            .collect();
        writer.write_all(&prefix).await.unwrap();

        let long_name: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(manifest_min_size)
            .map(char::from)
            .collect();

        let arrow_schema =
            ArrowSchema::new(vec![ArrowField::new(long_name, DataType::Int64, false)]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let mut config = HashMap::new();
        config.insert("key".to_string(), "value".to_string());

        let mut manifest = Manifest::new(
            schema,
            Arc::new(vec![]),
            DataStorageFormat::default(),
            HashMap::new(),
        );
        let pos = write_manifest(writer.as_mut(), &mut manifest, None, None)
            .await
            .unwrap();
        writer
            .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
            .await
            .unwrap();
        Writer::shutdown(writer.as_mut()).await.unwrap();

        let roundtripped_manifest = read_manifest(&store, &path, None).await.unwrap();

        assert_eq!(manifest, roundtripped_manifest);

        store.inner.delete(&path).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_large_manifest() {
        test_roundtrip_manifest(0, 100_000).await;
        test_roundtrip_manifest(1000, 100_000).await;
        test_roundtrip_manifest(1000, 1000).await;
    }

    /// A tiered manifest seals older fragments into immutable children and keeps
    /// only the buffer tail inline. Reading it back must reproduce the full flat
    /// fragment list and the root must not inline the sealed fragments.
    #[tokio::test]
    async fn tiered_manifest_round_trips_through_children() {
        use crate::format::TieredLayout;

        let store = ObjectStore::memory();
        let root = "mydata";
        let buffer_cap = 100;
        let version = 1u64;

        // 250 fragments, cap 100 → two sealed children + a 50-fragment buffer.
        let baseline: Vec<Fragment> = (0..250)
            .map(|id| Fragment::new(id).with_physical_rows((id as usize % 7) + 1))
            .collect();

        let layout = TieredLayout::seal(baseline.clone(), buffer_cap, version);
        let refs = layout.child_refs();
        assert_eq!(refs.len(), 2);

        // Persist each immutable child at {root}/{ref.path}.
        for child in layout.children() {
            let min = child.reference.min_fragment_id;
            let max = child.reference.max_fragment_id;
            assert!(
                child
                    .reference
                    .path
                    .starts_with(&format!("_manifest_children/v{version}-{min}-{max}-")),
                "unexpected child path {}",
                child.reference.path
            );
            let full = Path::from(format!("{root}/{}", child.reference.path));
            write_fragment_manifest_file(&store, &full, &child.fragments)
                .await
                .unwrap();
        }

        // Build and write the tiered root: full fragment list in memory, child
        // refs attached. `From<&Manifest>` strips the sealed fragments out.
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new("i", DataType::Int64, false)]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let mut manifest = Manifest::new(
            schema,
            Arc::new(baseline.clone()),
            DataStorageFormat::default(),
            HashMap::new(),
        );
        manifest.child_manifests = refs.clone();

        let manifest_path = Path::from(format!("{root}/_versions/1.manifest"));
        let mut writer = store.create(&manifest_path).await.unwrap();
        let pos = write_manifest(writer.as_mut(), &mut manifest, None, None)
            .await
            .unwrap();
        writer
            .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
            .await
            .unwrap();
        Writer::shutdown(writer.as_mut()).await.unwrap();

        // The root proto inlines only the 50-fragment buffer, not the 200 sealed.
        let raw: pb::Manifest = read_tail_proto(&store, &manifest_path, None).await.unwrap();
        assert!(raw.fragments.is_empty());
        assert_eq!(raw.buffer_fragments.len(), 50);
        assert_eq!(raw.child_manifests.len(), 2);

        // Reading back materializes children + buffer into the full flat list.
        let reopened = read_manifest(&store, &manifest_path, None).await.unwrap();
        assert!(reopened.is_tiered());
        assert_eq!(reopened.child_manifests, refs);
        let got: Vec<(u64, Option<usize>)> = reopened
            .fragments
            .iter()
            .map(|f| (f.id, f.num_rows()))
            .collect();
        let want: Vec<(u64, Option<usize>)> =
            baseline.iter().map(|f| (f.id, f.num_rows())).collect();
        assert_eq!(got, want);

        // Logical-row routing matches a flat offset search across the boundary.
        for (offset, fragment) in reopened.fragments_by_offset_range(0..3) {
            assert!(offset < 3 || fragment.id == 0);
        }
    }

    #[tokio::test]
    async fn test_update_schema_metadata() {
        let store = ObjectStore::memory();
        let path = Path::from("/update_schema_metadata");

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
        let mut file_writer = PreviousFileWriter::<ManifestDescribing>::try_new(
            &store,
            &path,
            schema.clone(),
            &Default::default(),
        )
        .await
        .unwrap();

        let array = Int32Array::from_iter_values(0..10);
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap();
        file_writer
            .write(std::slice::from_ref(&batch))
            .await
            .unwrap();
        let mut metadata = HashMap::new();
        metadata.insert(String::from("lance:extra"), String::from("for_test"));
        file_writer.finish_with_metadata(&metadata).await.unwrap();

        let reader = store.open(&path).await.unwrap();
        let reader = PreviousFileReader::try_new_self_described_from_reader(reader.into(), None)
            .await
            .unwrap();
        let schema = ArrowSchema::from(reader.schema());
        assert_eq!(schema.metadata().get("lance:extra").unwrap(), "for_test");
    }
}
