// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::Dataset;
use crate::session::caches::DeletionFileKey;
use lance_core::utils::deletion::DeletionVector;
use lance_table::format::DeletionFile;
use lance_table::io::deletion::read_deletion_file;
use std::sync::Arc;

pub async fn read_dataset_deletion_file(
    dataset: &Dataset,
    fragment_id: u64,
    deletion_file: &DeletionFile,
) -> lance_core::Result<Arc<DeletionVector>> {
    let dataset_dir = dataset.dataset_dir_for_deletion(deletion_file)?;
    let key = DeletionFileKey {
        fragment_id,
        deletion_file,
    };

    if let Some(cached) = dataset.metadata_cache.get_with_key(&key).await {
        Ok(cached)
    } else {
        let object_store = dataset.object_store_for_deletion(deletion_file).await?;
        let deletion_vector = Arc::new(
            read_deletion_file(
                fragment_id,
                deletion_file,
                &dataset_dir,
                object_store.as_ref(),
            )
            .await?,
        );

        dataset
            .metadata_cache
            .insert_with_key(&key, deletion_vector.clone())
            .await;

        Ok(deletion_vector)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatchIterator, record_batch};
    use lance_table::io::deletion::{DeletionFileNaming, write_deletion_file_with_naming};

    use super::*;
    use crate::dataset::CommitBuilder;
    use crate::dataset::transaction::{Operation, Transaction};

    async fn scanned_values(dataset: &Dataset) -> Vec<i32> {
        let batch = dataset.scan().try_into_batch().await.unwrap();
        batch["x"]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    /// Two deletion files whose fields other than the path are all equal hold
    /// different deletion vectors, even when one session reads both for the
    /// same fragment.
    #[tokio::test]
    async fn explicit_paths_do_not_share_cached_deletion_vectors() {
        let batch = record_batch!(("x", Int32, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])).unwrap();
        let schema = batch.schema();
        let reader = RecordBatchIterator::new([Ok(batch)], schema);
        let dataset = Dataset::write(reader, "memory://", None).await.unwrap();

        let mut deletion_files = Vec::new();
        for offset in [1, 2] {
            deletion_files.push(
                write_deletion_file_with_naming(
                    &dataset.base,
                    DeletionFileNaming::Uuid,
                    dataset.manifest.version,
                    &DeletionVector::from_iter([offset]),
                    dataset.object_store.as_ref(),
                )
                .await
                .unwrap()
                .unwrap(),
            );
        }
        let first = deletion_files[0].clone();
        let second = DeletionFile {
            id: first.id,
            ..deletion_files[1].clone()
        };
        assert_eq!(
            DeletionFile {
                path: None,
                ..first.clone()
            },
            DeletionFile {
                path: None,
                ..second.clone()
            },
            "only the path tells the two files apart"
        );
        assert_ne!(first.path, second.path);

        // Fragment 0 references `first` in one version and `second` in the next.
        let mut versions = Vec::new();
        let mut current = dataset;
        for deletion_file in [&first, &second] {
            let mut fragment = current.get_fragment(0).unwrap().metadata().clone();
            fragment.deletion_file = Some(deletion_file.clone());
            let read_version = current.manifest.version;
            current = CommitBuilder::new(Arc::new(current))
                .execute(Transaction::new(
                    read_version,
                    Operation::Delete {
                        updated_fragments: vec![fragment],
                        deleted_fragment_ids: vec![],
                        predicate: "x in (1, 2)".to_string(),
                    },
                    None,
                ))
                .await
                .unwrap();
            versions.push(current.clone());
        }
        let [with_first, with_second] = versions.try_into().unwrap();
        assert!(Arc::ptr_eq(&with_first.session(), &with_second.session()));

        // Read `first` into the shared cache, then `second`.
        let first_vector = read_dataset_deletion_file(&with_first, 0, &first)
            .await
            .unwrap();
        let second_vector = read_dataset_deletion_file(&with_second, 0, &second)
            .await
            .unwrap();
        assert_eq!(*first_vector, DeletionVector::from_iter([1]));
        assert_eq!(*second_vector, DeletionVector::from_iter([2]));
        assert_eq!(
            scanned_values(&with_first).await,
            [0, 2, 3, 4, 5, 6, 7, 8, 9]
        );
        assert_eq!(
            scanned_values(&with_second).await,
            [0, 1, 3, 4, 5, 6, 7, 8, 9]
        );
    }
}
