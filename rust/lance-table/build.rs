// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::io::Result;

fn main() -> Result<()> {
    // Watch each proto file explicitly. `protos/` is a symlink to the repo
    // root; directory-level rerun-if-changed can miss content updates and leave
    // release builds on a stale generated `lance.table` module.
    println!("cargo:rerun-if-changed=protos/table.proto");
    println!("cargo:rerun-if-changed=protos/transaction.proto");
    println!("cargo:rerun-if-changed=protos/rowids.proto");

    #[cfg(feature = "protoc")]
    // Use vendored protobuf compiler if requested.
    unsafe {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }

    let mut prost_build = prost_build::Config::new();
    prost_build.extern_path(".lance.file", "::lance_file::format::pb");
    prost_build.protoc_arg("--experimental_allow_proto3_optional");
    prost_build.enable_type_names();
    for name in [
        "FragmentMetadataTree",
        "FragmentMetadataRoot",
        "FragmentMetadataChild",
        "FragmentMetadataMutation",
        "FragmentAction",
        "AddDataFile",
        "RemoveDataFile",
        "ReplaceDataFile",
        "AddDeletionFile",
        "ClearDeletionFile",
        "DataFragment",
        "DataFile",
        "DataOverlayFile",
        "FieldCoverage",
        "DeletionFile",
        "ExternalFile",
    ] {
        prost_build.type_attribute(
            format!(".lance.table.{name}"),
            "#[derive(lance_core::deepsize::DeepSizeOf)]",
        );
    }
    prost_build.compile_protos(
        &[
            "./protos/table.proto",
            "./protos/transaction.proto",
            "./protos/rowids.proto",
        ],
        &["./protos"],
    )?;

    Ok(())
}
