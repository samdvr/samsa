fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // Generate protobuf types with tonic
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("samsa_descriptor.bin"))
        .compile_protos(&["proto/samsa.proto"], &["proto"])?;

    // Generate serde implementations only for types we need in PostgreSQL metadata repository
    // Exclude types that have FieldMask dependencies to avoid serde issues
    let types_to_include = vec![
        ".samsa.v1.BucketConfig",
        ".samsa.v1.StreamConfig",
        ".samsa.v1.AccessTokenInfo",
        ".samsa.v1.AccessTokenScope",
        ".samsa.v1.ResourceSet",
        ".samsa.v1.StorageClass",
        ".samsa.v1.TimestampingMode",
        ".samsa.v1.Operation",
    ];

    pbjson_build::Builder::new()
        .register_descriptors(&std::fs::read(out_dir.join("samsa_descriptor.bin"))?)?
        .build(&types_to_include)?;

    println!("cargo:rerun-if-changed=proto/samsa.proto");
    Ok(())
}
