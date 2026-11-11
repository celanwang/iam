fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    prost_build::Config::new()
        .bytes(["."])
        .enable_type_names()
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("protocol.bin"),
        )
        .compile_protos(
            &["../../proto/iam/protocol/protocol.proto"],
            &["../../proto"],
        )?;
    Ok(())
}
