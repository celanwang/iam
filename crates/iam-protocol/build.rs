fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    prost_build::Config::new()
        .bytes(["."])
        .compile_protos(&["../../proto/iam/protocol/protocol.proto"], &["../../proto"])?;
    Ok(())
}
