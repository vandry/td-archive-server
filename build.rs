fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fds_path =
        std::path::PathBuf::from(std::env::var("OUT_DIR").expect("$OUT_DIR")).join("fdset.bin");
    tonic_build::configure()
        .file_descriptor_set_path(fds_path)
        .proto_path("crate::preserve")
        .compile_protos(&["proto/td_feed.proto", "proto/td_index.proto"], &["proto"])?;
    Ok(())
}
