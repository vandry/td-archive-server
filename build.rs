fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().proto_path("crate::preserve").compile(
        &["proto/td_feed.proto", "proto/td_index.proto"],
        &["proto"]
    )?;
    Ok(())
}
