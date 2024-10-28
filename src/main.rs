pub mod openraildata_pb {
    tonic::include_proto!("openraildata");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("fdset");
}

mod archive;
mod common;
mod preserve;
mod recent;
mod server;

#[derive(comprehensive::ResourceDependencies)]
struct TopDependencies {
    _grpc_service: std::sync::Arc<server::TDArchiveFeedGrpcService>,
    _diag: std::sync::Arc<comprehensive::diag::HttpServer>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    comprehensive::Assembly::<TopDependencies>::new()?
        .run()
        .await?;
    Ok(())
}
