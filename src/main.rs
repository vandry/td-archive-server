pub mod openraildata_pb {
    tonic::include_proto!("openraildata");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("fdset");
}

mod archive;
mod common;
mod preserve;
mod recent;
mod server;

comprehensive_s3::bucket!(TDArchiveBucket, "TD storage", "");

#[derive(comprehensive::ResourceDependencies)]
struct TopDependencies {
    _grpc_service: std::sync::Arc<server::TDArchiveFeedResource>,
    _server: std::sync::Arc<comprehensive_grpc::server::GrpcServer>,
    _diag: std::sync::Arc<comprehensive_http::diag::HttpServer>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    comprehensive::Assembly::<TopDependencies>::new()?
        .run()
        .await?;
    Ok(())
}
