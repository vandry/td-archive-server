pub mod openraildata_pb {
    tonic::include_proto!("openraildata");
}

mod archive;
mod common;
mod health;
mod preserve;
mod recent;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    server::main().await
}
