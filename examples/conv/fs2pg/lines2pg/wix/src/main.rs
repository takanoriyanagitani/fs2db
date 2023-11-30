use std::net::SocketAddr;

use fs2db::tonic;
use tonic::transport::{server::Router, Server};

use fs2db::rpc::fs2db::proto::source;
use source::v1::select_service_server::SelectServiceServer;

const LISTEN_ADDR_DEFAULT: &str = "127.0.0.1:50051";

#[tokio::main]
async fn main() -> Result<(), String> {
    let src_svc = wix::source::lines::fs2rows::FsSvc::new_default()
        .map_err(|e| format!("Unable to create a source select service: {e}"))?;
    let src_svr: SelectServiceServer<_> = SelectServiceServer::new(src_svc);

    let mut sv: Server = Server::builder();

    let router: Router<_> = sv.add_service(src_svr);

    let addr: String = std::env::var("ENV_LISTEN_ADDR")
        .ok()
        .unwrap_or_else(|| LISTEN_ADDR_DEFAULT.into());
    let sa: SocketAddr = str::parse(addr.as_str()).map_err(|e| format!("Invalid addr: {e}"))?;

    router
        .serve(sa)
        .await
        .map_err(|e| format!("Unable to listen: {e}"))?;

    Ok(())
}
