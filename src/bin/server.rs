use beaker::Server;
#[tokio::main]
async fn main() {
    let _ = env_logger::builder().try_init();
    let server = Server::default();
    server.run().await.unwrap();
}
