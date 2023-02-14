use beaker::Cli;

#[tokio::main]
async fn main() {
    let client = Cli::default();
    client.run().await.unwrap();
}
