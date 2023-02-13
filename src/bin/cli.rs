use beaker::Cli;

#[tokio::main]
async fn main() {
    let client = Cli::new();
    client.run().await.unwrap();
}
