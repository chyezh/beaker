use beaker::{Server, DB};
#[tokio::main]
async fn main() {
    let _ = env_logger::builder().try_init();
    let db = DB::open("./data").unwrap();
    let server = Server::new(db);

    server.run("127.0.0.1:6379").await.unwrap();
}
