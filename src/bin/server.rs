use beaker::{Engine, Server};
#[tokio::main]
async fn main() {
    let db = Engine::open("./data").unwrap();
    let server = Server::new(db);

    server.run("127.0.0.1:6379").await.unwrap();
}
