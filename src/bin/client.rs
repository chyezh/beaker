use beaker::Client;
use bytes::Bytes;

#[tokio::main]
async fn main() {
    let _ = env_logger::builder().try_init();
    let client = Client::connect("127.0.0.1:6379").await.unwrap();
    // println!("{:?}", client.ping(None).await);
    // println!("{:?}", client.get(Bytes::from("123")).await);
    // println!(
    //     "{:?}",
    //     client.set(Bytes::from("123"), Bytes::from("456")).await
    // );
    // println!("{:?}", client.get(Bytes::from("123")).await);
    // // println!("{:?}", client.del(Bytes::from("123")).await);
    // println!("{:?}", client.get(Bytes::from("123")).await);
    println!(
        "{:?}",
        client.ping(Some(Bytes::from("1264782364876"))).await
    );
}
