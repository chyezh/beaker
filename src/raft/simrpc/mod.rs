// Simulation rpc for test

mod client;
mod error;
mod network;
mod server;

use super::Result;
pub use client::Client;
pub use error::TestError;
pub use network::Network;
pub use server::{Server, ServerBuilder, Service};
use tokio::sync::oneshot;

pub struct Request {
    client_name: String,
    service_name: String,
    data: Vec<u8>,
    response_sender: oneshot::Sender<Result<Response>>,
    response_receiver: Option<oneshot::Receiver<Result<Response>>>,
}

impl Request {
    pub fn new(service_name: String, data: Vec<u8>) -> Self {
        let (tx, rx) = oneshot::channel::<Result<Response>>();
        Request {
            client_name: "".to_string(),
            service_name,
            data,
            response_sender: tx,
            response_receiver: Some(rx),
        }
    }

    // Take response receiver
    fn take_response_receiver(
        &mut self,
        client_name: String,
    ) -> oneshot::Receiver<Result<Response>> {
        self.client_name = client_name;
        self.response_receiver.take().unwrap()
    }

    // Receive response
    fn send_response(self, response: Result<Response>) {
        self.response_sender.send(response).unwrap();
    }
}

#[derive(Debug)]
pub struct Response {
    pub data: Vec<u8>,
}
