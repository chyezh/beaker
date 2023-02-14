use super::{error::TestError, Request, Response, Result};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Client {
    name: String,
    sender: mpsc::Sender<Request>,
}

impl Client {
    pub fn new(name: String, sender: mpsc::Sender<Request>) -> Self {
        Client { name, sender }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    // Do a rpc in this client
    pub async fn rpc(&self, mut request: Request) -> Result<Response> {
        let receiver = request.take_response_receiver(self.name.clone());
        self.sender
            .send(request)
            .await
            .map_err(|_| TestError::ChannelLoss)?;

        receiver.await.map_err(|_| TestError::ChannelLoss)?
    }
}
