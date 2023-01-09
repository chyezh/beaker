use super::{error::TestError, Error, Request, Response, Result};
use tokio::sync::mpsc;

struct Client {
    name: String,
    connection_channel: mpsc::Sender<Request>,
}

impl Client {
    fn name(&self) -> &str {
        &self.name
    }

    // Do a rpc in this client
    async fn rpc(&self, mut request: Request) -> Result<Response> {
        let receiver = request.take_response_receiver(self.name.clone());
        self.connection_channel
            .send(request)
            .await
            .map_err(|_| TestError::ChannelLoss)?;

        receiver.await.map_err(|_| TestError::ChannelLoss)?
    }
}
