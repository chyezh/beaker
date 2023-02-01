use tokio::sync::{mpsc, watch};
use tracing::info;

// Shutdown notifier and waiting for application part to graceful shutdown
pub struct Notifier {
    // Shutdown signal broadcast
    sender: watch::Sender<bool>,
    // Wait for things to finish shutdown
    waiter: (Option<mpsc::Sender<()>>, mpsc::Receiver<()>),
}

impl Notifier {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        let (tx, rx) = mpsc::channel(1);

        Notifier {
            sender,
            waiter: (Some(tx), rx),
        }
    }

    // Notify shutdown signal and block util all listener finish its task
    pub async fn notify(&mut self) {
        info!("shutdown notifying...");
        self.sender.send(true).unwrap();
        self.waiter.0.take();
        self.waiter.1.recv().await;
        info!("shutdown complete");
    }

    // Create a shutdown signal listener
    pub fn listen(&self) -> Option<Listener> {
        self.waiter.0.as_ref().map(|sender| Listener {
            listener: self.sender.subscribe(),
            finisher: Some(sender.clone()),
        })
    }
}

#[derive(Clone)]
pub struct Listener {
    listener: watch::Receiver<bool>,
    // Finish when listener is drop
    finisher: Option<mpsc::Sender<()>>,
}

impl Listener {
    // Listen whether shutdown signal is received
    pub async fn listen(&mut self) {
        self.listener.changed().await.unwrap();
    }

    // Manually finish
    pub fn finish(&mut self) {
        self.finisher.take();
    }

    // Do nothing, just hold the listener
    pub fn hold(&self) {}
}
