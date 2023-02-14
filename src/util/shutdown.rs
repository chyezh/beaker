use tokio::sync::{mpsc, watch};
use tracing::debug;

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
        debug!("shutdown notifying...");
        // It's ok if send failed, there's no listener working
        self.sender.send(true).ok();
        self.waiter.0.take();
        self.waiter.1.recv().await;
        debug!("shutdown complete");
    }

    // Create a shutdown signal listener
    #[inline]
    pub fn listen(&self) -> Option<Listener> {
        self.waiter.0.as_ref().map(|sender| Listener {
            listener: self.sender.subscribe(),
            _finisher: Some(sender.clone()),
        })
    }
}

#[derive(Clone)]
pub struct Listener {
    listener: watch::Receiver<bool>,
    // Finish when listener is drop
    _finisher: Option<mpsc::Sender<()>>,
}

impl Listener {
    // Listen whether shutdown signal is received
    #[inline]
    pub async fn listen(&mut self) {
        self.listener.changed().await.unwrap();
    }

    // Manually finish
    #[inline]
    #[allow(dead_code)]
    pub fn finish(&mut self) {
        self._finisher.take();
    }

    // Do nothing, just hold the listener
    #[inline]
    pub fn hold(&self) {}
}
