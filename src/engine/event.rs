use super::{
    compact::Compactor,
    manifest::Manifest,
    memtable::{KVTable, MemTable},
    sstable::SSTableManager,
    Error, Result,
};
use crate::util::shutdown::Listener;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{interval, Interval},
};
use tracing::{info, warn};

static EVENT_ID_ALLOC: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub enum Event {
    // Dump a memtable into sstable
    // Trigger
    Dump,

    // Clean inactive reader in sstable reader manager
    // Timed
    InactiveReaderClean,

    // Clean inactive log file
    // Timed
    InactiveLogClean,

    // Clean inactive sstable file
    // Timed or trigger
    InactiveSSTableClean,

    // Find compact task and do it
    // Timed or Trigger
    Compact,
}

pub struct Config {
    pub enable_timer: bool,
    pub inactive_reader_clean_period: Interval,
    pub inactive_log_clean_period: Interval,
    pub inactive_sstable_clean_period: Interval,
    pub compact_period: Interval,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            enable_timer: true,
            inactive_reader_clean_period: interval(Duration::from_secs(60)),
            inactive_log_clean_period: interval(Duration::from_secs(500)),
            inactive_sstable_clean_period: interval(Duration::from_secs(500)),
            compact_period: interval(Duration::from_secs(60 * 10)),
        }
    }
}

pub struct EventLoopBuilder {
    manifest: Option<Manifest>,
    memtable: Option<MemTable>,
    shutdown: Option<Listener>,
    manager: Option<SSTableManager<tokio::fs::File>>,
    tx: EventNotifier,
    rx: UnboundedReceiver<EventMessage>,
}

impl EventLoopBuilder {
    // Create a event loop builder
    pub fn new() -> (EventNotifier, EventLoopBuilder) {
        let (tx, rx) = unbounded_channel();
        let notifier = EventNotifier::new(tx);
        (
            notifier.clone(),
            EventLoopBuilder {
                manifest: None,
                memtable: None,
                shutdown: None,
                manager: None,
                tx: notifier,
                rx,
            },
        )
    }

    // Set up manifest
    pub fn manifest(mut self, m: Manifest) -> Self {
        self.manifest = Some(m);
        self
    }

    // Set up memtable
    pub fn memtable(mut self, m: MemTable) -> Self {
        self.memtable = Some(m);
        self
    }

    // Set up shutdown listener
    pub fn shutdown(mut self, s: Listener) -> Self {
        self.shutdown = Some(s);
        self
    }

    pub fn manager(mut self, m: SSTableManager<tokio::fs::File>) -> Self {
        self.manager = Some(m);
        self
    }

    // Start a event loop
    pub fn run(mut self, mut config: Config) {
        assert!(self.manifest.is_some());
        assert!(self.memtable.is_some());
        assert!(self.shutdown.is_some());
        assert!(self.manager.is_some());
        let tx = self.tx.clone();
        // Start timer
        let mut shutdown = self.shutdown.take().unwrap();
        let mut timer_shutdown = shutdown.clone();

        if config.enable_timer {
            tokio::spawn(async move {
                loop {
                    if let Err(err) = tokio::select! {
                        _ = timer_shutdown.listen() => {
                            // Receive shutdown signal, stop timer
                            break;
                        }
                        _ = config.inactive_reader_clean_period.tick() => {
                            tx.notify(Event::InactiveReaderClean)
                        }
                        _ = config.inactive_log_clean_period.tick() => {
                            tx.notify(Event::InactiveLogClean)
                        }
                        _ = config.inactive_sstable_clean_period.tick() => {
                            tx.notify(Event::InactiveSSTableClean)
                        }
                        _ = config.compact_period.tick() => {
                            tx.notify(Event::Compact)
                        }
                    } {
                        warn!(error = err.to_string(), "send event from timer failed");
                    }
                }
            });
        }

        // Start background task
        tokio::spawn(async move {
            let manifest = self.manifest.take().unwrap();
            let memtable = self.memtable.take().unwrap();
            let manager = self.manager.take().unwrap();
            let mut rx = self.rx;

            info!("start a background task event loop...");
            loop {
                tokio::select! {
                    _ = shutdown.listen() => {
                        // Receive shutdown signal, stop creating new background task
                        break;
                    }
                    event = rx.recv() => {
                        // Receive new event, start a background task to handle it
                        if let Some(event) = event {
                            EventHandler::new(manifest.clone(), memtable.clone(), manager.clone(),shutdown.clone(),event ).handle_event();
                        } else {
                            break;
                        }
                    }
                }
            }
            info!("background task event loop exit");
        });
    }
}

#[derive(Clone)]
pub struct EventMessage {
    event: Event,
    _done: Option<mpsc::Sender<()>>,
}

impl EventMessage {
    // hold the message in scope, do nothing
    #[inline]
    fn hold(&self) {}
}

pub struct EventWatcher {
    _rx: mpsc::Receiver<()>,
    _done: bool,
}

impl EventWatcher {
    #[inline]
    fn new(rx: mpsc::Receiver<()>) -> Self {
        EventWatcher {
            _rx: rx,
            _done: false,
        }
    }

    #[cfg(test)]
    #[inline]
    pub async fn done(&mut self) {
        if self._done {
            return;
        }
        self._rx.recv().await;
    }

    #[cfg(test)]
    #[inline]
    pub fn is_done(&mut self) -> bool {
        if self._done {
            return true;
        }

        if matches!(
            self._rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ) {
            self._done = true;
            return true;
        }
        false
    }
}

#[derive(Clone)]
pub struct EventNotifier {
    sender: UnboundedSender<EventMessage>,
}

impl EventNotifier {
    // Create a new event notifier
    pub fn new(sender: UnboundedSender<EventMessage>) -> Self {
        EventNotifier { sender }
    }

    // Notify a event
    pub fn notify(&self, event: Event) -> Result<EventWatcher> {
        let (tx, rx) = mpsc::channel(1);
        self.sender
            .send(EventMessage {
                event,
                _done: Some(tx),
            })
            .map_err(|_e| Error::Other)?;

        Ok(EventWatcher::new(rx))
    }
}

struct EventHandler {
    manifest: Manifest,
    memtable: MemTable,
    manager: SSTableManager<tokio::fs::File>,
    shutdown: Listener,
    id: u64,
    event: EventMessage,
}

impl EventHandler {
    // Create a new event handler
    fn new(
        manifest: Manifest,
        memtable: MemTable,
        manager: SSTableManager<tokio::fs::File>,
        shutdown: Listener,
        event: EventMessage,
    ) -> Self {
        let id = EVENT_ID_ALLOC.fetch_add(1, Ordering::Relaxed);
        EventHandler {
            manifest,
            memtable,
            manager,
            shutdown,
            id,
            event,
        }
    }

    // Handle a incoming event
    fn handle_event(self) {
        tokio::spawn(async move {
            match self.event.event {
                Event::Dump => {
                    self.dump().await;
                }
                Event::InactiveLogClean => {
                    self.clean_inactive_log().await;
                }
                Event::InactiveReaderClean => {
                    self.clean_inactive_readers().await;
                }
                Event::InactiveSSTableClean => {
                    self.clean_inactive_sstable().await;
                }
                Event::Compact => {
                    self.compact().await;
                }
            }
        });
    }

    // Do compation task when compact event is coming
    async fn compact(self) {
        info!(id = self.id, "start find new compact task...");
        for (offset, task) in self
            .manifest
            .find_new_compact_tasks()
            .into_iter()
            .enumerate()
        {
            let manifest = self.manifest.clone();
            let manager = self.manager.clone();
            info!(id = self.id, "compact task found, {:?}", task);

            // Hold a shutdown and event message copy, to block shutdown util compact task finish and notify compact was finished
            let new_shutdown_listener = self.shutdown.clone();
            let event = self.event.clone();
            tokio::spawn(async move {
                new_shutdown_listener.hold();
                event.hold();

                info!(id = self.id, offset, "start do compact task");
                if let Err(err) = Compactor::new(task, manifest, manager).compact().await {
                    warn!(
                        id = self.id,
                        offset,
                        error = err.to_string(),
                        "compact task failed"
                    )
                } else {
                    info!(id = self.id, offset, "compact task was finished");
                }
            });
        }
        info!(id = self.id, "find new compact task finish");
    }

    // Do a dump task when dump event is coming
    async fn dump(self) {
        let id = self.id;
        info!(id, "start a new dump task...");

        let memtable = self.memtable.clone();
        let manifest = self.manifest.clone();

        if let Err(err) = memtable
            .dump_oldest_immutable(|tables: Vec<Arc<KVTable>>| {
                Box::pin(async move {
                    for table in tables.iter() {
                        if table.len() == 0 {
                            continue;
                        }

                        // Build a new sstable from memtable if memtable is not empty
                        let entry = manifest.alloc_new_sstable_entry(0);
                        let mut builder = entry.open_builder().await?;

                        for (key, value) in table.iter() {
                            builder.add(key.clone(), value.clone()).await?;
                        }
                        let entry = builder.finish().await?;
                        // Register the new sstable into manifest
                        manifest.add_new_sstable(entry)?;
                    }

                    Ok(())
                })
            })
            .await
        {
            warn!(id, error = err.to_string(), "dump task failed");
        } else {
            info!(id, "dump task was finished");
        }
    }

    // Clean inactive log
    async fn clean_inactive_log(self) {
        info!(id = self.id, "start clean inactive log...");
        if let Err(err) = self.memtable.clean_inactive_log().await {
            warn!(
                id = self.id,
                error = err.to_string(),
                "clean inactive log was failed"
            );
        } else {
            info!(id = self.id, "clean inactive log was finished");
        }
    }

    // Clean inactive sstable
    async fn clean_inactive_sstable(self) {
        info!(id = self.id, "start clean inactive sstable...");
        if let Err(err) = self.manifest.clean_inactive_sstable().await {
            warn!(
                id = self.id,
                error = err.to_string(),
                "clean inactive sstable was failed"
            );
        } else {
            info!(id = self.id, "clean inactive sstable was finished");
        }
    }

    // Clean inactive reader
    async fn clean_inactive_readers(self) {
        info!(id = self.id, "start a clean inactive reader task...");
        self.manager.clean_inactive_readers(60);
        info!(id = self.id, "clean inactive reader was finished")
    }
}
