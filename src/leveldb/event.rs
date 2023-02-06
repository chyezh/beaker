use super::{
    compact::Compactor,
    manifest::Manifest,
    memtable::{KVTable, MemTable},
};
use crate::{leveldb::sstable, util::shutdown::Listener};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{interval, Interval},
};
use tracing::{info, warn};

static EVENT_ID_ALLOC: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
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
    tx: UnboundedSender<Event>,
    rx: UnboundedReceiver<Event>,
}

impl EventLoopBuilder {
    // Create a event loop builder
    pub fn new() -> (UnboundedSender<Event>, EventLoopBuilder) {
        let (tx, rx) = unbounded_channel();
        (
            tx.clone(),
            EventLoopBuilder {
                manifest: None,
                memtable: None,
                shutdown: None,
                tx,
                rx,
            },
        )
    }

    // Set up manifest
    pub fn manifest(&mut self, m: Manifest) -> &mut Self {
        self.manifest = Some(m);
        self
    }

    // Set up memtable
    pub fn memtable(&mut self, m: MemTable) -> &mut Self {
        self.memtable = Some(m);
        self
    }

    // Set up shutdown listener
    pub fn shutdown(&mut self, s: Listener) -> &mut Self {
        self.shutdown = Some(s);
        self
    }

    // Start a event loop
    pub fn run(mut self, mut config: Config) {
        assert!(self.manifest.is_some());
        assert!(self.memtable.is_some());
        assert!(self.shutdown.is_some());
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
                            tx.send(Event::InactiveReaderClean)
                        }
                        _ = config.inactive_log_clean_period.tick() => {
                            tx.send(Event::InactiveLogClean)
                        }
                        _ = config.inactive_sstable_clean_period.tick() => {
                            tx.send(Event::InactiveSSTableClean)
                        }
                        _ = config.compact_period.tick() => {
                            tx.send(Event::Compact)
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
            let mut rx = self.rx;

            info!("Start a background task event loop...");
            loop {
                tokio::select! {
                    _ = shutdown.listen() => {
                        // Receive shutdown signal, stop creating new background task
                        break;
                    }
                    event = rx.recv() => {
                        // Receive new event, start a background task to handle it
                        if let Some(event) = event {
                            EventHandler::new(manifest.clone(), memtable.clone(),shutdown.clone()).handle_event(event);
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

struct EventHandler {
    manifest: Manifest,
    memtable: MemTable,
    shutdown: Listener,
    id: u64,
}

impl EventHandler {
    // Create a new event handler
    fn new(manifest: Manifest, memtable: MemTable, shutdown: Listener) -> Self {
        let id = EVENT_ID_ALLOC.fetch_add(1, Ordering::Relaxed);
        EventHandler {
            manifest,
            memtable,
            shutdown,
            id,
        }
    }

    // Handle a incoming event
    fn handle_event(self, event: Event) {
        tokio::spawn(async move {
            match event {
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
            info!(id = self.id, "compact task found, {:?}", task);

            // Hold a listener copy, to block shutdown util compact task finish
            let new_shutdown_listener = self.shutdown.clone();
            tokio::spawn(async move {
                new_shutdown_listener.hold();
                info!(id = self.id, offset, "start do compact task");
                if let Err(err) = Compactor::new(task, manifest).compact().await {
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
        sstable::clean_inactive_readers(60);
        info!(id = self.id, "clean inactive reader was finished")
    }
}
