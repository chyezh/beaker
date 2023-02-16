use std::path::PathBuf;
struct Snapshot {
    logs: Vec<PathBuf>,
    sstables: Vec<PathBuf>,
    manifest: Vec<PathBuf>,
}
