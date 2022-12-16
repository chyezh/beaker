use std::{fs::File, path::PathBuf};

use super::record::{RecordReader, RecordType, RecordWriter};

pub struct LogWriter {
    // log file path
    path: PathBuf,
    // root of log file
    root: PathBuf,
    writer: RecordWriter<File>,
}
