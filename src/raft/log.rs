use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

use super::{
    record::{RecordReader, RecordWriter},
    Error, Result,
};
use crate::util::from_le_bytes_64;

const LOG_HEADER: usize = 4 * 4; // term + index + commit_index + last_applied
const LOG_FILE_EXTENSION: &str = "log";
const SPLIT_LOG_SIZE_THRESHOLD: usize = 4 * 1024 * 1024; // 4M

pub struct Log<'a> {
    term: u64,         // term of this log item
    index: u64,        // index of this log item
    commit_index: u64, // commit index when writing this log item
    last_applied: u64, // last applied index when writing this log item
    content: &'a [u8], // log content
}

impl<'a> Log<'a> {
    fn decode(v: &'a [u8]) -> Result<Log<'a>> {
        if v.len() < LOG_HEADER {
            return Err(Error::IllegalLog);
        }

        let term = from_le_bytes_64(&v[0..8]);
        let index = from_le_bytes_64(&v[8..16]);
        let commit_index = from_le_bytes_64(&v[16..24]);
        let last_applied = from_le_bytes_64(&v[24..32]);
        Ok(Log {
            term,
            index,
            commit_index,
            last_applied,
            content: &v[32..],
        })
    }

    // Encode the log into binary format
    // 1. 0-8 term
    // 2. 8-16 index
    // 3. 16-24 commit_index
    // 4. 24-32 last_applied
    // 5. content
    fn encode(self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.content.len() + LOG_HEADER);
        v.extend_from_slice(&self.term.to_le_bytes()[0..8]);
        v.extend_from_slice(&self.index.to_le_bytes()[0..8]);
        v.extend_from_slice(&self.commit_index.to_le_bytes()[0..8]);
        v.extend_from_slice(&self.last_applied.to_le_bytes()[0..8]);
        v.extend_from_slice(self.content);
        v
    }
}

pub struct LogWriter {
    root_path: PathBuf,
    w: RecordWriter<File>,
    next_log_seq: u64,
}

impl LogWriter {
    // Write a new log
    fn write(&mut self, log: Log) -> Result<()> {
        self.w.append(log.encode())?;

        // TODO: configurable threshold
        // Split log, create a new log to write
        if self.w.written() > SPLIT_LOG_SIZE_THRESHOLD {
            (self.w, self.next_log_seq) = open_new_log(&self.root_path, self.next_log_seq)?;
        }
        Ok(())
    }
}

pub struct LogReader {
    log_files: Vec<(PathBuf, u64)>,
    offset: usize,
    reader: Option<RecordReader<File>>,
    now_log: Vec<u8>,
}

impl LogReader {
    fn now(&self) -> Result<Log> {
        Log::decode(&self.now_log)
    }
}

impl Iterator for LogReader {
    type Item = Result<()>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(reader) = self.reader.as_mut() {
                if let Some(parse_result) = reader.next() {
                    return match parse_result {
                        Ok(data) => {
                            self.now_log = data;
                            Some(Ok(()))
                        }
                        Err(e) => Some(Err(e)),
                    };
                }
                self.reader = None;
            } else {
                if self.offset >= self.log_files.len() {
                    return None;
                }

                let file = File::open(&self.log_files[self.offset].0);
                self.offset += 1;
                match file {
                    Ok(f) => {
                        self.reader = Some(RecordReader::new(f));
                    }
                    Err(err) => {
                        return Some(Err(Error::from(err)));
                    }
                }
            }
        }
    }
}

// Open a new log in given root path
pub fn open(root_path: impl Into<PathBuf>) -> Result<(LogReader, LogWriter)> {
    // Try to create log root directory at this path.
    let root_path: PathBuf = root_path.into();
    fs::create_dir_all(root_path.clone())?;

    // Scan the root_path directory, find all log sorted by sequence number
    let log_files = scan_sorted_file_at_path(&root_path)?;

    // Find last sequence number
    let next_log_seq = if log_files.is_empty() {
        log_files.last().unwrap().1 + 1
    } else {
        0
    };

    // Open a new empty logger
    let (w, next_log_seq) = open_new_log(&root_path, next_log_seq)?;

    Ok((
        LogReader {
            log_files,
            offset: 0,
            reader: None,
            now_log: vec![],
        },
        LogWriter {
            root_path,
            w,
            next_log_seq,
        },
    ))
}

// Open a new file for record writer to write log
fn open_new_log(root_path: &Path, log_seq: u64) -> Result<(RecordWriter<File>, u64)> {
    let new_log_path = log_path(root_path, log_seq);
    let new_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(new_log_path)?;
    Ok((RecordWriter::new(new_file), log_seq + 1))
}

// Scan given log root directory and get all sorted log file
fn scan_sorted_file_at_path(path: &Path) -> Result<Vec<(PathBuf, u64)>> {
    // Example of log file name format: 1.log 2.log
    let mut filenames: Vec<_> = fs::read_dir(path)?
        .flat_map(|elem| -> Result<PathBuf> { Ok(elem?.path()) }) // filter read_dir failed paths
        .filter_map(|elem| {
            if !elem.is_file() || elem.extension() != Some(LOG_FILE_EXTENSION.as_ref()) {
                None
            } else if let Some(Ok(log_num)) = elem
                .file_stem()
                .and_then(std::ffi::OsStr::to_str)
                .map(str::parse::<u64>)
            {
                Some((elem, log_num))
            } else {
                None
            }
        }) // Filter illegal filename and get log seq no
        .collect();

    // Sort filenames with log seq no
    filenames.sort_by_key(|elem| elem.1);
    Ok(filenames)
}

// Generate log path
fn log_path(dir: &Path, log_seq: u64) -> PathBuf {
    dir.join(format!("{}.{}", log_seq, LOG_FILE_EXTENSION))
}
