use super::{Command, Result};
use std::{
    fs,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

const MAX_LOG_FILE_SIZE: u64 = 256 * 1024 * 1024;

// Log-structured storage writer implementation, with auto-split-file feature
pub struct SplittedFileLogWriter {
    // Element: Splitted file path and its sequence-number
    splits: Vec<(PathBuf, u64)>,
    // Directory of log root path
    dir: PathBuf,
    // File of Current checkpoint
    current_file: LogFile,
}

struct LogFile {
    file: File,
    file_seq: u64,
    file_size: u64,
}

impl SplittedFileLogWriter {
    // Open the log directory without checking completeness of log files
    pub fn open_unchecked(path: impl Into<PathBuf>) -> Result<Self> {
        // try to create directory at this path
        let path: PathBuf = path.into();
        fs::create_dir_all(path.clone())?;

        // get all log files
        let mut log_files = scan_sorted_file_at_path(&path)?;

        // open the last log
        let (file, file_seq) = if let Some(last_log) = log_files.last() {
            // open the last log with append mode
            (
                OpenOptions::new()
                    .append(true)
                    .write(true)
                    .open(&last_log.0)?,
                last_log.1,
            )
        } else {
            let seq: u64 = 0;
            let new_log_path = log_path(&path, seq);
            // add to log splitted vec
            log_files.push((new_log_path.clone(), seq));

            // create new log if no existed log
            (
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(new_log_path)?,
                seq,
            )
        };
        let file_size = file.metadata()?.len();

        Ok(SplittedFileLogWriter {
            splits: log_files,
            dir: path,
            current_file: LogFile {
                file,
                file_seq,
                file_size,
            },
        })
    }

    // Write command to log file
    pub fn write_cmd(&mut self, cmd: &Command) -> Result<()> {
        let serde_cmd = serde_json::to_vec(cmd)?;

        // if current file size is exceed, create new log to write
        if serde_cmd.len() as u64 + self.current_file.file_size > MAX_LOG_FILE_SIZE {
            let file_seq = self.current_file.file_seq + 1;
            let new_log_path = log_path(&self.dir, file_seq);
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(new_log_path.clone())?;

            let mut new_log_file = LogFile {
                file,
                file_seq,
                file_size: 0,
            };

            std::mem::swap(&mut self.current_file, &mut new_log_file);
            self.splits.push((new_log_path, file_seq));
        }

        // write command to log file
        self.current_file.file.write_all(&serde_cmd[..])?;

        Ok(())
    }
}

pub struct SplittedFileLogReader {
    de: Box<dyn Iterator<Item = serde_json::Result<Command>>>,
}

impl SplittedFileLogReader {
    // Open the log for reader with given root directory
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        fs::create_dir_all(path.clone())?;

        let log_files = scan_sorted_file_at_path(&path)?;

        if let Some(first) = log_files.first() {
            let mut f: Box<dyn Read> = Box::new(File::open(&first.0)?);

            for elem in log_files.iter().skip(1) {
                f = Box::new(f.chain(File::open(&elem.0)?));
            }

            Ok(SplittedFileLogReader {
                de: Box::new(serde_json::Deserializer::from_reader(f).into_iter::<Command>()),
            })
        } else {
            let r = "".as_bytes();
            let f: Box<dyn Read> = Box::new(r);
            Ok(SplittedFileLogReader {
                de: Box::new(serde_json::Deserializer::from_reader(f).into_iter::<Command>()),
            })
        }
    }
}

impl Iterator for SplittedFileLogReader {
    type Item = Result<Command>;

    fn next(&mut self) -> Option<Self::Item> {
        // generate a new command for every iteration
        self.de.next().map(|cmd| cmd.map_err(super::Error::from))
    }
}

// Generate log path
fn log_path(dir: &Path, log_seq: u64) -> PathBuf {
    dir.join(format!("{}.log", log_seq))
}

// Scan given log root directory and get all sorted log file
fn scan_sorted_file_at_path(path: &Path) -> Result<Vec<(PathBuf, u64)>> {
    // example of log file name format: 1.log 2.log
    let mut filenames: Vec<_> = fs::read_dir(path)?
        .flat_map(|elem| -> Result<PathBuf> { Ok(elem?.path()) }) // filter read_dir failed paths
        .filter_map(|elem| {
            if !elem.is_file() || elem.extension() != Some("log".as_ref()) {
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
        }) // filter illegal filename and get log seq no
        .collect();

    // sort filenames with log seq no
    filenames.sort_by_key(|elem| elem.1);
    Ok(filenames)
}

mod tests {
    #[test]
    fn test_splitted_file_writer() {
        use super::{Command, SplittedFileLogWriter};
        let mut logger = SplittedFileLogWriter::open_unchecked("./data").unwrap();

        logger.write_cmd(&Command::del("123123".into())).unwrap();
        logger.write_cmd(&Command::del("123123".into())).unwrap();
        logger.write_cmd(&Command::del("123123".into())).unwrap();
        logger.write_cmd(&Command::del("123123".into())).unwrap();
        logger.write_cmd(&Command::del("123123".into())).unwrap();
        logger.write_cmd(&Command::del("123123".into())).unwrap();
        logger.write_cmd(&Command::del("123123".into())).unwrap();
        logger.write_cmd(&Command::del("123123".into())).unwrap();
    }

    #[test]
    fn test_splitted_file_reader() {
        use super::{Command, SplittedFileLogReader};
        let reader = SplittedFileLogReader::open("./data").unwrap();

        let mut cnt = 0;
        for c in reader {
            assert_eq!(c.unwrap(), Command::del("123123".into()));
            cnt += 1;
        }

        assert_eq!(cnt, 16);
    }
}
