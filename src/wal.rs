mod checkpoint;
mod log_record;
mod reader;
mod segment;
mod wal_log;

pub use self::{
    checkpoint::{CheckpointManager, DbState},
    log_record::LogRecord,
    wal_log::{CheckpointLog, WalLogRecord},
};

use self::{reader::WalReader, segment::Segment};

use crate::{
    memdebug::{MemContext, MemContextGuard},
    transaction::Timestamp,
    Error, Result,
};

use std::{
    fs::{self, DirBuilder, File},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Mutex, RwLock},
};

use fs2::FileExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub type LogPointer = u64;

#[allow(dead_code)]
pub fn is_invalid_lsn(lsn: LogPointer) -> bool {
    lsn == 0
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WalSyncMethod {
    OpenSyncData,
    OpenSyncAll,
    SyncData,
    SyncAll,
    NoSync,
}

#[derive(Clone)]
pub struct WalOptions {
    segment_capacity: usize,
    wal_sync_method: WalSyncMethod,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            segment_capacity: 16 * 1024 * 1024,
            wal_sync_method: WalSyncMethod::SyncData,
        }
    }
}

macro_rules! setters {
    ($($name:ident : $t:ty),*) => {
        $(
            pub fn $name(mut self, to: $t) -> Self {
                self.$name = to;
                self
            }
        )*
    }
}

impl WalOptions {
    pub fn new() -> Self {
        Self::default()
    }

    setters!(segment_capacity: usize, wal_sync_method: WalSyncMethod);
}

#[derive(Serialize, Deserialize, Debug)]
struct FullLogRecord<'a, K> {
    xid: Timestamp,
    #[serde(borrow)]
    payload: LogRecord<'a, K>,
}

pub struct Wal {
    #[allow(dead_code)]
    dir: File,

    path: PathBuf,
    capacity: usize,
    segment_creator: Mutex<SegmentCreator>,
    open_segment: RwLock<Segment>,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(path: P, options: &WalOptions) -> Result<Self> {
        let _mguard = MemContextGuard::new(MemContext::Wal);

        if !path.as_ref().exists() {
            DirBuilder::new().recursive(true).create(&path)?;
        } else if !path.as_ref().is_dir() {
            return Err(Error::WrongObjectType(format!(
                "'{}' exists but is not a directory",
                path.as_ref().display()
            )));
        }

        let dir = File::open(&path)?;
        dir.try_lock_exclusive()?;

        let mut last_segno: u32 = 0;
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;

            if !metadata.is_file() {
                return Err(Error::WrongObjectType(format!(
                    "unexpected segment in wal directory: {:?}",
                    entry.path()
                )));
            }

            let filename = entry.file_name().into_string().map_err(|_| {
                Error::WrongObjectType(format!(
                    "unexpected segment in wal directory: {:?}",
                    entry.path()
                ))
            })?;

            let segno = filename_to_segno(&filename)?;

            if segno > last_segno {
                last_segno = segno;
            }
        }

        let mut segment_creator = SegmentCreator::new(
            &path,
            options.segment_capacity,
            last_segno,
            options.wal_sync_method,
        );
        let segment = if last_segno == 0 {
            segment_creator.next_segment()
        } else {
            segment_creator.open_segment(last_segno)
        }?;

        Ok(Wal {
            dir,
            path: path.as_ref().to_path_buf(),
            capacity: options.segment_capacity,
            segment_creator: Mutex::new(segment_creator),
            open_segment: RwLock::new(segment),
        })
    }

    pub fn get_full_record<K>(&self, xid: Timestamp, record: LogRecord<K>) -> Vec<u8>
    where
        K: Serialize,
    {
        let _mguard = MemContextGuard::new(MemContext::Wal);

        let full_record = FullLogRecord {
            xid,
            payload: record,
        };
        bincode::serialize(&full_record).unwrap()
    }

    pub fn append<T>(&self, record: &T) -> Result<(LogPointer, LogPointer)>
    where
        T: Deref<Target = [u8]>,
    {
        let _mguard = MemContextGuard::new(MemContext::Wal);
        let mut guard = self.open_segment.write().unwrap();

        if !guard.sufficient_capacity(record.len()) {
            if guard.dirty() {
                guard.flush_page(true)?;
            }

            let mut creator = self.segment_creator.lock().unwrap();
            *guard = creator.next_segment()?;
        }

        let start_pos = guard.current_lsn();
        match guard.append(record)? {
            Some(end_pos) => Ok((start_pos, end_pos)),
            _ => unreachable!(),
        }
    }

    pub fn append_all<T>(&self, records: &[T]) -> Result<()>
    where
        T: Deref<Target = [u8]>,
    {
        let _mguard = MemContextGuard::new(MemContext::Wal);
        let mut guard = self.open_segment.write().unwrap();

        for record in records {
            if !guard.sufficient_capacity(record.len()) {
                if guard.dirty() {
                    guard.flush_page(true)?;
                }

                let mut creator = self.segment_creator.lock().unwrap();
                *guard = creator.next_segment()?;
            }

            guard.append(record)?;
        }

        Ok(())
    }

    pub fn append_record<K>(
        &self,
        xid: Timestamp,
        record: LogRecord<K>,
    ) -> Result<(LogPointer, LogPointer)>
    where
        K: Serialize,
    {
        let buf = self.get_full_record(xid, record);
        self.append(&buf)
    }

    pub fn flush(&self, lsn: Option<LogPointer>) -> Result<()> {
        let _mguard = MemContextGuard::new(MemContext::Wal);
        let mut guard = self.open_segment.write().unwrap();

        if let Some(lsn) = lsn {
            if guard.flushed_lsn() >= lsn {
                return Ok(());
            }
        }
        guard.flush_page(false)
    }

    pub fn current_lsn(&self) -> LogPointer {
        let guard = self.open_segment.read().unwrap();

        guard.current_lsn()
    }

    pub fn get_reader(&self, start_pos: LogPointer) -> Result<WalReader> {
        WalReader::open(&self.path, self.capacity, start_pos)
    }

    pub fn read_checkpoint_record<K>(
        &self,
        last_checkpoint_pos: LogPointer,
    ) -> Result<Option<CheckpointLog<K>>>
    where
        K: DeserializeOwned,
    {
        if !is_invalid_lsn(last_checkpoint_pos) {
            let reader = self.get_reader(last_checkpoint_pos)?;
            match reader.read_record(last_checkpoint_pos)? {
                None => Err(Error::DataCorrupted(
                    "cannot load the checkpoint log record".to_owned(),
                )),
                Some((_, recbuf)) => match bincode::deserialize::<FullLogRecord<K>>(&recbuf) {
                    Ok(FullLogRecord {
                        payload: LogRecord::Wal(WalLogRecord::Checkpoint(ckpt_log)),
                        ..
                    }) => Ok(Some(ckpt_log)),
                    Ok(_) => Err(Error::DataCorrupted(
                        "last checkpoint pos points to non checkpoint record".to_owned(),
                    )),
                    _ => Err(Error::DataCorrupted(
                        "cannot deserialize the checkpoint log record".to_owned(),
                    )),
                },
            }
        } else {
            Ok(None)
        }
    }
}

fn filename_to_segno(filename: &str) -> Result<u32> {
    u32::from_str_radix(filename, 16).map_err(|_| {
        Error::WrongObjectType(format!(
            "unexpected segment in wal directory: '{}'",
            filename
        ))
    })
}

struct SegmentCreator {
    path: PathBuf,
    last_segno: u32,
    capacity: usize,
    sync_method: WalSyncMethod,
}

impl SegmentCreator {
    fn new<P: AsRef<Path>>(
        path: P,
        capacity: usize,
        last_segno: u32,
        sync_method: WalSyncMethod,
    ) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            last_segno,
            capacity,
            sync_method,
        }
    }

    fn open_segment(&self, segno: u32) -> Result<Segment> {
        Segment::open(
            segno,
            self.segno_to_path(segno),
            self.capacity,
            self.sync_method,
        )
    }

    fn next_segment(&mut self) -> Result<Segment> {
        self.last_segno += 1;
        Segment::create(
            self.last_segno,
            self.segno_to_path(self.last_segno),
            self.capacity,
            self.sync_method,
        )
    }
    fn segno_to_path(&self, segno: u32) -> PathBuf {
        let mut path = self.path.clone();
        path.push(format!("{:08X}", segno));
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_wal() -> (Wal, tempfile::TempDir) {
        let db_dir = tempfile::tempdir().unwrap();
        let options = WalOptions::new();
        let wal = Wal::open(db_dir.path(), &options).unwrap();
        (wal, db_dir)
    }

    #[test]
    fn can_create_wal() {
        let (_, db_dir) = create_wal();

        let mut path = db_dir.path().to_path_buf();
        path.push("00000001");
        assert!(path.is_file());
    }

    #[test]
    fn can_append_wal() {
        let (wal, _db_dir) = create_wal();

        let record: &[u8] = &[42u8; 4096];
        for _ in 0..10 {
            assert!(wal.append(&record).is_ok());
        }
    }

    #[test]
    fn can_read_wal() {
        let (wal, _db_dir) = create_wal();

        let record: &[u8] = &[42u8; 100];
        for _ in 0..10 {
            assert!(wal.append(&record).is_ok());
        }

        wal.flush(None).unwrap();

        let reader = wal.get_reader(0).unwrap();
        let mut count = 0;
        for rec in reader.iter() {
            let (_, recbuf) = rec.unwrap();
            count += 1;
            assert_eq!(record, &recbuf[..]);
        }

        assert_eq!(count, 10);
    }
}
