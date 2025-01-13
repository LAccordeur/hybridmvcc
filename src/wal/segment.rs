use std::{
    fs::{File, OpenOptions},
    io::{prelude::*, SeekFrom},
    ops::Deref,
    path::Path,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use memmap::Mmap;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use super::{LogPointer, WalSyncMethod};
use crate::{Error, Result};

const SEGMENT_PAGE_SIZE: usize = 0x10000;
const CHECKSUM_SIZE: usize = 4;
const RECORD_HEADER_SIZE: usize = 3 + CHECKSUM_SIZE; /* 1-bit type + 2-bit length + checksum */

#[derive(Clone, Copy, Debug)]
enum RecordHeaderType {
    None = 0,
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
    Error = 5,
}

impl From<u8> for RecordHeaderType {
    fn from(value: u8) -> Self {
        match value {
            0 => RecordHeaderType::None,
            1 => RecordHeaderType::Full,
            2 => RecordHeaderType::First,
            3 => RecordHeaderType::Middle,
            4 => RecordHeaderType::Last,
            _ => RecordHeaderType::Error,
        }
    }
}

pub struct Segment {
    segno: u32,
    file: File,
    sync_method: WalSyncMethod,
    page: [u8; SEGMENT_PAGE_SIZE],
    page_allocated: usize,
    page_flushed: usize,
    page_start: usize,
    capacity: usize,
}

fn check_capacity(capacity: usize) -> Result<()> {
    if capacity > RECORD_HEADER_SIZE {
        Ok(())
    } else {
        Err(Error::InvalidArgument(
            "segment capacity is smalle than record header size".to_owned(),
        ))
    }
}

macro_rules! def_sync_bit {
    ($method:ident, $options:ident, $($name:path => $bit:expr),* $(,)?) => {
        match $method {
            $($name => {
                if cfg!(unix) {
                    $options.custom_flags($bit);
                } else {
                    return Err(Error::InvalidArgument(format!(
                        "{} is not supported on non-unix platforms", stringify!($name)),
                    ));
                }
            })*
            _ => {}
        }
    }
}

fn set_sync_bit(sync_method: WalSyncMethod, options: &mut OpenOptions) -> Result<()> {
    def_sync_bit!(
        sync_method,
        options,
        WalSyncMethod::OpenSyncData => libc::O_DSYNC,
        WalSyncMethod::OpenSyncAll => libc::O_SYNC,
    );

    Ok(())
}

impl Segment {
    pub fn create<P: AsRef<Path>>(
        segno: u32,
        path: P,
        capacity: usize,
        sync_method: WalSyncMethod,
    ) -> Result<Self> {
        check_capacity(capacity)?;

        let mut options = OpenOptions::new();
        options.read(false).write(true).create(true);
        set_sync_bit(sync_method, &mut options)?;

        let file = options.open(&path)?;

        let segment = Segment {
            segno,
            file,
            sync_method,
            page: [0u8; SEGMENT_PAGE_SIZE],
            page_allocated: 0,
            page_flushed: 0,
            page_start: 0,
            capacity,
        };

        Ok(segment)
    }

    pub fn open<P: AsRef<Path>>(
        segno: u32,
        path: P,
        capacity: usize,
        sync_method: WalSyncMethod,
    ) -> Result<Self> {
        check_capacity(capacity)?;

        let mut options = OpenOptions::new();
        options.read(false).write(true).create(false);
        set_sync_bit(sync_method, &mut options)?;

        let mut file = options.open(&path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        if file_size > capacity {
            return Err(Error::InvalidArgument(format!(
                "invalid segment capacity: file size = {}, capacity = {}",
                file_size, capacity
            )));
        }

        let mut page_start = file_size;

        file.seek(SeekFrom::End(0))?;
        if file_size % SEGMENT_PAGE_SIZE != 0 {
            let padding = SEGMENT_PAGE_SIZE - (file_size % SEGMENT_PAGE_SIZE);
            let zero_bytes = vec![0u8; padding];
            file.write_all(&zero_bytes[..])?;

            page_start += padding;
        }

        let page = [0u8; SEGMENT_PAGE_SIZE];

        let segment = Segment {
            segno,
            file,
            sync_method,
            page,
            page_allocated: 0,
            page_flushed: 0,
            page_start,
            capacity,
        };

        Ok(segment)
    }

    pub fn append<T>(&mut self, record: &T) -> Result<Option<LogPointer>>
    where
        T: Deref<Target = [u8]>,
    {
        let mut length = record.len();
        let mut offset = 0;

        if !self.sufficient_capacity(record.len()) {
            return Ok(None);
        }

        let mut record_type = RecordHeaderType::None;

        while length > 0 {
            if SEGMENT_PAGE_SIZE - self.page_allocated <= RECORD_HEADER_SIZE {
                self.flush_page(true)?;
            }

            let chunk_size = std::cmp::min(
                length,
                SEGMENT_PAGE_SIZE - self.page_allocated - RECORD_HEADER_SIZE,
            );
            let last_chunk = chunk_size == length;

            record_type = match record_type {
                RecordHeaderType::None => {
                    if last_chunk {
                        RecordHeaderType::Full
                    } else {
                        RecordHeaderType::First
                    }
                }
                RecordHeaderType::First | RecordHeaderType::Middle => {
                    if last_chunk {
                        RecordHeaderType::Last
                    } else {
                        RecordHeaderType::Middle
                    }
                }
                _ => RecordHeaderType::None,
            };

            let chunk = &record[offset..offset + chunk_size];
            let record_start = self.page_allocated;
            // record type
            self.page[self.page_allocated] = record_type as u8;
            self.page_allocated += 1;
            // chunk length
            (&mut self.page[self.page_allocated..]).write_u16::<LittleEndian>(chunk_size as u16)?;
            self.page_allocated += 2;
            // chunk data
            self.page[self.page_allocated..self.page_allocated + chunk_size].copy_from_slice(chunk);
            self.page_allocated += chunk_size;

            // checksum
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&self.page[record_start..self.page_allocated]);
            let checksum = hasher.finalize();

            (&mut self.page[self.page_allocated..]).write_u32::<LittleEndian>(checksum as u32)?;
            self.page_allocated += CHECKSUM_SIZE;

            length -= chunk_size;
            offset += chunk_size;
        }

        Ok(Some(self.current_lsn()))
    }

    pub fn flush_page(&mut self, reset: bool) -> Result<()> {
        let reset = reset || self.page_allocated + RECORD_HEADER_SIZE >= SEGMENT_PAGE_SIZE;

        if reset {
            self.page_allocated = SEGMENT_PAGE_SIZE;
        }

        self.file.seek(SeekFrom::End(0))?;
        self.file
            .write_all(&self.page[self.page_flushed..self.page_allocated])?;

        match self.sync_method {
            WalSyncMethod::SyncData => {
                self.file.sync_data()?;
            }
            WalSyncMethod::SyncAll => {
                self.file.sync_all()?;
            }
            _ => {}
        }

        self.page_flushed = self.page_allocated;

        if reset {
            for i in self.page.iter_mut() {
                *i = 0;
            }

            self.page_allocated = 0;
            self.page_flushed = 0;
            self.page_start += SEGMENT_PAGE_SIZE;
        }

        Ok(())
    }

    pub fn segment_start(&self) -> LogPointer {
        ((self.segno as usize - 1) * self.capacity) as LogPointer
    }

    pub fn current_lsn(&self) -> LogPointer {
        self.segment_start() + (self.page_start + self.page_allocated) as LogPointer
    }

    pub fn flushed_lsn(&self) -> LogPointer {
        self.segment_start() + (self.page_start + self.page_flushed) as LogPointer
    }

    pub fn sufficient_capacity(&self, record_size: usize) -> bool {
        assert!(self.capacity >= self.page_start);

        if self.capacity == self.page_start {
            return false;
        }

        let remaining_pages = (self.capacity - self.page_start) / SEGMENT_PAGE_SIZE;
        let mut remaining = SEGMENT_PAGE_SIZE - self.page_allocated;

        if remaining_pages > 0 {
            remaining += (SEGMENT_PAGE_SIZE - RECORD_HEADER_SIZE) * (remaining_pages - 1);
        }

        remaining >= record_size
    }

    pub fn dirty(&self) -> bool {
        self.page_allocated != self.page_flushed
    }
}

pub struct SegmentView {
    mmap: Option<Mmap>,
}

impl SegmentView {
    pub fn open<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self> {
        check_capacity(capacity)?;

        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;

        if file_size > capacity {
            return Err(Error::InvalidArgument(format!(
                "invalid segment capacity: file size = {}, capacity = {}",
                file_size, capacity
            )));
        }

        let mmap = if file_size == 0 {
            None
        } else {
            Some(unsafe { Mmap::map(&file)? })
        };
        let segment = Self { mmap };

        Ok(segment)
    }

    pub fn read_record(&self, offset: usize) -> Result<Option<(Vec<u8>, usize)>> {
        match &self.mmap {
            None => Ok(None),
            Some(mmap) => {
                if mmap.len() <= offset + RECORD_HEADER_SIZE {
                    return Ok(None);
                }

                let mut p = offset;
                let mut buffer = Vec::new();
                let mut started = false;
                loop {
                    if mmap.len() <= p + RECORD_HEADER_SIZE {
                        return Err(Error::DataCorrupted(
                            "unexpected EOF when reading WAL segment".to_owned(),
                        ));
                    }

                    let rec_start = p;
                    let rec_type = RecordHeaderType::from(unsafe { *mmap.get_unchecked(p) });
                    p += 1;

                    match rec_type {
                        RecordHeaderType::None => {
                            // go to the next page
                            p += SEGMENT_PAGE_SIZE - (p % SEGMENT_PAGE_SIZE);

                            if mmap.len() <= p + RECORD_HEADER_SIZE && !started {
                                // no more data
                                return Ok(None);
                            } else {
                                continue;
                            }
                        }
                        RecordHeaderType::Error => {
                            return Err(Error::DataCorrupted(
                                "invalid record type in segment".to_owned(),
                            ))
                        }
                        _ => {}
                    }

                    started = true;

                    let chunk_length = (unsafe { mmap.get_unchecked(p..p + 2) })
                        .read_u16::<LittleEndian>()
                        .unwrap() as usize;
                    p += 2;

                    match mmap.get(rec_start..p + chunk_length + CHECKSUM_SIZE) {
                        Some(chunk) => {
                            let (chunk, checksum_buf) = chunk.split_at(chunk.len() - CHECKSUM_SIZE);
                            let checksum_file =
                                (&checksum_buf[..]).read_u32::<LittleEndian>().unwrap();

                            let mut hasher = crc32fast::Hasher::new();
                            hasher.update(chunk);
                            let checksum = hasher.finalize();

                            if checksum != checksum_file {
                                return Err(Error::DataCorrupted(
                                    "record is corrupted (checksum does not match)".to_owned(),
                                ));
                            }

                            buffer.extend_from_slice(&chunk[p - rec_start..]);
                        }
                        _ => {
                            return Err(Error::DataCorrupted("segment is truncated".to_owned()));
                        }
                    }

                    p += chunk_length + CHECKSUM_SIZE;

                    match rec_type {
                        RecordHeaderType::Full | RecordHeaderType::Last => break,
                        _ => {}
                    }
                }

                Ok(Some((buffer, p - offset)))
            }
        }
    }
}
