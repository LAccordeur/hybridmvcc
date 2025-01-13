use crate::{transaction::Timestamp, wal::LogPointer, Error, Result};

use std::{
    fs::{File, OpenOptions},
    io::prelude::*,
    path::{Path, PathBuf},
    time::SystemTime,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use tracing::{event, instrument, Level};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DbState {
    Shutdowned,
    Shutdowning,
    InCrashRecovery,
    InProduction,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Manifest {
    db_state: DbState,
    last_checkpoint_pos: LogPointer,
    prev_checkpoint_pos: LogPointer,
    min_xip: Timestamp,
    time: SystemTime,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            db_state: DbState::Shutdowned,
            last_checkpoint_pos: 0,
            prev_checkpoint_pos: 0,
            min_xip: Timestamp::default(),
            time: SystemTime::now(),
        }
    }
}

impl Manifest {
    pub fn db_state(&self) -> DbState {
        self.db_state
    }
    pub fn last_checkpoint_pos(&self) -> LogPointer {
        self.last_checkpoint_pos
    }
}

struct ManifestFile {
    file_path: PathBuf,
}

impl ManifestFile {
    pub fn new<P: AsRef<Path>>(file_path: P) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
        }
    }

    pub fn read_manifest(&self) -> Result<Option<Manifest>> {
        if !self.file_path.exists() {
            return Ok(None);
        }

        if !self.file_path.is_file() {
            return Err(Error::WrongObjectType(format!(
                "'{}' exists but is not a regular file",
                self.file_path.as_path().display()
            )));
        }

        let mut file = File::open(&self.file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        if buffer.len() < 4 {
            return Err(Error::DataCorrupted(
                "master record is corrupted".to_owned(),
            ));
        }

        let crc_buf = buffer.split_off(buffer.len() - 4);
        let crc_file = (&crc_buf[..]).read_u32::<LittleEndian>().unwrap();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buffer);
        let crc = hasher.finalize();

        if crc != crc_file {
            return Err(Error::DataCorrupted(
                "master record is corrupted (checksum does not match)".to_owned(),
            ));
        }

        let record = match bincode::deserialize::<Manifest>(&buffer) {
            Ok(record) => record,
            _ => {
                return Err(Error::DataCorrupted(
                    "cannot deserialize the master record".to_owned(),
                ));
            }
        };

        Ok(Some(record))
    }

    pub fn write_manifest(&self, record: &Manifest) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(false)
            .open(&self.file_path)?;
        let mut buffer = bincode::serialize(record).unwrap();

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buffer);
        let checksum = hasher.finalize();
        buffer.write_u32::<LittleEndian>(checksum).unwrap();

        file.write_all(&buffer)?;

        Ok(())
    }
}

pub struct CheckpointManager {
    manifest_file: ManifestFile,
    manifest: Manifest,
}

impl CheckpointManager {
    pub fn open<P: AsRef<Path>>(manifest_path: P) -> Result<Self> {
        let manifest_file = ManifestFile::new(manifest_path);
        let mut ckptmgr = Self {
            manifest_file,
            manifest: Default::default(),
        };

        ckptmgr.read_manifest()?;

        Ok(ckptmgr)
    }

    #[instrument(skip(self))]
    pub fn create_checkpoint(&mut self, min_xip: Timestamp, checkpoint: LogPointer) -> Result<()> {
        // update the master record
        let manifest = &mut self.manifest;
        manifest.time = SystemTime::now();
        manifest.min_xip = min_xip;
        manifest.prev_checkpoint_pos = manifest.last_checkpoint_pos;
        manifest.last_checkpoint_pos = checkpoint;
        self.manifest_file.write_manifest(manifest)?;

        event!(Level::INFO, "write manifest {:?}", self.manifest);
        Ok(())
    }

    pub fn read_manifest(&mut self) -> Result<&Manifest> {
        self.manifest = match self.manifest_file.read_manifest()? {
            Some(record) => record,
            _ => {
                // the manifest file is not yet initialized
                let record = Manifest::default();
                self.manifest_file.write_manifest(&record)?;
                record
            }
        };
        Ok(&self.manifest)
    }

    pub fn set_db_state(&mut self, state: DbState) -> Result<()> {
        self.manifest.db_state = state;
        self.manifest.time = SystemTime::now();
        self.manifest_file.write_manifest(&self.manifest)
    }
}
