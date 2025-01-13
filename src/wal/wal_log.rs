use crate::{
    table::TableFileSet,
    transaction::Timestamp,
    wal::{LogPointer, LogRecord},
    Db, Result,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckpointLog<K> {
    pub min_xip: Timestamp,
    pub min_recovery_point: LogPointer,
    pub next_file_num: usize,
    pub file_set: TableFileSet<K>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum WalLogRecord<K> {
    Checkpoint(CheckpointLog<K>),
}

impl<K> WalLogRecord<K> {
    pub fn apply<V>(self, _db: &Db<K, V>, _xid: Timestamp, _lsn: LogPointer) -> Result<()> {
        Ok(())
    }

    pub fn create_checkpoint_log<'a>(
        min_xip: Timestamp,
        min_recovery_point: LogPointer,
        next_file_num: usize,
        file_set: TableFileSet<K>,
    ) -> LogRecord<'a, K> {
        let checkpoint_record = CheckpointLog {
            min_xip,
            min_recovery_point,
            next_file_num,
            file_set,
        };
        LogRecord::create_wal_record(WalLogRecord::Checkpoint(checkpoint_record))
    }
}
