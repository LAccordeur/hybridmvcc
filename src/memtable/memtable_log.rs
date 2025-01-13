use crate::{
    transaction::Timestamp,
    wal::{LogPointer, LogRecord},
    Db, Result,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct PutLog<'a> {
    #[serde(with = "serde_bytes")]
    key: &'a [u8],
    #[serde(with = "serde_bytes")]
    value: &'a [u8],
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeleteLog<'a> {
    #[serde(with = "serde_bytes")]
    key: &'a [u8],
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MemTableLogRecord<'a> {
    #[serde(borrow)]
    Put(PutLog<'a>),
    Delete(DeleteLog<'a>),
}

impl MemTableLogRecord<'_> {
    pub fn apply<K, V>(self, _db: &Db<K, V>, _xid: Timestamp, _lsn: LogPointer) -> Result<()> {
        Ok(())
    }

    pub fn create_put_log<'a, K>(key: &'a [u8], value: &'a [u8]) -> LogRecord<'a, K> {
        let put_record = PutLog { key, value };
        LogRecord::create_memtable_record(MemTableLogRecord::Put(put_record))
    }

    pub fn create_delete_log<K>(key: &[u8]) -> LogRecord<K> {
        let delete_record = DeleteLog { key };
        LogRecord::create_memtable_record(MemTableLogRecord::Delete(delete_record))
    }
}
