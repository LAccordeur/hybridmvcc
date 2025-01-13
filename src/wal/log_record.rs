use crate::{
    memtable::MemTableLogRecord,
    transaction::{Timestamp, TransactionLogRecord},
    wal::{wal_log::WalLogRecord, LogPointer},
    Db, Result,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum LogRecord<'a, K> {
    #[serde(borrow)]
    MemTable(MemTableLogRecord<'a>),
    Transaction(TransactionLogRecord),
    Wal(WalLogRecord<K>),
}

impl<'a, K> LogRecord<'a, K> {
    pub fn apply<V>(self, db: &Db<K, V>, xid: Timestamp, lsn: LogPointer) -> Result<()> {
        match self {
            LogRecord::MemTable(memtable_log) => memtable_log.apply(db, xid, lsn),
            LogRecord::Transaction(txn_log) => txn_log.apply(db, xid, lsn),
            LogRecord::Wal(wal_log) => wal_log.apply(db, xid, lsn),
        }
    }

    pub fn create_transaction_record(txn_log_record: TransactionLogRecord) -> LogRecord<'a, K> {
        LogRecord::Transaction(txn_log_record)
    }

    pub fn create_memtable_record(memtable_log_record: MemTableLogRecord<'a>) -> LogRecord<'a, K> {
        LogRecord::MemTable(memtable_log_record)
    }

    pub fn create_wal_record(wal_log_record: WalLogRecord<K>) -> LogRecord<'a, K> {
        LogRecord::Wal(wal_log_record)
    }
}
