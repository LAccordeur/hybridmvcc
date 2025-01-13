use crate::{
    transaction::Timestamp,
    wal::{LogPointer, LogRecord},
    Db, Result,
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TxnCommitLog {
    pub(super) commit_ts: Timestamp,
    pub(super) min_xip: Timestamp,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TransactionLogRecord {
    Commit(TxnCommitLog),
    Abort,
}

impl TransactionLogRecord {
    pub fn apply<K, V>(self, _db: &Db<K, V>, _xid: Timestamp, _lsn: LogPointer) -> Result<()> {
        Ok(())
    }

    pub fn create_transaction_commit_log<'a, K>(
        commit_ts: Timestamp,
        min_xip: Timestamp,
    ) -> LogRecord<'a, K> {
        let txn_commit_record = TxnCommitLog { commit_ts, min_xip };
        LogRecord::create_transaction_record(TransactionLogRecord::Commit(txn_commit_record))
    }

    pub fn create_transaction_abort_log<'a, K>() -> LogRecord<'a, K> {
        LogRecord::create_transaction_record(TransactionLogRecord::Abort)
    }
}
