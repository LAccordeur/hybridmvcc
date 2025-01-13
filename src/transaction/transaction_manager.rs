use crate::{
    memdebug::{MemContext, MemContextGuard},
    transaction::{Timestamp, TimestampType, Transaction, TransactionLogRecord, TransactionMode},
    Db, Result,
};

use std::{
    collections::BTreeSet,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    },
};

cfg_if::cfg_if! {
    if #[cfg(feature = "hybrid_ts")] {
        use crate::transaction::{Snapshot, transaction_table::{TransactionTable, TransactionStatus}};
        use std::{collections::HashSet, sync::Mutex};
    }
}

use serde::Serialize;

struct SnapshotData {
    /// Set of in-progress txns
    xips: BTreeSet<Timestamp>,
    latest_completed_xid: Timestamp,
}

impl Default for SnapshotData {
    fn default() -> Self {
        Self {
            xips: BTreeSet::new(),
            latest_completed_xid: Timestamp::default(),
        }
    }
}

pub struct TransactionManager<K, V> {
    timestamp_counter: AtomicU64,
    snapshot_data: RwLock<SnapshotData>,
    cached_min_xip: AtomicU64,
    #[cfg(feature = "hybrid_ts")]
    txn_table: Mutex<TransactionTable>,
    kv_type: PhantomData<(K, V)>,
}

impl<K, V> TransactionManager<K, V>
where
    K: Sync + Send + Serialize,
    V: Sync + Send,
{
    pub fn new() -> Self {
        let _mguard = MemContextGuard::new(MemContext::TransactionManager);

        Self {
            timestamp_counter: AtomicU64::new(1),
            snapshot_data: RwLock::new(SnapshotData::default()),
            cached_min_xip: AtomicU64::new(Timestamp::default().into()),
            #[cfg(feature = "hybrid_ts")]
            txn_table: Mutex::new(TransactionTable::new()),
            kv_type: PhantomData,
        }
    }

    pub fn start_transaction(&self, mode: TransactionMode) -> Result<Transaction<K, V>> {
        let _mguard = MemContextGuard::new(MemContext::TransactionManager);

        let ts = {
            // make sure that no txn with a commit timestamp lower than this start timestamp is committing
            let mut guard = self.snapshot_data.write().unwrap();
            let ts = self.timestamp_counter.fetch_add(1, Ordering::Relaxed);
            guard.xips.insert(Timestamp::new(TimestampType::Commit, ts));
            ts
        };

        let commit_ts = match mode {
            #[cfg(feature = "hybrid_ts")]
            TransactionMode::FastCommit => Timestamp::new(TimestampType::Start, ts),
            _ => Timestamp::new(TimestampType::Uncommitted, ts),
        };

        let txn = Transaction::new(mode, Timestamp::new(TimestampType::Commit, ts), commit_ts);

        Ok(txn)
    }

    pub fn commit(&self, db: &Db<K, V>, mut txn: Transaction<K, V>) -> Result<()> {
        let _mguard = MemContextGuard::new(MemContext::TransactionManager);

        let start_ts = txn.start_timestamp();
        let is_readonly = txn.is_readonly();
        let wal = db.get_wal();

        let commit_ts = {
            let mut guard = self.snapshot_data.write().unwrap();

            let commit_ts = Timestamp::new(
                TimestampType::Commit,
                self.timestamp_counter.fetch_add(1, Ordering::Relaxed),
            );

            if !is_readonly {
                // mark the version deltas with the final commit timestamp
                let redo_nodes = std::mem::replace(&mut txn.redo_nodes, Vec::new());

                for (_, _, redo) in redo_nodes {
                    redo.with_redo(|redo| unsafe {
                        redo.deref().store_timestamp(commit_ts, Ordering::Release)
                    });
                }
            }

            guard.xips.remove(&start_ts);
            if guard.latest_completed_xid < start_ts {
                guard.latest_completed_xid = start_ts;
            }

            commit_ts
        };

        if !is_readonly {
            txn.commit_log_records(wal)?;

            let min_xip = self.cached_min_xip(Ordering::Acquire).unwrap_or_default();

            // write txn commit log
            let txn_commit_log =
                TransactionLogRecord::create_transaction_commit_log::<K>(commit_ts, min_xip);
            let (_, lsn) = wal.append_record(start_ts, txn_commit_log)?;

            // flush the log
            wal.flush(Some(lsn))?;
        }

        #[cfg(feature = "hybrid_ts")]
        if txn.is_fast_commit() {
            let mut guard = self.txn_table.lock().unwrap();
            guard.set_transaction_status(start_ts, TransactionStatus::Committed)?;
        }

        Ok(())
    }

    pub fn abort(&self, db: &Db<K, V>, mut txn: Transaction<K, V>) -> Result<()> {
        let _mguard = MemContextGuard::new(MemContext::TransactionManager);

        let start_ts = txn.start_timestamp();
        let redo_nodes = txn.redo_nodes.drain(..).collect::<Vec<_>>();

        {
            let mut guard = self.snapshot_data.write().unwrap();
            guard.xips.remove(&start_ts);
            if guard.latest_completed_xid < start_ts {
                guard.latest_completed_xid = start_ts;
            }
        }

        for (table, key, redo) in redo_nodes {
            redo.with_redo(|redo| table.rollback(&txn, &key, unsafe { redo.deref() }))?;
        }

        // write txn abort log
        let wal = db.get_wal();
        let txn_abort_log = TransactionLogRecord::create_transaction_abort_log::<K>();
        let (_, lsn) = wal.append_record(start_ts, txn_abort_log)?;

        // flush the log
        wal.flush(Some(lsn))?;

        #[cfg(feature = "hybrid_ts")]
        if txn.is_fast_commit() {
            let mut guard = self.txn_table.lock().unwrap();
            guard.set_transaction_status(start_ts, TransactionStatus::Aborted)?;
        }

        Ok(())
    }

    pub fn min_xip(&self) -> Option<Timestamp> {
        let _mguard = MemContextGuard::new(MemContext::TransactionManager);

        let guard = self.snapshot_data.read().unwrap();
        let min_txn = guard.xips.iter().next().map(Clone::clone);

        if let Some(min_ts) = min_txn {
            self.cached_min_xip.store(min_ts.into(), Ordering::Release);
        }

        min_txn
    }

    pub fn cached_min_xip(&self, order: Ordering) -> Option<Timestamp> {
        let _mguard = MemContextGuard::new(MemContext::TransactionManager);

        let min_xip = Timestamp::from(self.cached_min_xip.load(order));

        if min_xip.is_invalid() {
            None
        } else {
            Some(min_xip)
        }
    }

    #[cfg(feature = "hybrid_ts")]
    fn record_snapshot(&self, txn: &Transaction<K, V>) -> Result<Snapshot> {
        let guard = self.snapshot_data.read().unwrap();

        let max_xid = guard.latest_completed_xid.inc();
        let mut min_xid = max_xid;
        let mut xips = HashSet::new();

        for xid in guard.xips.iter().copied() {
            if xid.is_invalid() {
                panic!("invalid XID in active transaction list");
            }

            if xid >= max_xid {
                continue;
            }

            if xid < min_xid {
                min_xid = xid;
            }

            if xid == txn.start_timestamp() {
                continue;
            }

            xips.insert(xid);
        }

        let snapshot = Snapshot {
            min_xid,
            max_xid,
            xips,
        };
        Ok(snapshot)
    }

    #[cfg(feature = "hybrid_ts")]
    pub fn get_snapshot<'a>(&self, txn: &'a mut Transaction<K, V>) -> Result<&'a Snapshot> {
        let snapshot = txn.current_snapshot.take();
        match snapshot {
            None => {
                // first call
                let snapshot = self.record_snapshot(txn)?;
                txn.current_snapshot = Some(snapshot);
            }
            Some(snapshot) => {
                txn.current_snapshot = Some(snapshot);
            }
        };

        match &txn.current_snapshot {
            Some(snapshot) => Ok(snapshot),
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "hybrid_ts")]
    pub fn get_transaction_status(&self, xid: Timestamp) -> Result<TransactionStatus> {
        let mut guard = self.txn_table.lock().unwrap();

        guard.get_transaction_status(xid)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "hybrid_ts")]
    use crate::test_util::get_temp_db;

    #[cfg(feature = "hybrid_ts")]
    #[test]
    fn can_get_snapshot() {
        let (db, _db_dir) = get_temp_db::<i32, i32>().unwrap();

        let mut txn1 = db.start_transaction().unwrap();
        let txn1_xid = txn1.start_timestamp();
        let snapshot = db
            .get_transaction_manager()
            .get_snapshot(&mut txn1)
            .unwrap();
        assert_eq!(snapshot.min_xid, txn1_xid);
        assert_eq!(snapshot.max_xid, txn1_xid);
        assert!(snapshot.xips.is_empty());

        db.commit_transaction(txn1).unwrap();

        let txn2 = db.start_transaction().unwrap();
        let txn3 = db.start_transaction().unwrap();
        let txn3_xid = txn3.start_timestamp();
        let txn4 = db.start_transaction().unwrap();
        let txn5 = db.start_transaction().unwrap();
        let txn5_xid = txn3.start_timestamp();
        let mut txn6 = db.start_transaction().unwrap();

        db.commit_transaction(txn3).unwrap();
        db.commit_transaction(txn5).unwrap();

        let snapshot = db
            .get_transaction_manager()
            .get_snapshot(&mut txn6)
            .unwrap();
        assert_eq!(snapshot.xips.len(), 2);
        assert!(snapshot.xips.contains(&txn2.start_timestamp()));
        assert!(snapshot.xips.contains(&txn4.start_timestamp()));
        assert!(snapshot.is_xid_in_progress(txn2.start_timestamp()));
        assert!(!snapshot.is_xid_in_progress(txn3_xid));
        assert!(snapshot.is_xid_in_progress(txn4.start_timestamp()));
        assert!(!snapshot.is_xid_in_progress(txn5_xid));

        db.commit_transaction(txn2).unwrap();
        db.commit_transaction(txn4).unwrap();
        db.commit_transaction(txn6).unwrap();
    }
}
