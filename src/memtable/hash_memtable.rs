use crate::{
    memdebug::{MemContext, MemContextGuard},
    storage::{RedoNode, RedoRecord},
    transaction::{GuardedRedoNode, Timestamp, TimestampType, Transaction},
    Db, Error, Result,
};

use super::{ImmutableMemTable, MemTable, MemTableIterator, MemTableLogRecord};

use bitflags::bitflags;
use crossbeam::epoch::{self, Atomic, Guard, Shared};
use lockfree::map::{Map as LFMap, Preview};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{event, instrument, Level};

use std::{
    collections::{btree_map, BTreeMap},
    hash::Hash,
    sync::{
        atomic::{AtomicUsize, Ordering},
        RwLock,
    },
};

cfg_if::cfg_if! {
    if #[cfg(feature = "hybrid_ts")] {
        use crate::transaction::TransactionStatus;
    }
}

enum Either<L, R> {
    Left(L),
    Right(R),
}

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct PointerTag: usize {
        const Present = 0b00000001;
        const Abort = 0b00000010;
    }
}

pub struct ImmutableHashTable<K, V>(BTreeMap<K, Option<V>>)
where
    K: Ord;

impl<K, V> Default for ImmutableHashTable<K, V>
where
    K: Ord,
{
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}

impl<K, V> ImmutableHashTable<K, V>
where
    K: Ord,
{
    pub fn new() -> Self {
        Default::default()
    }

    pub fn insert(&mut self, key: K, value: Option<V>) {
        self.0.insert(key, value);
    }
}

impl<K, V> ImmutableMemTable<K, V> for ImmutableHashTable<K, V>
where
    K: Ord + Sync + Send,
    V: Sync + Send,
{
    fn size(&self) -> usize {
        self.0.len()
    }

    fn get<'a>(&'a self, key: &K) -> Result<Option<&'a V>> {
        Ok(match self.0.get(key) {
            None => None,
            Some(v) => v.as_ref(),
        })
    }

    fn iter<'a>(&'a self) -> Box<dyn MemTableIterator<'a, K, V> + 'a> {
        Box::new(ImmutableHashTableIterator(self.0.iter()))
    }
}

pub struct ImmutableHashTableIterator<'a, K, V>(btree_map::Iter<'a, K, Option<V>>);

impl<'a, K, V> MemTableIterator<'a, K, V> for ImmutableHashTableIterator<'a, K, V> {
    fn next(&mut self) -> Option<(&'a K, Option<&'a V>)> {
        match self.0.next() {
            Some((key, Some(value))) => Some((key, Some(value))),
            Some((key, _)) => Some((key, None)),
            _ => None,
        }
    }
}

pub struct HashMemTable<K, V> {
    table: RwLock<LFMap<K, Atomic<RedoNode<V>>>>,
    size: AtomicUsize,
}

impl<K, V> HashMemTable<K, V>
where
    K: Sync + Send + Serialize,
    V: 'static + Sync + Send,
{
    pub fn new() -> Self {
        Default::default()
    }

    #[cfg(feature = "hybrid_ts")]
    fn check_version_visibility(
        &self,
        db: &Db<K, V>,
        txn: &mut Transaction<K, V>,
        version_ts: Timestamp,
    ) -> Result<bool> {
        let current_xid = txn.start_timestamp();
        let txn_mgr = db.get_transaction_manager();
        let snapshot = txn_mgr.get_snapshot(txn)?;
        let version_xid = version_ts.normalize();

        if version_xid == current_xid {
            Ok(true)
        } else if snapshot.is_xid_in_progress(version_xid) {
            Ok(false)
        } else {
            let status = txn_mgr.get_transaction_status(version_xid)?;

            Ok(status == TransactionStatus::Committed)
        }
    }

    fn get_value<'g>(
        &self,
        _db: &Db<K, V>,
        txn: &mut Transaction<K, V>,
        version_ptr: &Atomic<RedoNode<V>>,
        guard: &'g Guard,
    ) -> Result<Option<&'g V>> {
        let mut delta_ptr = version_ptr.load(Ordering::Acquire, &guard);

        if Self::is_abort(delta_ptr) {
            return Ok(None);
        }

        while !delta_ptr.is_null() {
            let delta = unsafe { delta_ptr.deref() };
            let delta_ts = delta.load_timestamp(Ordering::Acquire);

            let skip_delta = match delta_ts.type_() {
                TimestampType::Commit => delta_ts > txn.start_timestamp(),
                TimestampType::Uncommitted => delta_ts != txn.commit_timestamp(Ordering::Relaxed),
                #[cfg(feature = "hybrid_ts")]
                TimestampType::Start | TimestampType::StartCommitted => {
                    !self.check_version_visibility(_db, txn, delta_ts)?
                }
            };

            if skip_delta {
                delta_ptr = delta.next.load(Ordering::Acquire, &guard);
                continue;
            }

            match &delta.record {
                RedoRecord::Insert(v) => {
                    return Ok(Some(v));
                }
                RedoRecord::Delete => {
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    #[inline(always)]
    fn is_present(delta_ptr: Shared<RedoNode<V>>) -> bool {
        !(PointerTag::from_bits_truncate(delta_ptr.tag()) & PointerTag::Present).is_empty()
    }

    #[inline(always)]
    fn is_abort(delta_ptr: Shared<RedoNode<V>>) -> bool {
        !(PointerTag::from_bits_truncate(delta_ptr.tag()) & PointerTag::Abort).is_empty()
    }

    #[inline(always)]
    fn check_ww_conflict(
        _db: &Db<K, V>,
        txn: &mut Transaction<K, V>,
        version_ts: Timestamp,
    ) -> Result<bool> {
        let commit_ts = txn.commit_timestamp(Ordering::Relaxed);
        let start_ts = txn.start_timestamp();

        match version_ts.type_() {
            #[cfg(feature = "hybrid_ts")]
            TimestampType::Start | TimestampType::StartCommitted => {
                let snapshot = _db.get_transaction_manager().get_snapshot(txn)?;
                let version_xid = version_ts.normalize();

                Ok(snapshot.is_xid_in_progress(version_xid) && version_xid != start_ts)
            }
            _ => {
                // Case 1: Version timestamp is invalid (abort redo)
                // Case 2: Tuple is owned by another in-progress transaction
                // Case 3: Tuple is updated by a transaction that commits after this transaction starts
                Ok(version_ts.is_invalid()
                    || (!version_ts.is_committed() && commit_ts != version_ts)
                    || (version_ts.is_committed() && version_ts > start_ts))
            }
        }
    }

    #[inline(always)]
    fn is_version_safe_for_gc(
        version_ts: Timestamp,
        min_xip: Timestamp,
        _freeze_min_age: usize,
    ) -> bool {
        match version_ts.type_() {
            TimestampType::Commit => version_ts < min_xip,
            TimestampType::Uncommitted => false,
            #[cfg(feature = "hybrid_ts")]
            TimestampType::Start | TimestampType::StartCommitted => {
                let mut safe_limit = Timestamp::new(
                    TimestampType::Commit,
                    min_xip.raw_timestamp() - _freeze_min_age as u64,
                );

                if safe_limit.is_invalid() {
                    safe_limit = safe_limit.inc();
                }

                version_ts.normalize() < safe_limit
            }
        }
    }
}

impl<K, V> HashMemTable<K, V> {
    fn clear(&self) {
        let mut table = self.table.write().unwrap();
        let guard = &epoch::pin();

        for entry in &*table {
            let version_ptr = entry.val();
            let mut delta_ptr = version_ptr.load(Ordering::Relaxed, guard);

            while !delta_ptr.is_null() {
                delta_ptr = unsafe {
                    let next = delta_ptr.deref().next.load(Ordering::Relaxed, guard);
                    guard.defer_destroy(delta_ptr);
                    next
                };
            }
        }

        guard.flush();
        table.clear();
        table.optimize_space();
    }
}

impl<K, V> Default for HashMemTable<K, V> {
    fn default() -> Self {
        Self {
            table: RwLock::new(LFMap::default()),
            size: AtomicUsize::new(0),
        }
    }
}

impl<K, V> Drop for HashMemTable<K, V> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<K, V> MemTable<K, V> for HashMemTable<K, V>
where
    K: 'static + Hash + Ord + Clone + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
    V: 'static + Clone + Serialize + DeserializeOwned + Sync + Send,
{
    fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    fn get<'g>(
        &self,
        db: &Db<K, V>,
        txn: &mut Transaction<K, V>,
        key: &K,
        guard: &'g Guard,
    ) -> Result<Option<&'g V>> {
        let _mguard = MemContextGuard::new(MemContext::MemTable);
        let table = self.table.read().unwrap();

        let entry = match table.get(&key) {
            Some(entry) => entry,
            _ => return Ok(None),
        };

        self.get_value(db, txn, entry.val(), guard)
    }

    fn put<'t>(
        &'t self,
        db: &Db<K, V>,
        txn: &mut Transaction<'t, K, V>,
        key: K,
        value: V,
    ) -> Result<bool> {
        let _mguard = MemContextGuard::new(MemContext::MemTable);
        let table = self.table.read().unwrap();

        // look for the version pointer for this key
        let entry = match table.get(&key) {
            Some(entry) => entry,
            _ => {
                // if there is no such key then insert a new entry
                table.insert_with(key.clone(), |_, _, stored| {
                    if stored.is_some() {
                        Preview::Keep
                    } else {
                        self.size.fetch_add(1, Ordering::SeqCst);
                        Preview::New(Atomic::null())
                    }
                });

                match table.get(&key) {
                    Some(entry) => entry,
                    _ => unreachable!(),
                }
            }
        };

        let version_ptr = entry.val();
        let guard = Box::new(epoch::pin());
        let value_buf = bincode::serialize(&value).unwrap();
        let mut redo = txn
            .get_insert_redo(value)
            .with_tag(PointerTag::Present.bits());

        let guarded_redo = loop {
            let delta_ptr = version_ptr.load(Ordering::Acquire, &guard);

            if !delta_ptr.is_null() {
                let version_ts = unsafe { delta_ptr.deref().load_timestamp(Ordering::Acquire) };

                if Self::check_ww_conflict(db, txn, version_ts)? {
                    // return Ok(false);
                    return Err(Error::TransactionAborted(
                        "ww-conflict detected in put()".to_owned(),
                    ));
                }
            }

            redo.next.store(delta_ptr, Ordering::Relaxed);

            match version_ptr.compare_exchange(
                delta_ptr,
                redo,
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(redo_ptr) => {
                    let redo_raw_ptr = redo_ptr.as_raw();

                    let guarded_redo = GuardedRedoNode::from_raw(guard, redo_raw_ptr);
                    break guarded_redo;
                }
                Err(e) => redo = e.new,
            }
        };

        let key_buf = bincode::serialize(&key).unwrap();
        let put_log = MemTableLogRecord::create_put_log(&key_buf, &value_buf);
        txn.stage_log_record(db.get_wal(), put_log)?;

        txn.add_redo_node(self, key.clone(), guarded_redo);

        Ok(true)
    }

    fn update<'t>(
        &'t self,
        db: &Db<K, V>,
        txn: &mut Transaction<'t, K, V>,
        key: &K,
        f: &dyn Fn(&V) -> V,
    ) -> Result<bool> {
        let _mguard = MemContextGuard::new(MemContext::MemTable);
        let table = self.table.read().unwrap();

        let entry = match table.get(&key) {
            Some(entry) => entry,
            _ => return Ok(false),
        };

        let version_ptr = entry.val();
        let guard = Box::new(epoch::pin());

        let (guarded_redo, value_buf) = loop {
            let delta_ptr = version_ptr.load(Ordering::Acquire, &guard);

            if !delta_ptr.is_null() {
                let version_ts = unsafe { delta_ptr.deref().load_timestamp(Ordering::Acquire) };

                if Self::check_ww_conflict(db, txn, version_ts)? {
                    // return Ok(false);
                    return Err(Error::TransactionAborted(
                        "ww-conflict detected in update()".to_owned(),
                    ));
                }
            }

            if !Self::is_present(delta_ptr) {
                // value deleted or does not exist
                return Ok(false);
            }

            let new_value = match self.get_value(db, txn, version_ptr, &guard)? {
                Some(value) => f(value),
                _ => return Ok(false),
            };

            let value_buf = bincode::serialize(&new_value).unwrap();
            let redo = txn
                .get_insert_redo(new_value)
                .with_tag(PointerTag::Present.bits());
            redo.next.store(delta_ptr, Ordering::Relaxed);

            match version_ptr.compare_exchange(
                delta_ptr,
                redo,
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(redo_ptr) => {
                    let redo_raw_ptr = redo_ptr.as_raw();

                    let guarded_redo = GuardedRedoNode::from_raw(guard, redo_raw_ptr);
                    break (guarded_redo, value_buf);
                }
                _ => continue,
            }
        };

        let key_buf = bincode::serialize(key).unwrap();
        let put_log = MemTableLogRecord::create_put_log(&key_buf, &value_buf);
        txn.stage_log_record(db.get_wal(), put_log)?;

        txn.add_redo_node(self, key.clone(), guarded_redo);

        Ok(true)
    }

    fn delete<'t>(
        &'t self,
        db: &Db<K, V>,
        txn: &mut Transaction<'t, K, V>,
        key: &K,
    ) -> Result<bool> {
        let _mguard = MemContextGuard::new(MemContext::MemTable);
        let table = self.table.read().unwrap();

        let entry = match table.get(&key) {
            Some(entry) => entry,
            _ => return Ok(false),
        };

        let version_ptr = entry.val();
        let guard = Box::new(epoch::pin());
        let mut redo = txn.get_delete_redo().with_tag(0);

        let guarded_redo = loop {
            let delta_ptr = version_ptr.load(Ordering::Acquire, &guard);

            if !delta_ptr.is_null() {
                let version_ts = unsafe { delta_ptr.deref().load_timestamp(Ordering::Acquire) };

                if Self::check_ww_conflict(db, txn, version_ts)? {
                    // return Ok(false);
                    return Err(Error::TransactionAborted(
                        "ww-conflict detected in delete()".to_owned(),
                    ));
                }
            }

            if !Self::is_present(delta_ptr) {
                return Ok(false);
            }

            redo.next.store(delta_ptr, Ordering::Relaxed);

            match version_ptr.compare_exchange(
                delta_ptr,
                redo,
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(redo_ptr) => {
                    let redo_raw_ptr = redo_ptr.as_raw();

                    let guarded_redo = GuardedRedoNode::from_raw(guard, redo_raw_ptr);
                    break guarded_redo;
                }
                Err(e) => redo = e.new,
            }
        };

        let key_buf = bincode::serialize(&key).unwrap();
        let delete_log = MemTableLogRecord::create_delete_log(&key_buf);
        txn.stage_log_record(db.get_wal(), delete_log)?;

        txn.add_redo_node(self, key.clone(), guarded_redo);

        Ok(true)
    }

    fn rollback(&self, txn: &Transaction<K, V>, key: &K, _redo: &RedoNode<V>) -> Result<()> {
        let table = self.table.read().unwrap();
        let entry = match table.get(&key) {
            Some(entry) => entry,
            _ => return Ok(()),
        };

        let version_ptr = entry.val();
        let guard = &epoch::pin();
        let mut delta_ptr = version_ptr.load(Ordering::Acquire, guard);

        // chase the version chain to rollback the logs
        while !delta_ptr.is_null()
            && unsafe { delta_ptr.deref().load_timestamp(Ordering::Acquire) }
                == txn.commit_timestamp(Ordering::Relaxed)
        {
            let delta = unsafe { delta_ptr.deref() };

            let next = delta.next.load(Ordering::Acquire, guard);
            version_ptr.store(next, Ordering::Release);

            unsafe {
                guard.defer_destroy(delta_ptr);
            }

            delta_ptr = next;
        }

        Ok(())
    }

    fn snapshot(
        &self,
        min_xip: Timestamp,
        freeze_min_age: usize,
    ) -> Result<Box<dyn ImmutableMemTable<K, V>>> {
        let _mguard = MemContextGuard::new(MemContext::MemTableSnapshot);
        let table = self.table.read().unwrap();

        let mut imm_table = Box::new(ImmutableHashTable::new());
        let guard = &epoch::pin();

        for entry in &*table {
            let version_ptr = entry.val();
            let mut delta_ptr = version_ptr.load(Ordering::Acquire, guard);

            // for each entry, chase the version chain to find the first version that is older
            // than min_xip
            let value = loop {
                // whenever we see an abort record on the chain, we should give up right away
                if delta_ptr.is_null() || Self::is_abort(delta_ptr) {
                    break Either::Right(());
                }

                let delta = unsafe { delta_ptr.deref() };
                let delta_ts = delta.load_timestamp(Ordering::Acquire);

                if Self::is_version_safe_for_gc(delta_ts, min_xip, freeze_min_age) {
                    if !delta.try_set_collected(min_xip) {
                        // already collected
                        break Either::Right(());
                    }

                    // found
                    let val: Either<Option<V>, ()> = match &delta.record {
                        RedoRecord::Insert(v) => Either::Left(Some(v.clone())),
                        RedoRecord::Delete => Either::Left(None),
                    };

                    // let next = delta.next.load(Ordering::Relaxed, guard);

                    // // detach and schedule to reclaim the following nodes as they
                    // // are no longer needed
                    // delta.next.store(Shared::null(), Ordering::Release);

                    // delta_ptr = next;
                    // while !delta_ptr.is_null() {
                    //     delta_ptr = unsafe {
                    //         let next = delta_ptr.deref().next.load(Ordering::Relaxed, guard);
                    //         guard.defer_destroy(delta_ptr);
                    //         next
                    //     };
                    // }

                    break val;
                }

                let next = delta.next.load(Ordering::Relaxed, guard);
                delta_ptr = next;
            };

            match value {
                Either::Right(()) => {
                    // the chain is still hot or deleted, skip it
                    continue;
                }
                Either::Left(v) => imm_table.insert(entry.key().clone(), v),
            }
        }

        Ok(imm_table)
    }

    #[instrument(skip(self))]
    fn gc(&self, min_xip: Timestamp, freeze_min_age: usize) -> Result<()> {
        let _mguard = MemContextGuard::new(MemContext::MemTableGc);
        let table = self.table.read().unwrap();

        let guard = &epoch::pin();

        let mut removed_entries = 0usize;
        let mut skipped_collected = 0usize;
        let mut skipped_hot_entries = 0usize;
        let mut skipped_hot_entries_cas = 0usize;
        let mut skipped_new_entries = 0usize;

        for entry in &*table {
            let version_ptr = entry.val();
            let old_head = version_ptr.load(Ordering::Acquire, guard);
            let mut delta_ptr = old_head.clone();

            // whenever we see an abort record on the chain, we should give up right away
            if Self::is_abort(delta_ptr) {
                continue;
            }

            // check if the head record is older than min_xip
            if !delta_ptr.is_null() {
                let delta = unsafe { delta_ptr.deref() };

                let version_ts = delta.load_timestamp(Ordering::Relaxed);
                let collected = delta.load_collected(Ordering::Relaxed);

                // we can delete the versions that have been collected anyway
                while !delta_ptr.is_null() {
                    let delta = unsafe { delta_ptr.deref() };

                    let mut next_ptr = delta.next.load(Ordering::Relaxed, guard);

                    if !next_ptr.is_null() {
                        let next = unsafe { next_ptr.deref() };
                        let next_ts = next.load_timestamp(Ordering::Acquire);

                        if Self::is_version_safe_for_gc(next_ts, min_xip, freeze_min_age) {
                            if next.load_collected(Ordering::Relaxed) != min_xip {
                                break;
                            }

                            // this is the one
                            // detach and schedule to reclaim the following nodes as they
                            // are no longer needed
                            delta.next.store(Shared::null(), Ordering::Release);

                            while !next_ptr.is_null() {
                                next_ptr = unsafe {
                                    let next = next_ptr.deref().next.load(Ordering::Relaxed, guard);
                                    guard.defer_destroy(next_ptr);
                                    next
                                };
                            }

                            break;
                        }
                    }

                    delta_ptr = next_ptr;
                }

                // this pair has been updated after min_xip, skip it
                if !Self::is_version_safe_for_gc(version_ts, min_xip, freeze_min_age) {
                    skipped_hot_entries += 1;
                    continue;
                }

                // we should not clean this entry if it is not collected by us
                if collected != min_xip {
                    skipped_collected += 1;
                    continue;
                }
            } else {
                // if delta is null, then this is a new entry created by an in-progress txn
                // so just skip it
                skipped_new_entries += 1;
                continue;
            }

            // try to publish an abort record to block subsequent accesses
            let abort_ptr = old_head.with_tag(
                (PointerTag::from_bits_truncate(old_head.tag()) | PointerTag::Abort).bits(),
            );

            if version_ptr
                .compare_exchange(
                    old_head,
                    abort_ptr,
                    Ordering::Release,
                    Ordering::Relaxed,
                    &guard,
                )
                .is_err()
            {
                // other txns have modified the version chain, skip it
                skipped_hot_entries_cas += 1;
                continue;
            }

            removed_entries += 1;

            delta_ptr = version_ptr.load(Ordering::Relaxed, guard);

            // now accesses to the current version chain are blocked, we can remove
            // the entry from the hash table and reclaim the nodes
            table.remove(entry.key());
            self.size.fetch_sub(1, Ordering::SeqCst);

            while !delta_ptr.is_null() {
                delta_ptr = unsafe {
                    let next = delta_ptr.deref().next.load(Ordering::Relaxed, guard);
                    guard.defer_destroy(delta_ptr);
                    next
                };
            }
        }

        event!(
            Level::INFO,
            "hash memtable garbage collection finished, removed {} entries, skipped {} hot entries ({} updated, {} CAS failures), {} collected entries and {} new entries",
            removed_entries, skipped_hot_entries + skipped_hot_entries_cas, skipped_hot_entries, skipped_hot_entries_cas, skipped_collected, skipped_new_entries
        );

        Ok(())
    }

    fn optimize_space(&self) {
        let mut table = self.table.write().unwrap();
        table.optimize_space();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::get_temp_db;
    use std::sync::{Arc, Barrier};

    #[test]
    fn can_insert() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());
        let barrier = Arc::new(Barrier::new(3));

        let mut handles = Vec::new();
        for i in 0..3 {
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            handles.push(std::thread::spawn(move || {
                let mut txn = db.start_transaction().unwrap();
                let guard = &epoch::pin();
                barrier.wait();

                for j in 0..30 {
                    assert!(table.put(&db, &mut txn, i * 30 + j, j).is_ok());
                }

                for j in 0..30 {
                    // a txn can read its own updates
                    let val = table.get(&db, &mut txn, &(i * 30 + j), guard).unwrap();
                    assert_eq!(val, Some(&j));
                }

                barrier.wait();

                let mut count = 0;
                for i in 0..90 {
                    let val = table.get(&db, &mut txn, &i, guard).unwrap();

                    if val.is_some() {
                        count += 1;
                    }
                }
                // a txn cannot read uncommitted updates by other txns
                assert_eq!(count, 30);

                db.commit_transaction(txn).unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut txn = db.start_transaction().unwrap();
        let guard = &epoch::pin();

        for i in 0..90 {
            let val = table.get(&db, &mut txn, &i, guard).unwrap();
            assert_eq!(val, Some(&(i % 30)));
        }

        db.commit_transaction(txn).unwrap();
    }

    #[test]
    fn can_update() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());

        let mut txn = db.start_transaction().unwrap();
        assert!(table.put(&db, &mut txn, 1, 2).unwrap());
        assert!(table.update(&db, &mut txn, &1, &|x| x + 1).unwrap());

        let guard = &epoch::pin();
        let val = table.get(&db, &mut txn, &1, guard).unwrap();
        assert_eq!(val, Some(&3));

        db.commit_transaction(txn).unwrap();
    }

    #[test]
    fn can_detect_write_write_conflict() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());
        let barrier = Arc::new(Barrier::new(2));

        let mut handles = Vec::new();

        let mut txn = db.start_transaction().unwrap();
        assert!(table.put(&db, &mut txn, 1, 2).unwrap());
        db.commit_transaction(txn).unwrap();

        handles.push({
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut txn = db.start_transaction().unwrap();
                barrier.wait();

                assert!(table.put(&db, &mut txn, 1, 3).unwrap());
                db.commit_transaction(txn).unwrap();

                barrier.wait();
            })
        });

        handles.push({
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut txn = db.start_transaction().unwrap();
                barrier.wait();
                barrier.wait();

                // this update must fail because the tuple is already updated by a committed transaction
                assert!(table.put(&db, &mut txn, 1, 4).is_err());

                db.abort_transaction(txn).unwrap();
            })
        });

        for handle in handles {
            handle.join().unwrap();
        }

        let mut txn = db.start_transaction().unwrap();
        let guard = &epoch::pin();
        let val = table.get(&db, &mut txn, &1, guard).unwrap();

        assert_eq!(val, Some(&3));

        db.commit_transaction(txn).unwrap();
    }

    #[test]
    fn can_delete() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());
        let barrier = Arc::new(Barrier::new(2));

        let mut handles = Vec::new();

        let mut txn = db.start_transaction().unwrap();
        assert!(table.put(&db, &mut txn, 1, 2).unwrap());
        db.commit_transaction(txn).unwrap();

        handles.push({
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut txn = db.start_transaction().unwrap();
                barrier.wait();

                assert!(table.delete(&db, &mut txn, &1).unwrap());
                barrier.wait();

                db.commit_transaction(txn).unwrap();
                barrier.wait();
            })
        });

        handles.push({
            let table = table.clone();
            let db = db.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut txn = db.start_transaction().unwrap();
                let guard = &epoch::pin();

                barrier.wait();
                barrier.wait();

                {
                    let val = table.get(&db, &mut txn, &1, guard).unwrap();

                    assert_eq!(val, Some(&2));
                }

                barrier.wait();

                {
                    // repeatable read
                    let val = table.get(&db, &mut txn, &1, guard).unwrap();

                    assert!(val.is_some());
                    assert_eq!(val, Some(&2));
                }

                db.commit_transaction(txn).unwrap();
            })
        });

        for handle in handles {
            handle.join().unwrap();
        }

        let mut txn = db.start_transaction().unwrap();
        let guard = &epoch::pin();
        let val = table.get(&db, &mut txn, &1, guard).unwrap();

        assert!(val.is_none());

        db.commit_transaction(txn).unwrap();
    }

    #[test]
    fn can_abort_transaction() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());
        let barrier = Arc::new(Barrier::new(2));

        let mut handles = Vec::new();

        let mut txn = db.start_transaction().unwrap();
        assert!(table.put(&db, &mut txn, 1, 2).unwrap());
        db.commit_transaction(txn).unwrap();

        handles.push({
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut txn = db.start_transaction().unwrap();
                barrier.wait();

                assert!(table.put(&db, &mut txn, 1, 3).unwrap());
                db.abort_transaction(txn).unwrap();

                barrier.wait();
            })
        });

        handles.push({
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut txn = db.start_transaction().unwrap();
                barrier.wait();
                barrier.wait();

                assert!(table.put(&db, &mut txn, 1, 4).unwrap());

                db.commit_transaction(txn).unwrap();
            })
        });

        for handle in handles {
            handle.join().unwrap();
        }

        let mut txn = db.start_transaction().unwrap();
        let guard = &epoch::pin();

        let val = table.get(&db, &mut txn, &1, guard).unwrap();

        assert_eq!(val, Some(&4));

        db.commit_transaction(txn).unwrap();
    }

    #[test]
    fn can_gc() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());
        {
            let mut txn = db.start_transaction().unwrap();
            assert!(table.put(&db, &mut txn, 1, 2).unwrap());
            db.commit_transaction(txn).unwrap();
        }

        {
            let mut txn = db.start_transaction().unwrap();
            assert!(table.put(&db, &mut txn, 1, 3).unwrap());
            db.commit_transaction(txn).unwrap();
        }

        {
            let mut txn = db.start_transaction().unwrap();
            assert!(table.put(&db, &mut txn, 1, 4).unwrap());
            db.commit_transaction(txn).unwrap();
        }

        {
            let mut txn = db.start_transaction().unwrap();
            let guard = &epoch::pin();

            let val = table.get(&db, &mut txn, &1, guard).unwrap();

            assert_eq!(val, Some(&4));

            db.commit_transaction(txn).unwrap();
        }
        {
            let txn = db.start_transaction().unwrap();

            table.snapshot(Timestamp::from(9), 1000000).unwrap();
            table.gc(Timestamp::from(9), 1000000).unwrap();

            db.commit_transaction(txn).unwrap();
        }
    }

    #[cfg(feature = "hybrid_ts")]
    #[test]
    fn can_insert_fast_commit() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());
        let barrier = Arc::new(Barrier::new(3));

        let mut handles = Vec::new();
        for i in 0..3 {
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            handles.push(std::thread::spawn(move || {
                let mut txn = db.start_transaction_fast_commit().unwrap();
                let guard = &epoch::pin();
                barrier.wait();

                for j in 0..30 {
                    assert!(table.put(&db, &mut txn, i * 30 + j, j).is_ok());
                }

                for j in 0..30 {
                    // a txn can read its own updates
                    let val = table.get(&db, &mut txn, &(i * 30 + j), guard).unwrap();
                    assert_eq!(val, Some(&j));
                }

                barrier.wait();

                let mut count = 0;
                for i in 0..90 {
                    let val = table.get(&db, &mut txn, &i, guard).unwrap();

                    if val.is_some() {
                        count += 1;
                    }
                }
                // a txn cannot read uncommitted updates by other txns
                assert_eq!(count, 30);

                db.commit_transaction(txn).unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut txn = db.start_transaction().unwrap();
        let guard = &epoch::pin();

        for i in 0..90 {
            let val = table.get(&db, &mut txn, &i, guard).unwrap();
            assert_eq!(val, Some(&(i % 30)));
        }

        db.commit_transaction(txn).unwrap();
    }

    #[cfg(feature = "hybrid_ts")]
    #[test]
    fn can_update_fast_commit() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());

        {
            let mut txn = db.start_transaction_fast_commit().unwrap();
            assert!(table.put(&db, &mut txn, 1, 2).unwrap());
            assert!(table.update(&db, &mut txn, &1, &|x| x + 1).unwrap());

            let guard = &epoch::pin();
            let val = table.get(&db, &mut txn, &1, guard).unwrap();
            assert_eq!(val, Some(&3));

            db.commit_transaction(txn).unwrap();
        }

        {
            let mut txn = db.start_transaction().unwrap();
            assert!(table.update(&db, &mut txn, &1, &|x| x + 1).unwrap());

            let guard = &epoch::pin();
            let val = table.get(&db, &mut txn, &1, guard).unwrap();
            assert_eq!(val, Some(&4));

            db.commit_transaction(txn).unwrap();
        }
    }

    #[cfg(feature = "hybrid_ts")]
    #[test]
    fn can_detect_write_write_conflict_fast_commit() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let table = Arc::new(HashMemTable::new());
        let barrier = Arc::new(Barrier::new(2));

        let mut handles = Vec::new();

        let mut txn = db.start_transaction().unwrap();
        assert!(table.put(&db, &mut txn, 1, 2).unwrap());
        db.commit_transaction(txn).unwrap();

        handles.push({
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut txn = db.start_transaction_fast_commit().unwrap();
                barrier.wait();

                assert!(table.put(&db, &mut txn, 1, 3).unwrap());
                barrier.wait();

                barrier.wait();
                db.commit_transaction(txn).unwrap();
            })
        });

        handles.push({
            let db = db.clone();
            let table = table.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut txn = db.start_transaction_fast_commit().unwrap();
                barrier.wait();
                barrier.wait();

                // this update must fail because the tuple is already updated by a uncommitted transaction
                assert!(table.put(&db, &mut txn, 1, 4).is_err());

                barrier.wait();
                db.abort_transaction(txn).unwrap();
            })
        });

        for handle in handles {
            handle.join().unwrap();
        }

        let mut txn = db.start_transaction().unwrap();
        let guard = &epoch::pin();
        let val = table.get(&db, &mut txn, &1, guard).unwrap();

        assert_eq!(val, Some(&3));

        db.commit_transaction(txn).unwrap();
    }

    #[test]
    fn can_check_safe_version() {
        let min_xip = Timestamp::new(TimestampType::Commit, 100);

        assert!(HashMemTable::<u32, u32>::is_version_safe_for_gc(
            Timestamp::new(TimestampType::Commit, 99),
            min_xip,
            50
        ));

        assert!(!HashMemTable::<u32, u32>::is_version_safe_for_gc(
            Timestamp::new(TimestampType::Commit, 101),
            min_xip,
            50
        ));

        #[cfg(feature = "hybrid_ts")]
        assert!(HashMemTable::<u32, u32>::is_version_safe_for_gc(
            Timestamp::new(TimestampType::Start, 49),
            min_xip,
            50
        ));

        #[cfg(feature = "hybrid_ts")]
        assert!(!HashMemTable::<u32, u32>::is_version_safe_for_gc(
            Timestamp::new(TimestampType::Start, 51),
            min_xip,
            50
        ));

        #[cfg(feature = "hybrid_ts")]
        assert!(!HashMemTable::<u32, u32>::is_version_safe_for_gc(
            Timestamp::new(TimestampType::Start, 1),
            min_xip,
            150
        ));

        #[cfg(feature = "hybrid_ts")]
        assert!(!HashMemTable::<u32, u32>::is_version_safe_for_gc(
            Timestamp::new(TimestampType::Start, -50i64 as u64),
            min_xip,
            150
        ));

        #[cfg(feature = "hybrid_ts")]
        assert!(HashMemTable::<u32, u32>::is_version_safe_for_gc(
            Timestamp::new(TimestampType::Start, -51i64 as u64),
            min_xip,
            150
        ));
    }
}
