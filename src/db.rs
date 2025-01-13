use crate::{
    memdebug::{MemContext, MemContextGuard},
    memtable::{HashMemTable, ImmutableMemTable, MemTable},
    table::{TableCache, TableFileMeta, TableFileRef},
    transaction::{Timestamp, Transaction, TransactionManager, TransactionMode},
    version::{Compaction, VersionEdit, VersionSet},
    wal::{CheckpointManager, DbState, LogPointer, Wal},
    DbOptions, Error, Result,
};

use std::{
    collections::{btree_map, BTreeMap, BTreeSet},
    fs::{DirBuilder, File, OpenOptions},
    hash::Hash,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Condvar, Mutex, RwLock,
    },
    thread, time,
};

use crossbeam::epoch;
use fs2::FileExt;
use futures::{
    executor::{ThreadPool, ThreadPoolBuilder},
    task::SpawnExt,
    Future,
};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{event, instrument, span, Level};

type CollectSet = BTreeSet<Timestamp>;
type ImmutableTableSet<K, V> = BTreeMap<Timestamp, Arc<dyn ImmutableMemTable<K, V>>>;

struct DbInner<K, V> {
    options: DbOptions<K, V>,
    root_dir: Option<File>,

    txnmgr: TransactionManager<K, V>,
    wal: Wal,
    ckptmgr: Mutex<CheckpointManager>,
    thread_pool: ThreadPool,
    mem_table: Box<dyn MemTable<K, V>>,
    imm_tables: RwLock<(CollectSet, ImmutableTableSet<K, V>)>,
    vset: RwLock<VersionSet<K, V>>,

    last_flush_size: AtomicUsize,
    flushing_threads: AtomicUsize,
    collect_event: (Condvar, Mutex<()>),
    flush_event: (Condvar, Mutex<()>),

    running_compactions: AtomicUsize,
}

fn ensure_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    if !path.as_ref().exists() {
        DirBuilder::new().recursive(true).create(&path)?;
    } else if !path.as_ref().is_dir() {
        return Err(Error::WrongObjectType(format!(
            "'{}' exists but is not a directory",
            path.as_ref().display()
        )));
    }

    Ok(())
}

impl<K, V> DbInner<K, V> {
    fn init_dirs(&mut self) -> Result<()> {
        let root_path = self.options.get_root_path();

        ensure_dir(&root_path)?;
        let dir = File::open(root_path)?;
        dir.try_lock_exclusive()?;
        self.root_dir = Some(dir);

        ensure_dir(self.options.get_storage_path())?;
        Ok(())
    }
}

impl<K, V> DbInner<K, V>
where
    K: 'static + Clone + Ord + Sync + Send + Serialize + DeserializeOwned,
    V: 'static + Clone + Sync + Send,
{
    pub fn startup(&self) -> Result<()> {
        let mut guard = self.ckptmgr.lock().unwrap();

        let manifest = guard.read_manifest()?;
        let last_checkpoint_pos = manifest.last_checkpoint_pos();
        let checkpoint_log = self.wal.read_checkpoint_record::<K>(last_checkpoint_pos)?;
        let min_recovery_point = match checkpoint_log {
            Some(checkpoint_log) => {
                let min_recovery_point = checkpoint_log.min_recovery_point;

                {
                    let mut vset_guard = self.vset.write().unwrap();
                    vset_guard.recover(checkpoint_log);
                }

                min_recovery_point
            }
            _ => 0,
        };

        let current_lsn = self.wal.current_lsn();
        if current_lsn < min_recovery_point {
            return Err(Error::DataCorrupted(
                "invalid redo point in checkpoint record".to_owned(),
            ));
        }

        let need_recovery =
            current_lsn > min_recovery_point || manifest.db_state() != DbState::Shutdowned;

        if need_recovery {
            guard.set_db_state(DbState::InCrashRecovery)?;

            // self.wal.replay_logs(self, redo_pos)?;
        }

        guard.set_db_state(DbState::InProduction)?;
        Ok(())
    }

    fn should_flush(&self) -> bool {
        let last_flush_size = self.last_flush_size.load(Ordering::Relaxed);
        let write_buffer_full =
            (self.mem_table.size() - last_flush_size) > self.options.write_buffer_size;

        write_buffer_full
    }
}

// newtype
// this is used when we want to get a shared ownership of the db instance, e.g. sending it to
// a background thread.
#[derive(Clone)]
pub struct Db<K, V>(Arc<DbInner<K, V>>);

impl<K, V> Db<K, V>
where
    K: 'static + Clone + Ord + Hash + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
    V: 'static + Clone + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
{
    pub fn open(options: DbOptions<K, V>) -> Result<Self> {
        let _mguard = MemContextGuard::new(MemContext::Database);

        let wal = Wal::open(options.get_wal_path(), &options.wal_options)?;
        let ckptmgr = CheckpointManager::open(options.get_manifest_path())?;
        let table_cache = Arc::new(TableCache::new(options.clone()));
        let mut pool_builder = ThreadPoolBuilder::default();
        pool_builder.pool_size(options.background_threads);

        let thread_pool = pool_builder.create()?;

        let mut inner = DbInner {
            options: options.clone(),
            root_dir: None,

            txnmgr: TransactionManager::new(),
            wal,
            ckptmgr: Mutex::new(ckptmgr),
            thread_pool,
            mem_table: Box::new(HashMemTable::new()),
            imm_tables: RwLock::new((BTreeSet::new(), BTreeMap::new())),
            vset: RwLock::new(VersionSet::new(options, table_cache)),

            last_flush_size: AtomicUsize::new(0),
            flushing_threads: AtomicUsize::new(0),
            collect_event: (Condvar::new(), Mutex::new(())),
            flush_event: (Condvar::new(), Mutex::new(())),

            running_compactions: AtomicUsize::new(0),
        };

        inner.init_dirs()?;
        inner.startup()?;

        Ok(Self(Arc::new(inner)))
    }

    fn get_from_storage<'t>(
        &self,
        txn: &'t mut Transaction<K, V>,
        key: &K,
    ) -> Result<Option<&'t V>> {
        {
            let _mguard = MemContextGuard::new(MemContext::Database);

            // search in imm tables
            let imm_guard = self.0.imm_tables.read().unwrap();
            let (_, imm_set) = &*imm_guard;
            for (min_xip, imt) in imm_set.iter().rev() {
                let iv = imt.get(key)?;

                if iv.is_some() {
                    // extend the lifetime to 't and ref the imm table in the txn
                    let rv = unsafe { std::mem::transmute::<Option<_>, Option<&'t V>>(iv) };
                    txn.ref_imm_table(*min_xip, imt.clone());

                    return Ok(rv);
                }
            }
        }

        {
            // search in sstables
            let vset_guard = self.0.vset.read().unwrap();
            let current = vset_guard.current();

            if let Some(value) = current.get(key)? {
                return Ok(Some(txn.record_value(value)));
            }
        }

        Ok(None)
    }

    pub fn get<'t>(&self, txn: &'t mut Transaction<K, V>, key: &K) -> Result<Option<&'t V>> {
        let _mguard = MemContextGuard::new(MemContext::Database);

        // get a GC guard for reading memtables
        let txn_gc_guard = txn.take_gc_guard();
        let _need_gc_guard = txn_gc_guard.is_some();

        let gc_guard = match txn_gc_guard {
            Some(guard) => guard,
            None => epoch::pin(),
        };

        // search in mem table first
        let mv = self.0.mem_table.get(self, txn, key, &gc_guard)?;
        if mv.is_some() {
            // extend the lifetime to 't and store the GC guard in the txn
            let rv = unsafe { std::mem::transmute::<Option<_>, Option<&'t V>>(mv) };
            txn.set_gc_guard(gc_guard);
            return Ok(rv);
        }

        self.get_from_storage(txn, key)
    }

    pub fn put<'t>(&'t self, txn: &mut Transaction<'t, K, V>, key: K, value: V) -> Result<bool> {
        let _mguard = MemContextGuard::new(MemContext::Database);

        self.make_room_for_write()?;

        self.0.mem_table.put(self, txn, key, value)
    }

    pub fn update<'t>(
        &'t self,
        txn: &mut Transaction<'t, K, V>,
        key: &K,
        f: &dyn Fn(&V) -> V,
    ) -> Result<bool> {
        let _mguard = MemContextGuard::new(MemContext::Database);

        self.make_room_for_write()?;

        if self.0.mem_table.update(self, txn, key, f)? {
            return Ok(true);
        }

        let new_value = match self.get_from_storage(txn, key)? {
            Some(value) => f(value),
            _ => return Ok(false),
        };

        self.put(txn, key.clone(), new_value)
    }

    pub fn delete<'t>(&'t self, txn: &mut Transaction<'t, K, V>, key: &K) -> Result<bool> {
        let _mguard = MemContextGuard::new(MemContext::Database);

        self.0.mem_table.delete(self, txn, key)
    }

    pub fn make_room_for_write(&self) -> Result<()> {
        let _mguard = MemContextGuard::new(MemContext::Database);

        if self.0.should_flush() {
            self.flush_memtable()?;
        }

        Ok(())
    }

    #[instrument(skip(self))]
    pub fn flush_memtable(&self) -> Result<()> {
        let _mguard = MemContextGuard::new(MemContext::Database);

        loop {
            let flush_size = self.0.last_flush_size.load(Ordering::Acquire);

            if !self.0.should_flush() {
                return Ok(());
            }

            if self.0.last_flush_size.compare_exchange(
                flush_size,
                self.0.mem_table.size(),
                Ordering::Release,
                Ordering::Relaxed,
            ) == Ok(flush_size)
            {
                break;
            }
        }

        // flow control
        let old_threads = self.0.flushing_threads.fetch_add(1, Ordering::SeqCst);
        if old_threads >= self.0.options.max_background_flushes {
            self.0.flushing_threads.fetch_sub(1, Ordering::SeqCst);
            return Ok(());
        }

        // now kick the flush!!!
        self.schedule(self.clone().background_flush())
            .map_err(|e| {
                self.0.flushing_threads.fetch_sub(1, Ordering::SeqCst);
                e
            })?;

        event!(Level::INFO, "flushing job scheduled");

        Ok(())
    }

    async fn background_flush(self) {
        let _mguard = MemContextGuard::new(MemContext::BackgroundFlush);

        let span = span!(Level::TRACE, "background_flush");
        let _enter = span.enter();

        // Phase 1: collect
        let min_xip_imt = loop {
            let min_recovery_point = self.0.wal.current_lsn();
            // we can always get a min_xip because there is at least one txn (this txn)
            let min_xip = self.0.txnmgr.min_xip().unwrap_or_else(|| {
                self.exec_transaction(|db, _| Ok(db.0.txnmgr.min_xip().unwrap()))
                    .unwrap()
            });

            let collect_span = span!(Level::TRACE, "collect_phase", min_xip = ?min_xip, min_recovery_point = ?min_recovery_point);
            let _enter = collect_span.enter();

            event!(Level::TRACE, "min_xip recorded for flushing");

            {
                // register this flush in the collector set
                let mut imm_guard = self.0.imm_tables.write().unwrap();
                let (ref mut collect_set, _) = &mut *imm_guard;

                if !collect_set.insert(min_xip) {
                    // collector with this min_xip already exists
                    event!(Level::TRACE, "flushing aborted (duplicate min_xip)");
                    break None;
                }
            }

            // collect values into the imm table
            event!(Level::TRACE, "snapshot begins");
            let imm_table = self
                .0
                .mem_table
                .snapshot(min_xip, self.0.options.gc_freeze_min_age)
                .unwrap();
            event!(
                Level::TRACE,
                "snapshot finished, size = {:?}",
                imm_table.size()
            );

            if imm_table.size() < self.0.options.write_buffer_size / 2 {
                // cancel the flush if we cannot get enough pairs to fill the immutable table
                {
                    let mut imm_guard = self.0.imm_tables.write().unwrap();
                    let (ref mut collect_set, _) = &mut *imm_guard;

                    collect_set.remove(&min_xip);

                    // let other collector threads check again
                    let (event, _) = &self.0.collect_event;
                    event.notify_all();
                }
                event!(Level::TRACE, "flushing aborted (not enough data)");

                break None;
            } else {
                let imt: Arc<dyn ImmutableMemTable<K, V>> = imm_table.into();

                break Some((min_xip, min_recovery_point, imt));
            }
        };

        let (min_xip, min_recovery_point, imt) = match min_xip_imt {
            Some(min_xip_imt) => min_xip_imt,
            _ => {
                self.0.flushing_threads.fetch_sub(1, Ordering::SeqCst);

                return;
            }
        };

        {
            let cleanup_span = span!(Level::TRACE, "cleanup_phase", min_xip = ?min_xip, min_recovery_point = ?min_recovery_point);
            let _enter = cleanup_span.enter();

            // rendezvous point #1
            {
                let mut imm_guard = self.0.imm_tables.write().unwrap();

                loop {
                    let (ref mut collect_set, _) = &mut *imm_guard;
                    let min_collect = collect_set.iter().next().map(|k| k.clone()).unwrap();

                    if min_collect != min_xip {
                        event!(
                            Level::TRACE,
                            "wait on rendezvous point #1, min_collect = {}",
                            min_collect
                        );

                        // need to wait
                        let (event, m) = &self.0.collect_event;
                        let guard = m.lock().unwrap();

                        drop(imm_guard);

                        drop(event.wait(guard).unwrap());

                        // re-aquire the locks
                        imm_guard = self.0.imm_tables.write().unwrap();
                    } else {
                        break;
                    }
                }

                event!(Level::TRACE, "pass rendezvous point #1");

                let (ref mut collect_set, ref mut imm_set) = &mut *imm_guard;
                collect_set.remove(&min_xip);

                match imm_set.entry(min_xip) {
                    btree_map::Entry::Occupied(_) => unreachable!(),
                    btree_map::Entry::Vacant(v) => {
                        v.insert(imt.clone());
                    }
                }

                // let other collector threads check again
                let (event, _) = &self.0.collect_event;
                event.notify_all();
            }

            // Phase 2: clean-up
            event!(Level::TRACE, "memtable garbage collection begins");
            self.0
                .mem_table
                .gc(min_xip, self.0.options.gc_freeze_min_age)
                .unwrap();
            let mt_size = self.0.mem_table.size();
            event!(
                Level::TRACE,
                "memtable garbage collection finished, memtable size = {}",
                mt_size
            );

            self.0.last_flush_size.store(mt_size, Ordering::Relaxed);

            // prepare the version edit and flush the table to disk
            let mut ve = VersionEdit::new();
            ve.set_min_xip(min_xip);

            // write the imm table to an output file
            let meta = self.write_level0_table(&*imt, min_xip).unwrap();
            ve.add_file(0 /* level */, meta);

            // rendezvous point #2
            {
                let mut vset_guard = self.0.vset.write().unwrap();
                let mut imm_guard = self.0.imm_tables.write().unwrap();

                // we need to wait until we are the earliest version in the queue before we can
                // check out
                loop {
                    let (_, ref mut imm_set) = &mut *imm_guard;
                    let min_flush = imm_set.iter().next().map(|(k, _)| k.clone()).unwrap();

                    if min_flush != min_xip {
                        event!(
                            Level::TRACE,
                            "wait on rendezvous point #2, min_flush = {}",
                            min_flush
                        );

                        // need to wait
                        let (event, m) = &self.0.flush_event;
                        let guard = m.lock().unwrap();

                        drop(imm_guard);
                        drop(vset_guard);

                        drop(event.wait(guard).unwrap());

                        // re-aquire the locks
                        vset_guard = self.0.vset.write().unwrap();
                        imm_guard = self.0.imm_tables.write().unwrap();
                    } else {
                        break;
                    }
                }

                event!(Level::TRACE, "pass rendezvous point #2");

                // bump version
                let (_, ref mut imm_set) = &mut *imm_guard;
                vset_guard
                    .log_and_apply(&self, ve, min_recovery_point)
                    .unwrap();
                imm_set.remove(&min_xip);

                // let other flushing threads check again
                let (event, _) = &self.0.flush_event;
                event.notify_all();
            }

            self.schedule_compaction().unwrap();
        }

        event!(Level::TRACE, "flushing finished");

        let old_count = self.0.flushing_threads.fetch_sub(1, Ordering::SeqCst);
        assert!(old_count > 0);
    }

    #[instrument(skip(self, imm))]
    fn write_level0_table(
        &self,
        imm: &dyn ImmutableMemTable<K, V>,
        min_xip: Timestamp,
    ) -> Result<TableFileMeta<K>> {
        assert!(imm.size() > 0);

        let file_num = {
            let vset_guard = self.0.vset.read().unwrap();
            vset_guard.new_file_number()
        };

        let file_ref = TableFileRef::new(0 /* path_id */, file_num /* seqnum */);
        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(self.0.options.get_table_file_path(file_ref))?;

        let mut first_key = None;
        let mut last_key = None;

        {
            // scope for table builder
            let mut builder = self.0.options.table_factory.create_table_builder(&mut file);

            let mut imm_iter = imm.iter();
            while let Some((key, value)) = imm_iter.next() {
                if first_key.is_none() {
                    first_key = Some(key.clone());
                }

                last_key = Some(key);
                match value {
                    Some(value) => {
                        builder.add(key, value)?;
                    }
                    _ => {}
                }
            }

            builder.finish()?;
        }

        let size = file.metadata()?.len();

        let meta = match (first_key, last_key) {
            (Some(first_key), Some(last_key)) => TableFileMeta::new(
                file_ref,
                size as usize,
                min_xip,
                first_key,
                last_key.clone(),
            ),
            _ => unreachable!(),
        };

        event!(
            Level::INFO,
            "level-0 flush table {:?}, {} bytes",
            file_ref,
            size
        );

        Ok(meta)
    }

    #[instrument(skip(self))]
    pub fn schedule_compaction(&self) -> Result<()> {
        // flow control
        let old_threads = self.0.running_compactions.fetch_add(1, Ordering::SeqCst);
        if old_threads >= self.0.options.max_background_compactions {
            self.0.running_compactions.fetch_sub(1, Ordering::SeqCst);
            return Ok(());
        }

        // now kick the compaction!!!
        self.schedule(self.clone().background_compaction())
            .map_err(|e| {
                self.0.running_compactions.fetch_sub(1, Ordering::SeqCst);
                e
            })?;

        event!(Level::INFO, "compaction job scheduled");

        Ok(())
    }

    async fn background_compaction(self) {
        let _mguard = MemContextGuard::new(MemContext::BackgroundCompaction);

        let span = span!(Level::TRACE, "background_compaction");
        let _enter = span.enter();

        let c = {
            let mut vset_guard = self.0.vset.write().unwrap();
            vset_guard.pick_compaction().unwrap()
        };

        if let Some(c) = c {
            self.start_compaction(c).unwrap();
        }

        event!(Level::TRACE, "background compaction finished");

        let old_count = self.0.running_compactions.fetch_sub(1, Ordering::SeqCst);
        assert!(old_count > 0);
    }

    #[instrument(skip(self, c))]
    fn start_compaction(&self, mut c: Compaction<K>) -> Result<()> {
        let output_level = c.output_level();

        event!(
            Level::INFO,
            "start compaction from level {} to level {}",
            c.level(),
            output_level,
        );

        let mut num_outputs = 0;

        let mut citer = {
            let vset_guard = self.0.vset.read().unwrap();
            vset_guard.make_input_iterator(&c)?
        };

        while let Some((k, v)) = citer.next()? {
            let file_num = {
                let vset_guard = self.0.vset.read().unwrap();
                vset_guard.new_file_number()
            };

            let file_ref = TableFileRef::new(0 /* path_id */, file_num /* seqnum */);
            let mut file = OpenOptions::new()
                .read(false)
                .write(true)
                .create(true)
                .open(self.0.options.get_table_file_path(file_ref))?;

            let (first_key, last_key) = {
                let mut builder = self.0.options.table_factory.create_table_builder(&mut file);

                builder.add(&k, &v)?;
                let first_key = k.clone();
                let mut last_key = k;

                while let Some((k, v)) = citer.next()? {
                    builder.add(&k, &v)?;
                    last_key = k;

                    if builder.size_hint()
                        > self.0.options.max_file_size_for_level(c.output_level())
                    {
                        break;
                    }
                }

                builder.finish()?;

                (first_key, last_key)
            };

            let size = file.metadata()?.len();

            let meta = TableFileMeta::new(
                file_ref,
                size as usize,
                Timestamp::default(),
                first_key,
                last_key,
            );

            c.edit().add_file(output_level, meta);
            num_outputs += 1;
        }

        event!(Level::INFO, "created {} output table(s)", num_outputs);

        c.delete_inputs();

        {
            let mut vset_guard = self.0.vset.write().unwrap();
            vset_guard.finalize_compaction(self, c)?;
        }

        self.schedule_compaction()?;

        Ok(())
    }

    pub fn dump_version(&self) {
        let vset_guard = self.0.vset.read().unwrap();

        vset_guard.dump_current();
    }

    pub fn optimize_space(&self) {
        self.0.mem_table.optimize_space();
    }
}

impl<K, V> Db<K, V>
where
    K: Sync + Send + Serialize,
    V: 'static + Sync + Send,
{
    fn schedule<Fut>(&self, f: Fut) -> Result<()>
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.0
            .thread_pool
            .spawn(f)
            .map_err(|_| Error::InvalidState("failed to spawn threads".to_owned()))
    }

    pub fn wait_for_compaction(&self) {
        while self.0.flushing_threads.load(Ordering::SeqCst) > 0
            || self.0.running_compactions.load(Ordering::SeqCst) > 0
        {
            // wait for in-progress flushes and compactions
            thread::sleep(time::Duration::from_millis(100));
        }
    }

    pub fn shutdown(&self) {
        self.wait_for_compaction();
    }

    pub fn get_transaction_manager(&self) -> &TransactionManager<K, V> {
        &self.0.txnmgr
    }

    pub fn get_wal(&self) -> &Wal {
        &self.0.wal
    }

    pub fn start_transaction(&self) -> Result<Transaction<K, V>> {
        self.0.txnmgr.start_transaction(TransactionMode::Normal)
    }

    pub fn start_transaction_fast_commit(&self) -> Result<Transaction<K, V>> {
        self.0.txnmgr.start_transaction(TransactionMode::FastCommit)
    }

    pub fn commit_transaction(&self, txn: Transaction<K, V>) -> Result<()> {
        self.0.txnmgr.commit(self, txn)
    }

    pub fn abort_transaction(&self, txn: Transaction<K, V>) -> Result<()> {
        self.0.txnmgr.abort(self, txn)
    }

    pub fn exec_transaction<'t, F, R>(&'t self, func: F) -> Result<R>
    where
        F: FnOnce(&'t Db<K, V>, &mut Transaction<'t, K, V>) -> Result<R>,
    {
        let mut txn = self.start_transaction()?;
        let res = func(self, &mut txn);

        if res.is_err() {
            self.abort_transaction(txn)?;
        } else {
            self.commit_transaction(txn)?;
        }

        res
    }

    pub fn create_checkpoint(&self, min_xip: Timestamp, checkpoint: LogPointer) -> Result<()> {
        let mut guard = self.0.ckptmgr.lock().unwrap();
        guard.create_checkpoint(min_xip, checkpoint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_util::{get_temp_db, get_temp_db_with_options},
        version::test_util::*,
    };
    use std::sync::{Arc, Barrier};

    #[test]
    fn can_insert() {
        let (db, _db_dir) = get_temp_db().unwrap();
        let barrier = Arc::new(Barrier::new(3));

        let mut handles = Vec::new();
        for i in 0..3 {
            let db = db.clone();
            let barrier = barrier.clone();

            handles.push(std::thread::spawn(move || {
                let mut txn = db.start_transaction().unwrap();
                barrier.wait();

                for j in 0..30 {
                    assert!(db.put(&mut txn, i * 30 + j, j).is_ok());
                }

                for j in 0..30 {
                    // a txn can read its own updates
                    let val = db.get(&mut txn, &(i * 30 + j)).unwrap();
                    assert_eq!(val, Some(&j));
                }

                barrier.wait();

                let mut count = 0;
                for i in 0..90 {
                    let val = db.get(&mut txn, &i).unwrap();

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

        assert!(db
            .exec_transaction(|db, txn| {
                for i in 0..90 {
                    let val = db.get(txn, &i)?;
                    assert!(val.is_some());
                    assert_eq!(val, Some(&(i % 30)));
                }

                Ok(())
            })
            .is_ok());

        db.shutdown();
    }

    #[test]
    fn can_update() {
        let (db, _db_dir) = get_temp_db().unwrap();

        assert!(db
            .exec_transaction(|db, mut txn| {
                assert!(db.put(&mut txn, 1, 2).unwrap());
                assert!(db.update(&mut txn, &1, &|x| x + 1).unwrap());

                let val = db.get(txn, &1).unwrap();
                assert_eq!(val, Some(&3));

                Ok(())
            })
            .is_ok());

        db.shutdown();
    }

    #[test]
    fn can_flush_memtable() {
        let (db, _db_dir) =
            get_temp_db_with_options(DbOptions::new().write_buffer_size(120)).unwrap();

        assert!(db
            .exec_transaction(|db, txn| {
                for i in 0..100 {
                    db.put(txn, i, i + 1)?;
                }

                Ok(())
            })
            .is_ok());

        assert!(db.flush_memtable().is_ok());

        db.wait_for_compaction();

        assert!(db
            .exec_transaction(|db, txn| {
                for i in 0..100 {
                    let val = db.get(txn, &i)?;
                    assert_eq!(val, Some(&(i + 1)));
                }

                Ok(())
            })
            .is_ok());

        db.shutdown();
    }

    #[test]
    fn can_do_l0_compaction() {
        let (mut version, options, _db_dir) = make_version().unwrap();
        let db = Db::open(options).unwrap();

        version.set_compaction_score(vec![2.0, 0.0, 0.0, 0.0, 0.0, 0.0]);
        {
            let mut guard = db.0.vset.write().unwrap();
            guard.set_next_file_number(20);
            guard.set_current(version);
        }

        assert!(db.schedule_compaction().is_ok());

        db.wait_for_compaction();

        db.exec_transaction(|db, txn| {
            let val = db.get(txn, &"aaa".to_owned())?;
            assert_eq!(val, Some(&"val2".to_owned()));

            Ok(())
        })
        .unwrap();

        db.shutdown();
    }

    #[test]
    fn can_do_l1_compaction() {
        let (mut version, options, _db_dir) = make_version().unwrap();
        let db = Db::open(options).unwrap();

        version.set_compaction_score(vec![0.0, 2.0, 0.0, 0.0, 0.0, 0.0]);
        {
            let mut guard = db.0.vset.write().unwrap();
            guard.set_next_file_number(20);
            guard.set_current(version);
        }

        assert!(db.schedule_compaction().is_ok());

        db.wait_for_compaction();

        db.exec_transaction(|db, txn| {
            let val = db.get(txn, &"daa".to_owned())?;
            assert_eq!(val, Some(&"val2".to_owned()));

            Ok(())
        })
        .unwrap();

        db.shutdown();
    }
}
