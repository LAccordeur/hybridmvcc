mod transaction_log;
mod transaction_manager;
#[cfg(feature = "hybrid_ts")]
mod transaction_table;

pub use self::{transaction_log::TransactionLogRecord, transaction_manager::TransactionManager};

#[cfg(feature = "hybrid_ts")]
pub use self::transaction_table::TransactionStatus;

use crate::{
    memdebug::{MemContext, MemContextGuard},
    memtable::{ImmutableMemTable, MemTable},
    storage::RedoNode,
    wal::{LogRecord, Wal},
    Result,
};

use std::{
    collections::HashMap,
    fmt,
    num::Wrapping,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[cfg(feature = "hybrid_ts")]
use std::collections::HashSet;

use crossbeam::epoch::{Guard, Owned, Shared};
use serde::{Deserialize, Serialize};

use ouroboros::self_referencing;

cfg_if::cfg_if! {
    if #[cfg(feature = "hybrid_ts")] {

        const TIMESTAMP_TYPE_WIDTH: usize = 2;

        #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
        pub enum TimestampType {
            Commit = 0,
            Start = 1,
            StartCommitted = 2,
            Uncommitted = 3,
        }

        impl From<u8> for TimestampType {
            fn from(value: u8) -> Self {
                match value {
                    0 => TimestampType::Commit,
                    1 => TimestampType::Start,
                    2 => TimestampType::StartCommitted,
                    _ => TimestampType::Uncommitted,
                }
            }
        }
    } else {
        const TIMESTAMP_TYPE_WIDTH: usize = 1;

        #[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
        pub enum TimestampType {
            Commit = 0,
            Uncommitted = 1,
        }

        impl From<u8> for TimestampType {
            fn from(value: u8) -> Self {
                match value {
                    0 => TimestampType::Commit,
                    _ => TimestampType::Uncommitted,
                }
            }
        }
    }
}

const TIMESTAMP_MASK: u64 = (1u64 << (64 - TIMESTAMP_TYPE_WIDTH)) - 1;
const INVALID_TIMESTAMP: u64 = 0;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct Timestamp(u64);

impl Default for Timestamp {
    fn default() -> Self {
        Self(INVALID_TIMESTAMP)
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Timestamp) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Timestamp) -> std::cmp::Ordering {
        if self.is_invalid() || other.is_invalid() {
            match (self.is_invalid(), other.is_invalid()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => unreachable!(),
            }
        } else if self.type_() != other.type_() {
            self.type_().cmp(&other.type_())
        } else {
            let delta = (Wrapping(self.raw_timestamp()) - Wrapping(other.raw_timestamp())).0;

            match delta {
                0 => std::cmp::Ordering::Equal,
                i if (i & (1u64 << (64 - TIMESTAMP_TYPE_WIDTH - 1))) != 0 => {
                    std::cmp::Ordering::Less
                }
                _ => std::cmp::Ordering::Greater,
            }
        }
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_invalid() {
            write!(f, "<Invalid>")
        } else {
            write!(f, "<{:?}:{}>", self.type_(), self.raw_timestamp())
        }
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Into<u64> for Timestamp {
    fn into(self) -> u64 {
        self.0
    }
}

impl Timestamp {
    pub fn new(typ: TimestampType, timestamp: u64) -> Self {
        Self(
            (timestamp & TIMESTAMP_MASK)
                | (((typ as u64) & ((1u64 << TIMESTAMP_TYPE_WIDTH) - 1))
                    << (64 - TIMESTAMP_TYPE_WIDTH)),
        )
    }

    pub fn type_(&self) -> TimestampType {
        TimestampType::from(
            ((self.0 >> (64 - TIMESTAMP_TYPE_WIDTH)) & ((1u64 << TIMESTAMP_TYPE_WIDTH) - 1)) as u8,
        )
    }

    pub fn raw_timestamp(&self) -> u64 {
        self.0 & TIMESTAMP_MASK
    }

    pub fn is_invalid(self) -> bool {
        self.0 == INVALID_TIMESTAMP
    }

    pub fn is_committed(self) -> bool {
        self.type_() != TimestampType::Uncommitted
    }

    pub fn normalize(self) -> Self {
        if self.is_invalid() {
            Default::default()
        } else {
            Self::new(TimestampType::Commit, self.raw_timestamp())
        }
    }

    pub fn inc(self) -> Self {
        let mut xid = Wrapping(self.raw_timestamp());

        loop {
            xid += Wrapping(1);

            if !Timestamp::new(self.type_(), xid.0).is_invalid() {
                break;
            }
        }

        Timestamp::new(self.type_(), xid.0)
    }

    pub fn dec(self) -> Self {
        let mut xid = Wrapping(self.raw_timestamp());

        loop {
            xid -= Wrapping(1);

            if !Timestamp::new(self.type_(), xid.0).is_invalid() {
                break;
            }
        }

        Timestamp::new(self.type_(), xid.0)
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransactionMode {
    Normal = 0,
    FastCommit = 1,
}

/// A shared pointer to an redo node protected by the epoch GC and the pin guard with
/// which it is loaded.
#[self_referencing]
pub struct GuardedRedoNode<V>
where
    V: 'static,
{
    guard: Box<Guard>,
    #[borrows(guard)]
    #[covariant]
    redo: Shared<'this, RedoNode<V>>,
}

impl<V> GuardedRedoNode<V>
where
    V: 'static,
{
    pub fn from_raw(guard: Box<Guard>, redo: *const RedoNode<V>) -> Self {
        GuardedRedoNodeBuilder {
            guard,
            redo_builder: |_| Shared::from(redo).with_tag(1),
        }
        .build()
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "hybrid_ts")] {

        #[derive(Clone)]
        pub struct Snapshot {
            // First active transaction.
            min_xid: Timestamp,
            // First unassigned XID.
            max_xid: Timestamp,
            // Active transactions at the time of snapshot.
            xips: HashSet<Timestamp>,
        }

        impl Snapshot {
            /// Is the XID in progress according to the snapshot
            pub fn is_xid_in_progress(&self, xid: Timestamp) -> bool {
                if xid < self.min_xid {
                    // Txn must be committed or aborted
                    return false;
                }

                if xid >= self.max_xid {
                    return true;
                }

                self.xips.contains(&xid)
            }
        }

        impl fmt::Display for Snapshot {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    "{}:{}:{}",
                    self.min_xid,
                    self.max_xid,
                    self.xips
                        .iter()
                        .map(Timestamp::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }

    }
}

pub struct Transaction<'t, K, V>
where
    V: 'static,
{
    mode: TransactionMode,
    start_timestamp: Timestamp,
    commit_timestamp: AtomicU64,
    is_readonly: bool,
    /// Redo nodes that need to be marked with the commit timestamp at commit time.
    redo_nodes: Vec<(&'t dyn MemTable<K, V>, K, GuardedRedoNode<V>)>,

    /// Guard to prevent the data accessed by this transaction from being collected.
    gc_guard: Option<Guard>,

    /// Immutable tables referenced by this txn
    imm_tables: HashMap<Timestamp, Arc<dyn ImmutableMemTable<K, V>>>,

    /// Values read from sstables by this txn
    value_cache: Vec<Pin<Box<V>>>,

    staged_log_records: Vec<Vec<u8>>,

    #[cfg(feature = "hybrid_ts")]
    current_snapshot: Option<Snapshot>,
}

impl<'t, K, V> fmt::Display for Transaction<'t, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Transaction({})", self.start_timestamp,)
    }
}

impl<'t, K, V> Transaction<'t, K, V>
where
    K: Serialize,
{
    pub fn new(
        mode: TransactionMode,
        start_timestamp: Timestamp,
        commit_timestamp: Timestamp,
    ) -> Self {
        Self {
            mode,
            start_timestamp,
            commit_timestamp: AtomicU64::new(commit_timestamp.into()),
            is_readonly: true,
            redo_nodes: Vec::new(),
            gc_guard: None,
            imm_tables: HashMap::new(),
            value_cache: Vec::new(),
            staged_log_records: Vec::new(),
            #[cfg(feature = "hybrid_ts")]
            current_snapshot: None,
        }
    }

    pub fn mode(&self) -> TransactionMode {
        self.mode
    }

    pub fn start_timestamp(&self) -> Timestamp {
        self.start_timestamp
    }

    pub fn commit_timestamp(&self, order: Ordering) -> Timestamp {
        Timestamp::from(self.commit_timestamp.load(order))
    }

    pub fn is_readonly(&self) -> bool {
        self.is_readonly
    }

    pub fn is_fast_commit(&self) -> bool {
        self.mode == TransactionMode::FastCommit
    }

    pub fn get_insert_redo(&self, inserted: V) -> Owned<RedoNode<V>> {
        let _mguard = MemContextGuard::new(MemContext::RedoNode);
        let redo = RedoNode::new_insert(self.commit_timestamp(Ordering::Relaxed), inserted);
        Owned::new(redo)
    }

    pub fn get_delete_redo(&self) -> Owned<RedoNode<V>> {
        let _mguard = MemContextGuard::new(MemContext::RedoNode);
        let redo = RedoNode::new_delete(self.commit_timestamp(Ordering::Relaxed));
        Owned::new(redo)
    }

    pub fn add_redo_node(
        &mut self,
        table: &'t dyn MemTable<K, V>,
        key: K,
        redo_node: GuardedRedoNode<V>,
    ) {
        let _mguard = MemContextGuard::new(MemContext::TransactionPrivate);
        self.is_readonly = false;

        if !self.is_fast_commit() {
            self.redo_nodes.push((table, key, redo_node));
        }
    }

    pub fn take_gc_guard(&mut self) -> Option<Guard> {
        self.gc_guard.take()
    }

    pub fn set_gc_guard(&mut self, guard: Guard) {
        self.gc_guard = Some(guard);
    }

    pub fn ref_imm_table(&mut self, min_xip: Timestamp, imt: Arc<dyn ImmutableMemTable<K, V>>) {
        self.imm_tables.insert(min_xip, imt);
    }

    pub fn record_value(&mut self, value: V) -> &'t V {
        let _mguard = MemContextGuard::new(MemContext::TransactionPrivate);

        let boxed = Box::pin(value);
        let rv = unsafe { std::mem::transmute::<_, &'t V>(boxed.as_ref()) };

        self.value_cache.push(boxed);
        rv
    }

    pub unsafe fn drop_read_guard(&mut self) {
        self.gc_guard = None;
        self.imm_tables.clear();
        self.value_cache.clear();
    }

    pub fn stage_log_record(&mut self, wal: &Wal, record: LogRecord<K>) -> Result<()> {
        let _mguard = MemContextGuard::new(MemContext::TransactionPrivate);
        let buf = wal.get_full_record(self.start_timestamp, record);
        self.staged_log_records.push(buf);

        Ok(())
    }

    pub fn commit_log_records(&self, wal: &Wal) -> Result<()> {
        wal.append_all(&self.staged_log_records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_compare_timestamp() {
        let xid = Timestamp::new(TimestampType::Uncommitted, 1);
        assert!(!xid.dec().is_invalid());
        assert!(xid.dec() < xid);
        assert!(!xid.inc().is_invalid());
        assert!(xid.inc() > xid);

        let xid1 = xid.dec();
        assert!(!xid1.dec().is_invalid());
        assert!(xid1.dec() < xid1);
        assert!(!xid1.inc().is_invalid());
        assert!(xid1.inc() > xid1);

        assert!(
            Timestamp::new(TimestampType::Commit, 1)
                < Timestamp::new(TimestampType::Uncommitted, 1)
        );
    }
}
