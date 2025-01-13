use crate::transaction::Timestamp;

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crossbeam::epoch::Atomic;

#[derive(Debug)]
pub enum RedoRecord<V> {
    Insert(V /* inserted */),
    Delete,
}

pub struct RedoNode<V> {
    timestamp: AtomicU64,
    collected: AtomicU32,
    pub(crate) record: RedoRecord<V>,
    pub(crate) next: Atomic<RedoNode<V>>,
}

impl<V> RedoNode<V> {
    pub fn new_insert(timestamp: Timestamp, inserted: V) -> Self {
        RedoNode {
            timestamp: AtomicU64::new(timestamp.into()),
            collected: AtomicU32::new(0),
            record: RedoRecord::Insert(inserted),
            next: Atomic::null(),
        }
    }

    pub fn new_delete(timestamp: Timestamp) -> Self {
        RedoNode {
            timestamp: AtomicU64::new(timestamp.into()),
            collected: AtomicU32::new(0),
            record: RedoRecord::Delete,
            next: Atomic::null(),
        }
    }

    pub fn load_timestamp(&self, order: Ordering) -> Timestamp {
        Timestamp::from(self.timestamp.load(order))
    }

    pub fn store_timestamp(&self, timestamp: Timestamp, order: Ordering) {
        self.timestamp.store(timestamp.into(), order)
    }

    pub fn try_set_collected(&self, collected: Timestamp) -> bool {
        let new: u64 = collected.into();
        self.collected
            .compare_exchange(0, new as u32, Ordering::Release, Ordering::Relaxed)
            == Ok(0)
    }

    pub fn load_collected(&self, order: Ordering) -> Timestamp {
        Timestamp::from(self.collected.load(order) as u64)
    }
}
