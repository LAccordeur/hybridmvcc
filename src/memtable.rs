use crate::{
    storage::RedoNode,
    transaction::{Timestamp, Transaction},
    Db, Result,
};

use crossbeam::epoch::Guard;

mod hash_memtable;
mod memtable_log;

pub use self::{hash_memtable::HashMemTable, memtable_log::MemTableLogRecord};

pub trait MemTableIterator<'a, K, V> {
    fn next(&mut self) -> Option<(&'a K, Option<&'a V>)>;
}

pub trait ImmutableMemTable<K, V>: Sync + Send
where
    K: Sync + Send,
    V: Sync + Send,
{
    fn size(&self) -> usize;

    fn get<'a>(&'a self, key: &K) -> Result<Option<&'a V>>;

    fn iter<'a>(&'a self) -> Box<dyn MemTableIterator<'a, K, V> + 'a>;
}

pub trait MemTable<K, V>: Sync + Send
where
    K: Sync + Send,
    V: Sync + Send,
{
    fn size(&self) -> usize;

    fn get<'g>(
        &self,
        db: &Db<K, V>,
        txn: &mut Transaction<K, V>,
        key: &K,
        guard: &'g Guard,
    ) -> Result<Option<&'g V>>;

    fn put<'t>(
        &'t self,
        db: &Db<K, V>,
        txn: &mut Transaction<'t, K, V>,
        key: K,
        value: V,
    ) -> Result<bool>;

    fn update<'t>(
        &'t self,
        db: &Db<K, V>,
        txn: &mut Transaction<'t, K, V>,
        key: &K,
        f: &dyn Fn(&V) -> V,
    ) -> Result<bool>;

    fn delete<'t>(
        &'t self,
        db: &Db<K, V>,
        txn: &mut Transaction<'t, K, V>,
        key: &K,
    ) -> Result<bool>;

    fn rollback(&self, txn: &Transaction<K, V>, key: &K, redo: &RedoNode<V>) -> Result<()>;

    /// Take a snapshot of the memory table that is valid to all transactions that start
    /// after min_xip.
    fn snapshot(
        &self,
        min_xip: Timestamp,
        freeze_min_age: usize,
    ) -> Result<Box<dyn ImmutableMemTable<K, V>>>;

    /// Clean-up key-value pairs that are not active after min_xip.
    fn gc(&self, min_xip: Timestamp, freeze_min_age: usize) -> Result<()>;

    fn optimize_space(&self);
}
