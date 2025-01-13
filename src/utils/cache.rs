use std::{
    borrow::Borrow,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
};

use fasthash::{xx, FastHasher};
use lru::{KeyRef, LruCache};

trait Product<A, B> {
    fn fst(&self) -> &A;
    fn snd(&self) -> &B;
}

impl<'a, A, B> Borrow<dyn Product<A, B> + 'a> for KeyRef<(A, B)>
where
    A: Eq + Hash + 'a,
    B: Eq + Hash + 'a,
{
    fn borrow(&self) -> &(dyn Product<A, B> + 'a) {
        Borrow::<(A, B)>::borrow(self)
    }
}

impl<A: Hash, B: Hash> Hash for (dyn Product<A, B> + '_) {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fst().hash(state);
        self.snd().hash(state);
    }
}

impl<A: Eq, B: Eq> PartialEq for (dyn Product<A, B> + '_) {
    fn eq(&self, other: &Self) -> bool {
        self.fst() == other.fst() && self.snd() == other.snd()
    }
}

impl<A: Eq, B: Eq> Eq for (dyn Product<A, B> + '_) {}

impl<A, B> Product<A, B> for (A, B) {
    fn fst(&self) -> &A {
        &self.0
    }
    fn snd(&self) -> &B {
        &self.1
    }
}

impl<A, B> Product<A, B> for (&A, &B) {
    fn fst(&self) -> &A {
        self.0
    }
    fn snd(&self) -> &B {
        self.1
    }
}

pub type CacheId = u32;
pub type CacheKey<K> = (CacheId, K);

#[allow(dead_code)]
pub struct Cache<K, V> {
    cache: LruCache<CacheKey<K>, V>,
    last_id: CacheId,
}

#[allow(dead_code)]
impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(capacity: usize) -> Cache<K, V> {
        assert!(capacity > 0);
        Cache {
            cache: LruCache::new(capacity),
            last_id: 0,
        }
    }

    pub fn get_cache_id(&mut self) -> CacheId {
        self.last_id += 1;
        self.last_id
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn capacity(&self) -> usize {
        self.cache.cap()
    }

    /// Insert a new element into the cache.
    pub fn insert(&mut self, cache_id: CacheId, key: K, value: V) -> Option<V> {
        self.cache.put((cache_id, key), value)
    }

    /// Retrieve an element from the cache and pin the element.
    pub fn get(&mut self, cache_id: CacheId, key: &K) -> Option<&V> {
        self.cache
            .get(&(&cache_id, key) as &dyn Product<CacheId, K>)
    }

    /// Invalidate an element in the cache.
    pub fn remove(&mut self, cache_id: CacheId, key: &K) -> Option<V> {
        self.cache
            .pop(&(&cache_id, key) as &dyn Product<CacheId, K>)
    }
}

pub struct ShardedCache<K, V> {
    num_shards: usize,
    shards: Vec<Mutex<LruCache<CacheKey<K>, V>>>,
    last_id: AtomicU32,
}

impl<K, V> ShardedCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new(num_shards: usize, capacity: usize) -> Self {
        assert!(capacity > 0);

        let shards = (0..num_shards)
            .map(|_| Mutex::new(LruCache::new(capacity / num_shards)))
            .collect();

        Self {
            num_shards,
            shards,
            last_id: AtomicU32::new(1),
        }
    }

    pub fn get_cache_id(&self) -> CacheId {
        self.last_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline(always)]
    fn get_shard(&self, cache_id: CacheId, key: &K) -> usize {
        let mut s = xx::Hasher32::new();
        (cache_id, key).hash(&mut s);
        let hash = s.finish();

        hash as usize % self.num_shards
    }

    /// Insert a new element into the cache.
    pub fn insert(&self, cache_id: CacheId, key: K, value: V) -> Option<V> {
        let mut shard = self.shards[self.get_shard(cache_id, &key)].lock().unwrap();
        shard.put((cache_id, key), value)
    }

    /// Retrieve an element from the cache and pin the element.
    pub fn get(&self, cache_id: CacheId, key: &K) -> Option<V> {
        let mut shard = self.shards[self.get_shard(cache_id, &key)].lock().unwrap();
        shard
            .get(&(&cache_id, key) as &dyn Product<CacheId, K>)
            .cloned()
    }

    /// Invalidate an element in the cache.
    pub fn remove(&self, cache_id: CacheId, key: &K) -> Option<V> {
        let mut shard = self.shards[self.get_shard(cache_id, &key)].lock().unwrap();
        shard.pop(&(&cache_id, key) as &dyn Product<CacheId, K>)
    }
}

unsafe impl<K: Send, V: Send> Send for Cache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for Cache<K, V> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache() {
        let mut cache = Cache::new(3);

        cache.insert(0, "a", 1);
        cache.insert(0, "b", 2);
        cache.insert(0, "c", 3);
        cache.insert(0, "d", 4);
        cache.insert(0, "e", 5);

        assert_eq!(cache.len(), 3);

        assert_eq!(cache.get(0, &"a"), None);
        assert_eq!(cache.get(0, &"b"), None);
        assert_eq!(cache.get(0, &"c"), Some(&3));
        assert_eq!(cache.get(0, &"d"), Some(&4));
        assert_eq!(cache.get(0, &"e"), Some(&5));
    }
}
