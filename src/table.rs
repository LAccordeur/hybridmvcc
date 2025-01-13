mod block_table;

use crate::{
    memdebug::{MemContext, MemContextGuard},
    transaction::Timestamp,
    utils::ShardedCache,
    DbOptions, Result,
};

pub use self::block_table::{
    BlockTableBuilder, BlockTableFactory, BlockTableOptions, BlockTableReader,
};

use std::{
    fs::{File, OpenOptions},
    io::prelude::*,
    sync::Arc,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use ouroboros::self_referencing;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableFileRef(usize /* path id */, usize /* seqnum */);

impl TableFileRef {
    pub fn new(path_id: usize, seqnum: usize) -> Self {
        Self(path_id, seqnum)
    }

    pub fn path_id_seqnum(&self) -> (usize, usize) {
        (self.0, self.1)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableFileMeta<K> {
    pub file_ref: TableFileRef,
    pub size: usize,
    pub min_xip: Timestamp,
    pub infimum: K,
    pub supremum: K,
    pub being_compacted: bool,
}

pub type TableFileSet<K> = Vec<Vec<TableFileMeta<K>>>;

impl<K> TableFileMeta<K> {
    pub fn new(
        file_ref: TableFileRef,
        size: usize,
        min_xip: Timestamp,
        infimum: K,
        supremum: K,
    ) -> Self {
        Self {
            file_ref,
            size,
            min_xip,
            infimum,
            supremum,
            being_compacted: false,
        }
    }
}

pub trait TableBuilder<K, V> {
    fn size_hint(&self) -> usize;

    fn add(&mut self, key: &K, value: &V) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

pub trait TableReader<K, V>: Sync + Send {
    fn get(&self, key: &K) -> Result<Option<V>>;

    fn iter(&self, for_compaction: bool) -> Box<dyn TableIterator<K, V> + '_>;
}

pub trait TableIterator<K, V> {
    fn next(&mut self) -> Result<Option<(K, V)>>;

    fn reset(&mut self);
    fn seek(&mut self, key: &K);
}

#[self_referencing]
pub struct OwningTableIterator<K: 'static, V: 'static> {
    table: Arc<dyn TableReader<K, V>>,
    #[borrows(table)]
    #[covariant]
    iter: Box<dyn TableIterator<K, V> + 'this>,
}

impl<K, V> OwningTableIterator<K, V> {
    pub fn from_table(table: Arc<dyn TableReader<K, V>>, for_compaction: bool) -> Self {
        OwningTableIteratorBuilder {
            table: table,
            iter_builder: |table| table.iter(for_compaction),
        }
        .build()
    }
}

impl<K, V> TableIterator<K, V> for OwningTableIterator<K, V> {
    fn next(&mut self) -> Result<Option<(K, V)>> {
        self.with_iter_mut(|iter| iter.next())
    }
    fn reset(&mut self) {
        self.with_iter_mut(|iter| iter.reset());
    }
    fn seek(&mut self, key: &K) {
        self.with_iter_mut(|iter| iter.seek(key));
    }
}

pub trait TableFactory<K, V>: Sync + Send {
    fn create_table_builder<'a>(&self, out: &'a mut dyn Write) -> Box<dyn TableBuilder<K, V> + 'a>;
    fn create_table_reader<'a>(
        &self,
        reader: File,
        reader_size: usize,
    ) -> Result<Box<dyn TableReader<K, V> + 'a>>;
}

pub fn default_table_factory<K, V>() -> Arc<dyn TableFactory<K, V>>
where
    K: 'static + Clone + Ord + Serialize + DeserializeOwned,
    V: 'static + Serialize + DeserializeOwned,
{
    let options = BlockTableOptions::default();

    Arc::new(BlockTableFactory::new(options))
}

pub struct TableCache<K, V> {
    options: DbOptions<K, V>,
    cache: ShardedCache<TableFileRef, Arc<dyn TableReader<K, V>>>,
}

impl<K, V> TableCache<K, V> {
    pub fn new(options: DbOptions<K, V>) -> Self {
        let max_open_files = options.max_open_files;

        Self {
            options,
            cache: ShardedCache::new(32, max_open_files),
        }
    }

    pub fn get_table(&self, file_ref: TableFileRef) -> Result<Arc<dyn TableReader<K, V>>> {
        let _mguard = MemContextGuard::new(MemContext::TableCache);

        if let Some(table) = self.cache.get(0, &file_ref) {
            return Ok(table.clone());
        }

        // need to open the table
        let filename = self.options.get_table_file_path(file_ref);
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(filename)?;
        let file_size = file.metadata()?.len();

        let table: Arc<dyn TableReader<K, V>> = Arc::from(
            self.options
                .table_factory
                .create_table_reader(file, file_size as usize)?,
        );

        self.cache.insert(0, file_ref, table.clone());
        Ok(table)
    }
}
