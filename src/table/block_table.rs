use crate::{
    block::{
        create_filter_block_builder, create_filter_block_reader, default_filter_policy,
        BinarySearchIndexBuilder, Block, BlockIndexBuilder, DataBlockBuilder, DataBlockIterator,
        DataBlockView, FilterBlockBuilder, FilterBlockReader, FilterPolicy, ItemPointer,
        DEFAULT_BLOCK_SIZE,
    },
    table::{TableBuilder, TableFactory, TableIterator, TableReader},
    utils::{CacheId, RandomAccess, ShardedCache},
    Error, Result,
};

use std::{cmp::Ordering, fs::File, io::prelude::*, marker::PhantomData, sync::Arc};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{de::DeserializeOwned, Serialize};

const BLOCK_TABLE_MAGIC_NUMBER: u32 = 0xb10cba5e;
const BLOCK_TRAILER_LENGTH: usize = 4;
const TABLE_FOOTER_LENGTH: usize = 40;

const DEFAULT_BLOCK_CACHE_CAPACITY: usize = 4096 << 20;
const DEFAULT_BLOCK_CACHE_SHARDS: usize = 32;

pub struct TableFooter {
    index_ptr: ItemPointer,
    filter_ptr: Option<ItemPointer>,
}

impl TableFooter {
    pub fn new(index_ptr: ItemPointer, filter_ptr: Option<ItemPointer>) -> Self {
        Self {
            index_ptr,
            filter_ptr,
        }
    }
}

pub struct BlockTableBuilder<'a, K, V> {
    options: BlockTableOptions,
    block_builder: DataBlockBuilder,
    index_builder: Box<dyn BlockIndexBuilder<K>>,
    filter_builder: Option<Box<dyn FilterBlockBuilder>>,
    out: &'a mut dyn Write,
    out_offset: usize,
    last_key: Option<K>,
    value_type: PhantomData<V>,
}

impl<'a, K, V> BlockTableBuilder<'a, K, V>
where
    K: Serialize + DeserializeOwned,
{
    pub fn new(options: BlockTableOptions, out: &'a mut dyn Write) -> Self {
        let restart_interval = options.block_restart_interval;

        Self {
            options: options.clone(),
            block_builder: DataBlockBuilder::new(restart_interval),
            index_builder: Box::new(BinarySearchIndexBuilder::new(restart_interval)),
            filter_builder: Some(create_filter_block_builder(options)),
            out,
            out_offset: 0,
            last_key: None,
            value_type: PhantomData,
        }
    }

    fn need_flush(&self, key: &[u8], value: &[u8]) -> bool {
        let cur_len = self.block_builder.full_len();

        cur_len + key.len() + value.len() > self.options.block_size
    }

    fn flush(&mut self) -> Result<Option<ItemPointer>> {
        if self.block_builder.empty() {
            return Ok(None);
        }

        let buf = self.block_builder.seal();
        self.block_builder.reset();
        let ptr = self.write_block(&buf[..])?;

        Ok(Some(ptr))
    }

    fn write_block(&mut self, buf: &[u8]) -> Result<ItemPointer> {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(buf);
        let crc = hasher.finalize();

        let offset = self.out_offset;
        let length = buf.len();

        self.out.write_all(buf)?;
        self.out_offset += buf.len();

        self.out.write_u32::<LittleEndian>(crc as u32)?;
        self.out_offset += 4;

        assert_eq!(offset + buf.len() + BLOCK_TRAILER_LENGTH, self.out_offset);

        Ok(ItemPointer {
            offset: offset as u64,
            length: length as u64,
        })
    }

    fn write_toc(&mut self, footer: &TableFooter) -> Result<()> {
        let mut toc = Vec::new();

        toc.write_u32::<LittleEndian>(BLOCK_TABLE_MAGIC_NUMBER)?;
        toc.write_u64::<LittleEndian>(footer.index_ptr.offset)?;
        toc.write_u64::<LittleEndian>(footer.index_ptr.length)?;

        if let Some(filter_ptr) = footer.filter_ptr {
            toc.write_u64::<LittleEndian>(filter_ptr.offset)?;
            toc.write_u64::<LittleEndian>(filter_ptr.length)?;
        } else {
            toc.write_u64::<LittleEndian>(0)?;
            toc.write_u64::<LittleEndian>(0)?;
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&toc);
        let crc = hasher.finalize();
        toc.write_u32::<LittleEndian>(crc)?;

        self.out.write_all(&toc)?;
        assert_eq!(toc.len(), TABLE_FOOTER_LENGTH);
        self.out_offset += toc.len();

        Ok(())
    }

    fn add_raw(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let need_flush = self.need_flush(&key, &value);

        if need_flush {
            let ptr = self.flush()?;

            if let (Some(ptr), Some(last_key)) = (ptr, &self.last_key) {
                self.index_builder.add_item(last_key, &ptr)?;
            }

            if let Some(filter_builder) = &mut self.filter_builder {
                filter_builder.start_block(self.out_offset);
            }
        }

        if let Some(filter_builder) = &mut self.filter_builder {
            filter_builder.add(key)?;
        }

        self.block_builder.add(&key, &value)?;

        Ok(())
    }
}

impl<K, V> TableBuilder<K, V> for BlockTableBuilder<'_, K, V>
where
    K: Clone + Serialize + DeserializeOwned + AsRef<[u8]>,
    V: Serialize + DeserializeOwned + AsRef<[u8]>,
{
    fn add(&mut self, key: &K, value: &V) -> Result<()> {
        self.add_raw(key.as_ref(), value.as_ref())?;
        self.last_key = Some(key.clone());

        Ok(())
    }
}

impl<K, V> TableBuilder<K, V> for BlockTableBuilder<'_, K, V>
where
    K: Clone + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    default fn size_hint(&self) -> usize {
        let mut size = self.out_offset;

        size += self.block_builder.full_len();
        size += TABLE_FOOTER_LENGTH;

        size
    }

    default fn add(&mut self, key: &K, value: &V) -> Result<()> {
        let key_buf = bincode::serialize(key).unwrap();
        let value_buf = bincode::serialize(value).unwrap();

        self.add_raw(&key_buf, &value_buf)?;
        self.last_key = Some(key.clone());

        Ok(())
    }

    default fn finish(&mut self) -> Result<()> {
        let ptr = self.flush()?;

        if let (Some(ptr), Some(last_key)) = (ptr, &self.last_key) {
            self.index_builder.add_item(last_key, &ptr)?;
        }

        let filter_ptr = if let Some(filter_builder) = &mut self.filter_builder {
            let filter_block = filter_builder.seal();
            Some(self.write_block(&filter_block)?)
        } else {
            None
        };

        let index_block = self.index_builder.seal();
        self.index_builder.reset();
        let index_ptr = self.write_block(&index_block)?;

        let footer = TableFooter::new(index_ptr, filter_ptr);
        self.write_toc(&footer)
    }
}

type SharedBlockCache = Arc<ShardedCache<u64, Block>>;

#[derive(Clone)]
pub struct BlockTableOptions {
    pub block_cache: SharedBlockCache,
    pub block_size: usize,
    pub block_restart_interval: usize,
    pub filter_policy: Arc<dyn FilterPolicy>,
}

impl Default for BlockTableOptions {
    fn default() -> Self {
        Self {
            block_cache: Arc::new(ShardedCache::new(
                DEFAULT_BLOCK_CACHE_SHARDS,
                DEFAULT_BLOCK_CACHE_CAPACITY / DEFAULT_BLOCK_SIZE,
            )),
            block_size: DEFAULT_BLOCK_SIZE,
            block_restart_interval: 16,
            filter_policy: default_filter_policy(),
        }
    }
}

impl BlockTableOptions {
    pub fn with_cache(block_cache: SharedBlockCache) -> Self {
        Self {
            block_cache,
            block_size: DEFAULT_BLOCK_SIZE,
            block_restart_interval: 16,
            filter_policy: default_filter_policy(),
        }
    }
}

pub struct BlockTableReader<R> {
    reader: R,
    _reader_size: usize,
    opts: BlockTableOptions,
    _toc: TableFooter,
    index_block: Block,
    filter_reader: Option<Box<dyn FilterBlockReader>>,

    cache_id: CacheId,
}

impl<R: RandomAccess> BlockTableReader<R> {
    pub fn open(opts: BlockTableOptions, reader: R, reader_size: usize) -> Result<Self> {
        let cache_id = opts.block_cache.get_cache_id();

        let footer = Self::read_footer(&reader, reader_size)?;
        let index_block = Self::read_block(&reader, footer.index_ptr)?;
        let filter_block = if let Some(filter_ptr) = footer.filter_ptr {
            Some(Self::read_block(&reader, filter_ptr)?)
        } else {
            None
        };

        let table_reader = Self {
            reader,
            _reader_size: reader_size,
            _toc: footer,
            index_block,
            filter_reader: filter_block.map(|fb| create_filter_block_reader(&opts, fb)),
            cache_id,
            opts,
        };

        Ok(table_reader)
    }

    fn read_footer(reader: &R, reader_size: usize) -> Result<TableFooter> {
        let mut footer_buf = [0u8; TABLE_FOOTER_LENGTH];
        reader.read_at(&mut footer_buf, reader_size - TABLE_FOOTER_LENGTH)?;

        // checksum
        let (footer, crc_buf) = footer_buf.split_at(TABLE_FOOTER_LENGTH - 4);
        let crc_file = (&crc_buf[..]).read_u32::<LittleEndian>()?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&footer);
        let crc = hasher.finalize();

        if crc != crc_file {
            return Err(Error::DataCorrupted("checksum does not match".to_owned()));
        }

        // check magic
        let magic = (&footer[0..]).read_u32::<LittleEndian>()?;
        if magic != BLOCK_TABLE_MAGIC_NUMBER {
            return Err(Error::DataCorrupted(
                "table magic number does not match".to_owned(),
            ));
        }

        // read index pointer
        let index_offset = (&footer[4..]).read_u64::<LittleEndian>()?;
        let index_length = (&footer[12..]).read_u64::<LittleEndian>()?;

        let index_ptr = ItemPointer {
            offset: index_offset,
            length: index_length,
        };

        // read filter pointer
        let filter_offset = (&footer[20..]).read_u64::<LittleEndian>()?;
        let filter_length = (&footer[28..]).read_u64::<LittleEndian>()?;

        let filter_ptr = if filter_length > 0 {
            Some(ItemPointer {
                offset: filter_offset,
                length: filter_length,
            })
        } else {
            None
        };

        Ok(TableFooter::new(index_ptr, filter_ptr))
    }

    fn read_block(reader: &R, block_ptr: ItemPointer) -> Result<Block> {
        let mut contents = vec![0; block_ptr.length as usize + BLOCK_TRAILER_LENGTH];
        reader.read_at(&mut contents, block_ptr.offset as usize)?;

        let trailer = contents.split_off(block_ptr.length as usize);
        let crc_file = (&trailer[0..]).read_u32::<LittleEndian>()?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&contents);
        let crc = hasher.finalize();

        if crc != crc_file {
            return Err(Error::DataCorrupted(
                "block checksum does not match".to_owned(),
            ));
        }

        Ok(Block::new(contents))
    }

    fn get_block(&self, block_ptr: ItemPointer, for_compaction: bool) -> Result<Block> {
        if let Some(block) = self.opts.block_cache.get(self.cache_id, &block_ptr.offset) {
            return Ok(block.clone());
        }

        let block = Self::read_block(&self.reader, block_ptr)?;

        if !for_compaction {
            self.opts
                .block_cache
                .insert(self.cache_id, block_ptr.offset, block.clone());
        }

        Ok(block)
    }
}

// impl BlockTableReader<File> {
//     pub fn open_file<P: AsRef<Path>>(opts: BlockTableOptions, path: P) -> Result<Self> {
//         let file = OpenOptions::new().read(true).open(path)?;
//         let file_size = file.metadata()?.len() as usize;
//         Self::open(opts, file, file_size)
//     }
// }

impl<R, K, V> TableReader<K, V> for BlockTableReader<R>
where
    R: RandomAccess,
    K: 'static + Ord + Serialize + DeserializeOwned + AsRef<[u8]>,
    V: 'static + DeserializeOwned + AsRef<[u8]> + From<String>,
{
    fn get(&self, key: &K) -> Result<Option<V>> {
        // check bloom filter
        if let Some(filter_reader) = &self.filter_reader {
            if !filter_reader.key_may_match(key.as_ref()) {
                return Ok(None);
            }
        }

        let index_view = DataBlockView::new(self.index_block.clone());
        let mut index_iter = index_view.iter(<[u8]>::cmp);
        index_iter.seek(key.as_ref());

        let block_ptr = match index_iter.current() {
            Some((last_key, block_ptr)) => {
                if key.as_ref() <= &last_key {
                    bincode::deserialize(&block_ptr).unwrap()
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        };

        let block = self.get_block(block_ptr, false)?;
        let block_view = DataBlockView::new(block);
        let mut block_iter = block_view.iter(<[u8]>::cmp);
        block_iter.seek(key.as_ref());

        match block_iter.current() {
            Some((key_buf, value_buf)) => {
                if key_buf == key.as_ref() {
                    Ok(Some(
                        unsafe { String::from_utf8_unchecked(value_buf) }.into(),
                    ))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn iter(&self, for_compaction: bool) -> Box<dyn TableIterator<K, V> + '_> {
        let index_view = DataBlockView::new(self.index_block.clone());
        let index_iter = index_view.iter(<[u8]>::cmp);

        Box::new(BlockTableIterator {
            table: self,
            block_iter: None,
            index_iter,
            key_comp: <[u8]>::cmp,
            for_compaction,
            kv_type: PhantomData,
        })
    }
}

impl<R, K, V> TableReader<K, V> for BlockTableReader<R>
where
    R: RandomAccess,
    K: 'static + Ord + Serialize + DeserializeOwned,
    V: 'static + DeserializeOwned,
{
    default fn get(&self, key: &K) -> Result<Option<V>> {
        let cmp = |a: &[u8], b: &[u8]| {
            let ka = bincode::deserialize::<K>(a).unwrap();
            let kb = bincode::deserialize::<K>(b).unwrap();

            ka.cmp(&kb)
        };
        let key_buf = bincode::serialize(key).unwrap();

        // check bloom filter
        if let Some(filter_reader) = &self.filter_reader {
            if !filter_reader.key_may_match(&key_buf) {
                return Ok(None);
            }
        }

        let index_view = DataBlockView::new(self.index_block.clone());
        let mut index_iter = index_view.iter(cmp);
        index_iter.seek(&key_buf);

        let block_ptr = match index_iter.current() {
            Some((k, block_ptr)) => {
                let last_key = bincode::deserialize::<K>(&k).unwrap();

                if key <= &last_key {
                    bincode::deserialize(&block_ptr).unwrap()
                } else {
                    return Ok(None);
                }
            }
            _ => return Ok(None),
        };

        let block = self.get_block(block_ptr, false)?;
        let block_view = DataBlockView::new(block);
        let mut block_iter = block_view.iter(cmp);
        block_iter.seek(&key_buf);

        match block_iter.current() {
            Some((key_buf, value_buf)) => {
                let k = bincode::deserialize::<K>(&key_buf).unwrap();

                if &k == key {
                    let value = bincode::deserialize(&value_buf).unwrap();
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    default fn iter(&self, for_compaction: bool) -> Box<dyn TableIterator<K, V> + '_> {
        let cmp = |a: &[u8], b: &[u8]| {
            let ka = bincode::deserialize::<K>(a).unwrap();
            let kb = bincode::deserialize::<K>(b).unwrap();

            ka.cmp(&kb)
        };

        let index_view = DataBlockView::new(self.index_block.clone());
        let index_iter = index_view.iter(cmp);

        Box::new(BlockTableIterator {
            table: self,
            block_iter: None,
            index_iter,
            key_comp: cmp,
            for_compaction,
            kv_type: PhantomData,
        })
    }
}

pub struct BlockTableIterator<'a, R, K, V, KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Ordering,
{
    table: &'a BlockTableReader<R>,
    block_iter: Option<DataBlockIterator<KCmp>>,
    index_iter: DataBlockIterator<KCmp>,
    key_comp: KCmp,
    for_compaction: bool,
    kv_type: PhantomData<(K, V)>,
}

impl<'a, R, K, V, KCmp> BlockTableIterator<'a, R, K, V, KCmp>
where
    R: RandomAccess,
    K: Ord + DeserializeOwned,
    KCmp: Clone + Fn(&[u8], &[u8]) -> Ordering,
{
    fn get_next_block(&mut self) -> Result<bool> {
        match self.index_iter.next() {
            Some((_, block_ptr)) => {
                let block_ptr = bincode::deserialize(&block_ptr).unwrap();
                self.load_block(block_ptr).map(|_| true)
            }
            _ => Ok(false),
        }
    }

    fn load_block(&mut self, block_ptr: ItemPointer) -> Result<()> {
        let block = self.table.get_block(block_ptr, self.for_compaction)?;
        let block_view = DataBlockView::new(block);
        self.block_iter = Some(block_view.iter(self.key_comp.clone()));

        Ok(())
    }
}

impl<'a, R, K, V, KCmp> TableIterator<K, V> for BlockTableIterator<'a, R, K, V, KCmp>
where
    R: RandomAccess,
    K: Ord + DeserializeOwned + From<String>,
    V: DeserializeOwned + From<String>,
    KCmp: Clone + Fn(&[u8], &[u8]) -> Ordering,
{
    fn next(&mut self) -> Result<Option<(K, V)>> {
        if self.block_iter.is_none() {
            if self.get_next_block()? {
                return self.next();
            } else {
                return Ok(None);
            }
        }

        if let Some(ref mut block_iter) = self.block_iter {
            if let Some((key_buf, value_buf)) = block_iter.next() {
                let key = unsafe { String::from_utf8_unchecked(key_buf) }.into();
                let value = unsafe { String::from_utf8_unchecked(value_buf) }.into();
                return Ok(Some((key, value)));
            }
        }

        self.block_iter = None;
        if self.get_next_block()? {
            self.next()
        } else {
            Ok(None)
        }
    }
}

impl<'a, R, K, V, KCmp> TableIterator<K, V> for BlockTableIterator<'a, R, K, V, KCmp>
where
    R: RandomAccess,
    K: Ord + DeserializeOwned,
    V: DeserializeOwned,
    KCmp: Clone + Fn(&[u8], &[u8]) -> Ordering,
{
    default fn next(&mut self) -> Result<Option<(K, V)>> {
        if self.block_iter.is_none() {
            if self.get_next_block()? {
                return self.next();
            } else {
                return Ok(None);
            }
        }

        if let Some(ref mut block_iter) = self.block_iter {
            if let Some((key_buf, value_buf)) = block_iter.next() {
                let key = bincode::deserialize::<K>(&key_buf).unwrap();
                let value = bincode::deserialize(&value_buf).unwrap();
                return Ok(Some((key, value)));
            }
        }

        self.block_iter = None;
        if self.get_next_block()? {
            self.next()
        } else {
            Ok(None)
        }
    }

    default fn reset(&mut self) {}

    default fn seek(&mut self, _key: &K) {}
}

pub struct BlockTableFactory {
    options: BlockTableOptions,
}

impl BlockTableFactory {
    pub fn new(options: BlockTableOptions) -> Self {
        Self { options }
    }
}

impl<K, V> TableFactory<K, V> for BlockTableFactory
where
    K: 'static + Clone + Ord + Serialize + DeserializeOwned,
    V: 'static + Serialize + DeserializeOwned,
{
    fn create_table_builder<'a>(&self, out: &'a mut dyn Write) -> Box<dyn TableBuilder<K, V> + 'a> {
        Box::new(BlockTableBuilder::new(self.options.clone(), out))
    }

    fn create_table_reader<'a>(
        &self,
        reader: File,
        reader_size: usize,
    ) -> Result<Box<dyn TableReader<K, V> + 'a>> {
        Ok(Box::new(BlockTableReader::open(
            self.options.clone(),
            reader,
            reader_size,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn can_build_table() {
        let mut file = tempfile::tempfile().unwrap();
        let options = BlockTableOptions::default();
        let mut builder = BlockTableBuilder::new(options, &mut file);

        for i in 0..1000 {
            assert!(builder.add(&i, &(i + 1)).is_ok());
        }

        assert!(builder.finish().is_ok());
    }

    #[test]
    fn can_read_table() {
        let mut file = tempfile::tempfile().unwrap();
        let options = BlockTableOptions::default();
        let mut builder = BlockTableBuilder::<u32, u32>::new(options, &mut file);

        for i in 1..=1000 {
            assert!(builder.add(&i, &(i + 1)).is_ok());
        }

        assert!(builder.finish().is_ok());
        file.sync_all().unwrap();

        let options = BlockTableOptions::default();
        let file_size = file.metadata().unwrap().len() as usize;
        let reader = BlockTableReader::open(options, file, file_size).unwrap();

        for i in 1..=1000 {
            assert_eq!(reader.get(&i).unwrap() as Option<u32>, Some(i + 1));
        }
        assert_eq!(reader.get(&0).unwrap() as Option<u32>, None);
        assert_eq!(reader.get(&1001).unwrap() as Option<u32>, None);

        let mut iter = reader.iter(false) as Box<dyn TableIterator<u32, u32>>;
        let mut count = 0;
        while let Some((k, v)) = iter.next().unwrap() {
            assert_eq!(k + 1, v);
            count += 1;
        }
        assert_eq!(count, 1000);
    }

    #[test]
    fn can_read_string_table() {
        let mut map = BTreeMap::new();

        let mut file = tempfile::tempfile().unwrap();
        let options = BlockTableOptions::default();
        let mut builder = BlockTableBuilder::<String, String>::new(options, &mut file);

        for i in 1..=1000 {
            map.insert(i.to_string(), (i + 1).to_string());
        }

        for (k, v) in &map {
            assert!(builder.add(k, v).is_ok());
        }

        assert!(builder.finish().is_ok());
        file.sync_all().unwrap();

        let options = BlockTableOptions::default();
        let file_size = file.metadata().unwrap().len() as usize;
        let reader = BlockTableReader::open(options, file, file_size).unwrap();

        for (k, v) in &map {
            assert_eq!(reader.get(k).unwrap() as Option<String>, Some(v.to_owned()));
        }
        assert_eq!(reader.get(&"0".to_owned()).unwrap() as Option<String>, None);
        assert_eq!(
            reader.get(&"1001".to_owned()).unwrap() as Option<String>,
            None
        );

        let mut iter = reader.iter(false) as Box<dyn TableIterator<String, String>>;
        let mut count = 0;
        while let Some((k, v)) = iter.next().unwrap() {
            assert_eq!(k.parse::<u32>().unwrap() + 1, v.parse::<u32>().unwrap());
            count += 1;
        }
        assert_eq!(count, 1000);
    }
}
