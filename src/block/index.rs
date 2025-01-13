use crate::{block::DataBlockBuilder, Result};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug, Serialize, Deserialize)]
pub struct ItemPointer {
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

pub trait BlockIndexBuilder<K> {
    fn add_item(&mut self, key: &K, ptr: &ItemPointer) -> Result<()>;
    fn seal(&mut self) -> Vec<u8>;
    fn reset(&mut self);
}

pub struct BinarySearchIndexBuilder {
    block_builder: DataBlockBuilder,
}

impl BinarySearchIndexBuilder {
    pub fn new(restart_interval: usize) -> Self {
        Self {
            block_builder: DataBlockBuilder::new(restart_interval),
        }
    }
}

impl<'de, K> BlockIndexBuilder<K> for BinarySearchIndexBuilder
where
    K: Serialize + AsRef<[u8]>,
{
    fn add_item(&mut self, key: &K, ptr: &ItemPointer) -> Result<()> {
        let key_buf = key.as_ref();
        let ptr_buf = bincode::serialize(ptr).unwrap();

        self.block_builder.add(&key_buf, &ptr_buf)
    }
}

impl<'de, K> BlockIndexBuilder<K> for BinarySearchIndexBuilder
where
    K: Serialize,
{
    default fn add_item(&mut self, key: &K, ptr: &ItemPointer) -> Result<()> {
        let key_buf = bincode::serialize(key).unwrap();
        let ptr_buf = bincode::serialize(ptr).unwrap();

        self.block_builder.add(&key_buf, &ptr_buf)
    }

    default fn seal(&mut self) -> Vec<u8> {
        self.block_builder.seal()
    }

    default fn reset(&mut self) {
        self.block_builder.reset();
    }
}

pub trait BlockIndexIterator<K>: Iterator<Item = (K, ItemPointer)> {
    fn seek(&mut self, key: &K);
}

pub trait BlockIndexReader<K> {
    fn iter(&self) -> dyn BlockIndexIterator<K>;
}
