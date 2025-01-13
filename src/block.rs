mod data_block;
mod filter;
mod filter_policy;
mod index;

use std::sync::Arc;

pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4 << 10;

use crate::memdebug::{MemContext, MemContextGuard};

pub use self::{
    data_block::{DataBlockBuilder, DataBlockIterator, DataBlockReader, DataBlockView},
    filter::{
        create_filter_block_builder, create_filter_block_reader, FilterBlockBuilder,
        FilterBlockReader,
    },
    filter_policy::{default_filter_policy, FilterBitsBuilder, FilterPolicy},
    index::{BinarySearchIndexBuilder, BlockIndexBuilder, ItemPointer},
};

#[derive(Clone)]
pub struct Block(Arc<Vec<u8>>);

impl Block {
    pub fn new(contents: Vec<u8>) -> Self {
        let _mguard = MemContextGuard::new(MemContext::TableBlock);

        Block(Arc::new(contents))
    }

    #[inline(always)]
    pub fn contents(&self) -> &Arc<Vec<u8>> {
        &self.0
    }
}
