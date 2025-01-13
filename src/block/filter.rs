use crate::{
    block::{Block, FilterBitsBuilder, FilterPolicy},
    table::BlockTableOptions,
    Result,
};

use std::sync::Arc;

pub trait FilterBlockBuilder {
    fn start_block(&mut self, block_offset: usize);
    fn add(&mut self, key: &[u8]) -> Result<()>;
    fn seal(&mut self) -> Vec<u8>;
}

pub trait FilterBlockReader: Sync + Send {
    fn key_may_match(&self, key: &[u8]) -> bool;
}

pub struct FullFilterBlockBuilder {
    filter_bits_builder: Box<dyn FilterBitsBuilder>,
    num_added: usize,
}

impl FullFilterBlockBuilder {
    fn new(filter_bits_builder: Box<dyn FilterBitsBuilder>) -> Self {
        Self {
            filter_bits_builder,
            num_added: 0,
        }
    }
}

impl FilterBlockBuilder for FullFilterBlockBuilder {
    fn start_block(&mut self, _block_offset: usize) {}

    fn add(&mut self, key: &[u8]) -> Result<()> {
        self.filter_bits_builder.add_key(key);
        self.num_added += 1;

        Ok(())
    }

    fn seal(&mut self) -> Vec<u8> {
        if self.num_added > 0 {
            self.filter_bits_builder.seal()
        } else {
            Vec::new()
        }
    }
}

pub struct FullFilterBlockReader {
    policy: Arc<dyn FilterPolicy>,
    block: Block,
}

impl FullFilterBlockReader {
    fn new(policy: Arc<dyn FilterPolicy>, block: Block) -> Self {
        Self { policy, block }
    }
}

impl FilterBlockReader for FullFilterBlockReader {
    fn key_may_match(&self, key: &[u8]) -> bool {
        let filter_bits_reader = self.policy.get_filter_bits_reader(&self.block.contents());

        filter_bits_reader.may_match(key)
    }
}

pub fn create_filter_block_builder(opts: BlockTableOptions) -> Box<dyn FilterBlockBuilder> {
    let filter_bits_builder = opts.filter_policy.get_filter_bits_builder();

    Box::new(FullFilterBlockBuilder::new(filter_bits_builder))
}

pub fn create_filter_block_reader(
    opts: &BlockTableOptions,
    block: Block,
) -> Box<dyn FilterBlockReader> {
    Box::new(FullFilterBlockReader::new(
        opts.filter_policy.clone(),
        block,
    ))
}
