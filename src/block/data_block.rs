use crate::{block::Block, Result};

use std::{
    cmp::{self, Ordering},
    io::prelude::*,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use integer_encoding::{VarInt, VarIntWriter};

pub trait DataBlockReader {
    fn get_data_block_payload(&self) -> &[u8];

    fn len(&self) -> usize {
        self.get_data_block_payload().len()
    }
}

pub struct DataBlockView(Block);

impl DataBlockView {
    pub fn new(block: Block) -> Self {
        Self(block)
    }

    pub fn iter<KCmp>(self, key_comp: KCmp) -> DataBlockIterator<KCmp>
    where
        KCmp: Fn(&[u8], &[u8]) -> Ordering,
    {
        DataBlockIterator::new(self.0, key_comp)
    }
}

impl<'a> DataBlockReader for DataBlockView {
    fn get_data_block_payload(&self) -> &[u8] {
        &self.0.contents()
    }
}

pub struct DataBlockIterator<KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Ordering,
{
    block: Block,
    offset: usize,
    value_offset: usize,
    key_comp: KCmp,

    restarts_offset: usize,
    restart_key: Vec<u8>,
}

impl<KCmp> DataBlockIterator<KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Ordering,
{
    pub fn new(block: Block, key_comp: KCmp) -> Self {
        let mut iter = Self {
            block,
            offset: 0,
            value_offset: 0,
            key_comp,

            restarts_offset: 0,
            restart_key: Vec::new(),
        };

        iter.restarts_offset = iter.block.contents().len() - iter.num_restarts() * 4 - 4;

        iter
    }

    fn num_restarts(&self) -> usize {
        let contents = self.block.contents();
        (&contents[contents.len() - 4..])
            .read_u32::<LittleEndian>()
            .unwrap() as usize
    }

    fn get_restart_offset(&self, index: usize) -> usize {
        let restart = self.restarts_offset + 4 * index;
        (&self.block.contents()[restart..restart + 4])
            .read_u32::<LittleEndian>()
            .unwrap() as usize
    }

    fn next_entry(&mut self) -> (usize, usize, usize, usize) {
        let mut header_len = 0;
        let contents = self.block.contents();

        let (shared, shared_len) = usize::decode_var(&contents[self.offset..]);
        header_len += shared_len;

        let (non_shared, non_shared_len) = usize::decode_var(&contents[self.offset + header_len..]);
        header_len += non_shared_len;

        let (value_len, value_len_len) = usize::decode_var(&contents[self.offset + header_len..]);
        header_len += value_len_len;

        self.value_offset = self.offset + header_len + non_shared;
        self.offset = self.value_offset + value_len;

        (shared, non_shared, value_len, header_len)
    }

    fn assemble_key(&mut self, key_offset: usize, shared: usize, non_shared: usize) {
        self.restart_key.truncate(shared);
        self.restart_key
            .extend_from_slice(&self.block.contents()[key_offset..key_offset + non_shared]);
    }

    pub fn reset(&mut self) {
        self.offset = 0;
        self.value_offset = 0;
        self.restart_key.clear();
    }

    fn seek_to_restart(&mut self, index: usize) {
        let offset = self.get_restart_offset(index);

        self.offset = offset;
        let (shared, non_shared, _, header_len) = self.next_entry();

        self.assemble_key(offset + header_len, shared, non_shared);
    }

    pub fn seek(&mut self, key: &[u8]) {
        self.reset();

        let mut low = 0;
        let mut high = if self.num_restarts() > 0 {
            self.num_restarts() - 1
        } else {
            0
        };

        while low < high {
            let mid = (low + high + 1) >> 1;
            self.seek_to_restart(mid);

            if (self.key_comp)(&self.restart_key, key) >= Ordering::Equal {
                high = mid - 1;
            } else {
                low = mid;
            }
        }

        self.offset = self.get_restart_offset(low);

        while let Some((k, _)) = self.next() {
            if (self.key_comp)(&k, key) >= Ordering::Equal {
                return;
            }
        }
    }

    pub fn valid(&self) -> bool {
        !self.restart_key.is_empty()
            && self.value_offset > 0
            && self.value_offset <= self.restarts_offset
    }

    pub fn current(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.valid() {
            Some((
                self.restart_key.clone(),
                self.block.contents()[self.value_offset..self.offset].to_owned(),
            ))
        } else {
            None
        }
    }
}

impl<KCmp> Iterator for DataBlockIterator<KCmp>
where
    KCmp: Fn(&[u8], &[u8]) -> Ordering,
{
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.restarts_offset {
            self.reset();
            return None;
        }

        let current_offset = self.offset;
        let (shared, non_shared, value_len, header_len) = self.next_entry();
        self.assemble_key(current_offset + header_len, shared, non_shared);

        let value = &self.block.contents()[self.value_offset..self.value_offset + value_len];

        Some((self.restart_key.to_owned(), value.to_owned()))
    }
}

pub struct DataBlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<usize>,
    restart_interval: usize,

    last_key: Option<Vec<u8>>,
    restart_counter: usize,
}

impl<'a> DataBlockReader for DataBlockBuilder {
    fn get_data_block_payload(&self) -> &[u8] {
        &self.buffer[..]
    }
}

impl DataBlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        Self {
            buffer: Vec::new(),
            restarts: vec![0],
            restart_interval,

            last_key: None,
            restart_counter: 0,
        }
    }

    pub fn empty(&self) -> bool {
        self.len() == 0
    }

    pub fn full_len(&self) -> usize {
        self.len() + self.restarts.len() * 4 + 4
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.restarts = vec![0];
        self.restart_counter = 0;
        self.last_key = None;
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let shared = if self.restart_counter < self.restart_interval {
            if let Some(last_key) = &self.last_key {
                let min_len = cmp::min(last_key.len(), key.len());

                let mut shared = 0;
                while shared < min_len && last_key[shared] == key[shared] {
                    shared += 1;
                }

                shared
            } else {
                0
            }
        } else {
            self.restarts.push(self.buffer.len());
            self.last_key = None;
            self.restart_counter = 0;

            0
        };

        let non_shared = key.len() - shared;

        self.buffer.write_varint(shared as usize)?;
        self.buffer.write_varint(non_shared as usize)?;
        self.buffer.write_varint(value.len())?;

        self.buffer.write_all(&key[shared..])?;
        self.buffer.write_all(value)?;

        if let Some(ref mut last_key) = &mut self.last_key {
            last_key.resize(shared, 0);
            last_key.extend_from_slice(&key[shared..]);
        } else {
            self.last_key = Some(key.to_owned());
        }

        self.restart_counter += 1;

        Ok(())
    }

    pub fn seal(&mut self) -> Vec<u8> {
        self.buffer.reserve(self.restarts.len() * 4 + 4);

        let num_restarts = self.restarts.len();
        for r in self.restarts.drain(..) {
            self.buffer.write_u32::<LittleEndian>(r as u32).unwrap();
        }

        self.buffer
            .write_u32::<LittleEndian>(num_restarts as u32)
            .unwrap();

        std::mem::replace(&mut self.buffer, Vec::new())
    }
}
