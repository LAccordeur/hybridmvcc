#![feature(specialization)]

#[cfg(test)]
extern crate tempfile;

mod block;
mod db;
mod memtable;
mod merging_iter;
mod options;
mod result;
mod storage;
mod table;
mod transaction;
mod utils;
mod version;
mod wal;

pub mod memdebug;

#[cfg(test)]
mod test_util;

pub use self::{
    db::Db,
    options::DbOptions,
    result::{Error, Result},
    table::BlockTableOptions,
    transaction::Transaction,
};
