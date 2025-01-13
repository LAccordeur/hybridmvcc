use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    table::{self, TableFactory, TableFileRef},
    wal::WalOptions,
};

use serde::{de::DeserializeOwned, Serialize};

const DEFAULT_ROOT_PATH: &str = "kagidb";
const SSTABLE_FILE_EXT: &str = "sst";

#[derive(Clone)]
pub struct DbOptions<K, V> {
    pub(crate) root_path: PathBuf,
    pub(crate) wal_options: WalOptions,
    pub(crate) background_threads: usize,
    pub(crate) max_background_flushes: usize,
    pub(crate) max_background_compactions: usize,
    pub(crate) write_buffer_size: usize,
    pub(crate) gc_freeze_min_age: usize,
    pub(crate) num_levels: usize,
    pub(crate) max_open_files: usize,
    pub(crate) level0_file_num_compaction_trigger: usize,
    pub(crate) max_bytes_for_level_base: usize,
    pub(crate) max_bytes_for_level_multiplier: usize,
    pub(crate) target_file_size_base: usize,
    pub(crate) target_file_size_multiplier: usize,
    pub(crate) table_factory: Arc<dyn TableFactory<K, V>>,
}

impl<K, V> Default for DbOptions<K, V>
where
    K: 'static + Clone + Ord + Serialize + DeserializeOwned,
    V: 'static + Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self {
            root_path: PathBuf::from(DEFAULT_ROOT_PATH),
            wal_options: WalOptions::default(),
            background_threads: 24,
            max_background_flushes: 20,
            max_background_compactions: 4,
            write_buffer_size: 200000,
            gc_freeze_min_age: 100000,
            num_levels: 6,
            max_open_files: 100,
            level0_file_num_compaction_trigger: 4,
            max_bytes_for_level_base: 256 << 20,
            max_bytes_for_level_multiplier: 10,
            target_file_size_base: 256 << 20,
            target_file_size_multiplier: 1,
            table_factory: table::default_table_factory(),
        }
    }
}

macro_rules! setters {
    ($($name:ident : $t:ty),*) => {
        $(
            pub fn $name(mut self, to: $t) -> Self {
                self.$name = to;
                self
            }
        )*
    }
}

impl<K, V> DbOptions<K, V>
where
    K: 'static + Clone + Ord + Serialize + DeserializeOwned,
    V: 'static + Serialize + DeserializeOwned,
{
    pub fn new() -> Self {
        DbOptions::default()
    }
}

impl<K, V> DbOptions<K, V> {
    pub fn root_path<P: AsRef<Path>>(mut self, p: P) -> Self {
        self.root_path = p.as_ref().to_path_buf();
        self
    }

    pub fn wal_segment_capacity(mut self, segment_capacity: usize) -> Self {
        self.wal_options = self.wal_options.segment_capacity(segment_capacity);
        self
    }

    pub fn get_root_path(&self) -> PathBuf {
        self.root_path.clone()
    }

    pub fn get_storage_path(&self) -> PathBuf {
        let mut path = self.root_path.clone();
        path.push("base");
        path
    }

    pub fn get_wal_path(&self) -> PathBuf {
        let mut path = self.root_path.clone();
        path.push("wal");
        path
    }

    pub fn get_transaction_path(&self) -> PathBuf {
        let mut path = self.root_path.clone();
        path.push("txn");
        path
    }

    pub fn get_manifest_path(&self) -> PathBuf {
        let mut path = self.root_path.clone();
        path.push("kg_manifest");
        path
    }

    pub fn get_table_file_path(&self, file_ref: TableFileRef) -> PathBuf {
        let (path_id, seqnum) = file_ref.path_id_seqnum();
        let mut path = self.get_storage_path();

        path.push(format!("{:03}{:010}.{}", path_id, seqnum, SSTABLE_FILE_EXT));
        path
    }

    pub fn max_file_size_for_level(&self, level: usize) -> usize {
        self.target_file_size_base * self.target_file_size_multiplier.pow(level as u32)
    }

    setters!(
        background_threads: usize,
        max_background_flushes: usize,
        max_background_compactions: usize,
        write_buffer_size: usize,
        gc_freeze_min_age: usize,
        num_levels: usize,
        level0_file_num_compaction_trigger: usize,
        max_bytes_for_level_base: usize,
        max_bytes_for_level_multiplier: usize,
        target_file_size_base: usize,
        target_file_size_multiplier: usize
    );
}
