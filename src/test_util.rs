#![cfg(test)]

use crate::{Db, DbOptions, Result};

use std::hash::Hash;

use serde::{de::DeserializeOwned, Serialize};

pub fn get_temp_options<K, V>() -> (DbOptions<K, V>, tempfile::TempDir)
where
    K: 'static + Clone + Ord + Hash + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
    V: 'static + Clone + Default + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
{
    let db_dir = tempfile::tempdir().unwrap();
    let options = DbOptions::default().root_path(&db_dir.path());

    (options, db_dir)
}

pub fn get_temp_db_with_options<K, V>(
    options: DbOptions<K, V>,
) -> Result<(Db<K, V>, tempfile::TempDir)>
where
    K: 'static + Clone + Ord + Hash + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
    V: 'static + Clone + Default + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
{
    let db_dir = tempfile::tempdir().unwrap();
    let options = options.root_path(&db_dir.path());
    let db = Db::open(options)?;

    Ok((db, db_dir))
}

pub fn get_temp_db<K, V>() -> Result<(Db<K, V>, tempfile::TempDir)>
where
    K: 'static + Clone + Ord + Hash + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
    V: 'static + Clone + Default + Serialize + DeserializeOwned + Sync + Send + std::fmt::Debug,
{
    get_temp_db_with_options(Default::default())
}
