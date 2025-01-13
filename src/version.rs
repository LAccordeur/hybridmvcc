use crate::{
    merging_iter::MergingIter,
    table::{
        OwningTableIterator, TableCache, TableFileMeta, TableFileRef, TableFileSet, TableIterator,
    },
    transaction::Timestamp,
    wal::{CheckpointLog, LogPointer, WalLogRecord},
    Db, DbOptions, Result,
};

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use serde::Serialize;
use tracing::{event, instrument, Level};

pub struct VersionEdit<K> {
    min_xip: Option<Timestamp>,
    add_files: Vec<(usize, TableFileMeta<K>)>,
    deleted: HashSet<(usize, TableFileRef)>,
    mark_compacted: Vec<(usize, TableFileRef)>,
}

impl<K> Default for VersionEdit<K> {
    fn default() -> Self {
        Self {
            min_xip: None,
            add_files: Vec::with_capacity(8),
            deleted: HashSet::new(),
            mark_compacted: Vec::with_capacity(8),
        }
    }
}

impl<K> VersionEdit<K> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_min_xip(&mut self, min_xip: Timestamp) {
        self.min_xip = Some(min_xip)
    }

    pub fn add_file(&mut self, level: usize, file: TableFileMeta<K>) {
        self.add_files.push((level, file));
    }

    pub fn delete_file(&mut self, level: usize, file: &TableFileMeta<K>) {
        self.deleted.insert((level, file.file_ref));
    }

    pub fn mark_as_being_compacted(&mut self, level: usize, file: &TableFileMeta<K>) {
        self.mark_compacted.push((level, file.file_ref));
    }
}

#[derive(Clone)]
pub struct Version<K, V> {
    table_cache: Arc<TableCache<K, V>>,
    files: TableFileSet<K>,
    compaction_score: Vec<f64>,
}

impl<K, V> Version<K, V> {
    #[cfg(test)]
    pub fn set_compaction_score(&mut self, score: Vec<f64>) {
        self.compaction_score = score;
    }
}

fn find_in_files<'a, K>(files: &'a [TableFileMeta<K>], key: &K) -> Option<&'a TableFileMeta<K>>
where
    K: Ord,
{
    let mut count = files.len();
    let mut first = 0;

    while count > 0 {
        let step = count >> 1;

        let mid = &files[first + step];
        if &mid.supremum < key {
            first += step + 1;
            count -= step + 1;
        } else {
            count = step;
        }
    }

    if first >= files.len() {
        None
    } else {
        Some(&files[first])
    }
}

impl<K, V> Version<K, V>
where
    K: Clone + Ord,
{
    pub fn new(num_levels: usize, table_cache: Arc<TableCache<K, V>>) -> Self {
        Self {
            table_cache,
            files: (0..num_levels).map(|_| Vec::new()).collect(),
            compaction_score: (0..num_levels).map(|_| 0.0).collect(),
        }
    }

    fn get_in_file(&self, f: &TableFileMeta<K>, key: &K) -> Result<Option<V>> {
        let table = self.table_cache.get_table(f.file_ref)?;

        table.get(key)
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        for (level, files) in self.files.iter().enumerate() {
            if level == 0 {
                // check all files in L0
                for f in files {
                    if key < &f.infimum || key > &f.supremum {
                        continue;
                    }

                    if let Some(value) = self.get_in_file(f, key)? {
                        return Ok(Some(value));
                    }
                }
            } else {
                if let Some(f) = find_in_files(files, key) {
                    if let Some(value) = self.get_in_file(f, key)? {
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }

    fn calculate_compaction_score(&mut self, opt: &DbOptions<K, V>) {
        self.compaction_score = self
            .files
            .iter()
            .enumerate()
            .map(|(level, files)| {
                if level == self.files.len() - 1 {
                    // never compact the highest level
                    return 0.0;
                }

                let num_files_score = if level == 0 {
                    files.iter().filter(|f| !f.being_compacted).count() as f64
                        / opt.level0_file_num_compaction_trigger as f64
                } else {
                    0.0
                };

                let level_size: usize = files
                    .iter()
                    .filter_map(|f| {
                        if f.being_compacted {
                            None
                        } else {
                            Some(f.size)
                        }
                    })
                    .sum();
                let max_size = opt.max_bytes_for_level_base
                    * opt.max_bytes_for_level_multiplier.pow(level as u32);
                let size_score = level_size as f64 / max_size as f64;

                if level > 0 && size_score > num_files_score {
                    size_score
                } else {
                    num_files_score
                }
            })
            .collect();
    }

    fn get_overlapping_inputs(
        &self,
        level: usize,
        prev_files: &Vec<&TableFileMeta<K>>,
        infimum: &K,
        supremum: &K,
    ) -> Vec<&TableFileMeta<K>> {
        if level == 0 {
            let max_ts = prev_files.iter().map(|f| f.min_xip).max().unwrap();

            let (mut inputs, _) = self.files[level].iter().rev().fold(
                (Vec::new(), false),
                |(mut inputs, done), f| {
                    if done {
                        (inputs, true)
                    } else if f.min_xip <= max_ts {
                        // files earlier than the last file in the selection must be included
                        inputs.push(f);
                        (inputs, false)
                    } else if &f.supremum >= infimum && &f.infimum <= supremum && !f.being_compacted
                    {
                        // overlap
                        inputs.push(f);
                        (inputs, false)
                    } else {
                        // break
                        (inputs, true)
                    }
                },
            );

            inputs.reverse();
            inputs
        } else {
            self.files[level]
                .iter()
                .filter(|f| &f.supremum >= infimum && &f.infimum <= supremum)
                .collect()
        }
    }

    fn expand_inputs<'a>(
        &'a self,
        level: usize,
        mut files: Vec<&'a TableFileMeta<K>>,
    ) -> Option<Vec<&'a TableFileMeta<K>>> {
        loop {
            // grow selection incrementally
            let old_len = files.len();

            if old_len == 0 {
                break;
            }

            let (infimum, supremum) = get_range(level, &files);
            files = self.get_overlapping_inputs(level, &files, infimum, supremum);

            if files.len() <= old_len {
                break;
            }
        }

        if files.iter().any(|f| f.being_compacted) {
            None
        } else {
            Some(files)
        }
    }

    fn try_pick_files_to_compact(
        &self,
        level: usize,
    ) -> Option<(Vec<TableFileMeta<K>>, Vec<TableFileMeta<K>>)> {
        assert!(level < self.files.len());

        let output_level = level + 1;
        assert!(output_level < self.files.len());

        let mut level_files = self.files[level]
            .iter()
            .filter(|f| !f.being_compacted)
            .collect::<Vec<&TableFileMeta<K>>>();

        level_files.sort_by(|a, b| {
            if level == 0 {
                // sort by timestamp in descending order
                a.min_xip.cmp(&b.min_xip)
            } else {
                // sort by file size in descending order
                a.size.cmp(&b.size).reverse()
            }
        });

        for f in level_files {
            let expanded_start_level_inputs = self.expand_inputs(level, vec![f]);

            let start_level_inputs = match expanded_start_level_inputs {
                Some(res) => res,
                _ => {
                    if level == 0 {
                        // no need to check later files in level 0
                        break;
                    } else {
                        continue;
                    }
                }
            };

            let (infimum, supremum) = get_range(level, &start_level_inputs);
            let expanded_output_level_inputs = self.expand_inputs(
                output_level,
                self.get_overlapping_inputs(output_level, &Vec::new(), infimum, supremum),
            );

            if let Some(output_level_inputs) = expanded_output_level_inputs {
                return Some((
                    start_level_inputs.into_iter().cloned().collect(),
                    output_level_inputs.into_iter().cloned().collect(),
                ));
            }

            if level == 0 {
                break;
            }
        }

        None
    }
}

pub struct Compaction<K> {
    level: usize,
    inputs: [Vec<TableFileMeta<K>>; 2],
    edit: VersionEdit<K>,
}

impl<K> Compaction<K> {
    fn new(level: usize, inputs: [Vec<TableFileMeta<K>>; 2]) -> Self {
        Self {
            level,
            inputs,
            edit: Default::default(),
        }
    }

    pub fn level(&self) -> usize {
        self.level
    }

    pub fn output_level(&self) -> usize {
        self.level + 1
    }

    pub fn num_inputs(&self, idx: usize) -> usize {
        assert!(idx < 2);
        self.inputs[idx].len()
    }

    pub fn edit(&mut self) -> &mut VersionEdit<K> {
        &mut self.edit
    }

    pub fn delete_inputs(&mut self) {
        for (i, files) in self.inputs.iter().enumerate() {
            for f in files.iter() {
                self.edit.delete_file(self.level + i, &f);
            }
        }
    }
}

pub struct VersionSet<K, V> {
    options: DbOptions<K, V>,
    table_cache: Arc<TableCache<K, V>>,

    next_file_num: AtomicUsize,
    min_xip: Timestamp,
    min_recovery_point: LogPointer,

    current: Arc<Version<K, V>>,
    level0_compactions_in_progress: bool,
}

impl<K, V> VersionSet<K, V> {
    pub fn current(&self) -> Arc<Version<K, V>> {
        self.current.clone()
    }

    pub fn new_file_number(&self) -> usize {
        self.next_file_num.fetch_add(1, Ordering::Relaxed)
    }

    pub fn set_next_file_number(&self, next_file_num: usize) {
        self.next_file_num.store(next_file_num, Ordering::Relaxed);
    }
}

impl<K, V> VersionSet<K, V>
where
    K: 'static + Clone + Ord + Sync + Send + Serialize,
    V: 'static + Clone + Sync + Send,
{
    pub fn new(options: DbOptions<K, V>, table_cache: Arc<TableCache<K, V>>) -> Self {
        let num_levels = options.num_levels;

        Self {
            options,
            table_cache: table_cache.clone(),

            next_file_num: AtomicUsize::new(2),
            min_xip: Timestamp::from(0),
            min_recovery_point: LogPointer::default(),

            current: Arc::new(Version::new(num_levels, table_cache)),
            level0_compactions_in_progress: false,
        }
    }

    #[instrument(skip(self, db, edit))]
    pub fn log_and_apply(
        &mut self,
        db: &Db<K, V>,
        edit: VersionEdit<K>,
        min_recovery_point: LogPointer,
    ) -> Result<()> {
        let wal = db.get_wal();
        let new_min_xip = edit.min_xip.unwrap_or(self.min_xip);
        assert!(new_min_xip >= self.min_xip);

        let mut new_version = Version::new(self.options.num_levels, self.table_cache.clone());

        {
            let mut builder = VersionBuilder::new(self.options.num_levels);
            builder.apply(&edit);
            builder.save_to(&self.current, &mut new_version);
        }
        new_version.calculate_compaction_score(&self.options);

        // write WAL
        let next_file_num = self.next_file_num.load(Ordering::Relaxed);
        let checkpoint_log = WalLogRecord::create_checkpoint_log(
            new_min_xip,
            min_recovery_point,
            next_file_num,
            new_version.files.clone(),
        );
        // use a default timestamp so that this record is never replayed
        let (checkpoint_pos, checkpoint_lsn) =
            wal.append_record(Timestamp::default(), checkpoint_log)?;
        // let the checkpoint hit disk
        wal.flush(Some(checkpoint_lsn))?;
        event!(
            Level::INFO,
            "checkpoint record flushed, min recovery point = {}, next file number = {}",
            min_recovery_point,
            next_file_num
        );

        // write manifest
        db.create_checkpoint(new_min_xip, checkpoint_pos)?;

        event!(
            Level::INFO,
            "installing new version, compaction score = {:?}",
            new_version.compaction_score
        );
        self.set_current(new_version);
        self.min_xip = new_min_xip;
        self.min_recovery_point = min_recovery_point;

        Ok(())
    }

    pub fn set_current(&mut self, version: Version<K, V>) {
        self.current = Arc::new(version);
    }

    fn register_compaction(&mut self, c: &Compaction<K>) {
        let mut edit = VersionEdit::new();

        if c.level() == 0 {
            self.level0_compactions_in_progress = true;
        }

        for f in &c.inputs[0] {
            edit.mark_as_being_compacted(c.level, f);
        }
        for f in &c.inputs[1] {
            edit.mark_as_being_compacted(c.level + 1, f);
        }

        let mut new_version = Version::new(self.options.num_levels, self.table_cache.clone());

        {
            let mut builder = VersionBuilder::new(self.options.num_levels);
            builder.apply(&edit);
            builder.save_to(&self.current, &mut new_version);
        }
        new_version.calculate_compaction_score(&self.options);

        self.set_current(new_version);
    }

    pub fn pick_compaction(&mut self) -> Result<Option<Compaction<K>>> {
        let (start_level, start_level_inputs, output_level_inputs) = match self
            .current
            .compaction_score
            .iter()
            .enumerate()
            .filter_map(|(level, score)| {
                if self.level0_compactions_in_progress && level == 0 {
                    // disallow concurrent level-0 compactions
                    None
                } else if score < &1.0 {
                    // no need to compact
                    None
                } else {
                    self.current
                        .try_pick_files_to_compact(level)
                        .map(|(ifs, ofs)| (level, ifs, ofs))
                }
            })
            .next()
        {
            Some(res) => res,
            _ => return Ok(None),
        };

        let c = Compaction::new(start_level, [start_level_inputs, output_level_inputs]);
        self.register_compaction(&c);

        Ok(Some(c))
    }

    pub fn make_input_iterator(&self, c: &Compaction<K>) -> Result<Box<dyn TableIterator<K, V>>> {
        let mut iters: Vec<(Timestamp, Box<dyn TableIterator<K, V>>)> = Vec::new();

        for (i, inputs) in c.inputs.iter().enumerate() {
            let level = c.level() + i;

            if c.num_inputs(i) == 0 {
                continue;
            } else {
                for f in inputs {
                    let ts = if level == 0 {
                        f.min_xip
                    } else {
                        Timestamp::default()
                    };

                    let table = self.table_cache.get_table(f.file_ref)?;
                    iters.push((ts, Box::new(OwningTableIterator::from_table(table, true))));
                }
            }
        }

        let merging_iter = MergingIter::new(iters);
        Ok(Box::new(merging_iter))
    }

    pub fn finalize_compaction(&mut self, db: &Db<K, V>, c: Compaction<K>) -> Result<()> {
        if c.level() == 0 {
            self.level0_compactions_in_progress = false;
        }

        self.log_and_apply(db, c.edit, self.min_recovery_point)
    }

    pub fn recover(&mut self, checkpoint: CheckpointLog<K>) {
        self.min_xip = checkpoint.min_xip;
        self.set_next_file_number(checkpoint.next_file_num);
        self.min_recovery_point = checkpoint.min_recovery_point;

        let mut version = Version::new(self.options.num_levels, self.table_cache.clone());
        version.files = checkpoint.file_set;

        version.calculate_compaction_score(&self.options);
        self.set_current(version);
    }

    pub fn dump_current(&self) {
        println!(
            "Next file num: {}, min xip: {}, min recovery point: {}",
            self.next_file_num.load(Ordering::SeqCst),
            self.min_xip,
            self.min_recovery_point
        );

        println!("Compaction score: {:?}", self.current.compaction_score);
        for (level, files) in self.current.files.iter().enumerate() {
            if files.is_empty() {
                continue;
            }

            println!("Level {}", level);
            for f in files {
                println!("  {:?}@{:?}, size {} bytes", f.file_ref, f.min_xip, f.size);
            }
        }
    }
}

struct VersionBuilder<K> {
    added: Vec<Vec<TableFileMeta<K>>>,
    deleted: HashSet<(usize, TableFileRef)>,
    mark_compacted: Vec<HashSet<TableFileRef>>,
}

impl<K> VersionBuilder<K>
where
    K: Clone + Ord,
{
    fn new(num_levels: usize) -> Self {
        Self {
            added: (0..num_levels).map(|_| Vec::new()).collect(),
            deleted: HashSet::new(),
            mark_compacted: (0..num_levels).map(|_| HashSet::new()).collect(),
        }
    }

    fn apply(&mut self, edit: &VersionEdit<K>) {
        for (level, f) in &edit.add_files {
            assert!(*level < self.added.len());

            self.added[*level].push(f.clone());
        }

        for (level, file_ref) in &edit.mark_compacted {
            assert!(*level < self.mark_compacted.len());

            self.mark_compacted[*level].insert(*file_ref);
        }

        self.deleted = edit.deleted.clone();
    }

    fn save_to<V>(&mut self, base: &Version<K, V>, v: &mut Version<K, V>) {
        assert_eq!(base.files.len(), self.added.len());
        assert_eq!(v.files.len(), self.added.len());

        let deleted = std::mem::replace(&mut self.deleted, Default::default());

        for (level, added) in self.added.iter_mut().enumerate() {
            added.sort_by(|a, b| compare_level_files(level, a, b));
            // base files should be sorted

            let added_files = added.clone();
            let base_files = base.files[level].clone();

            let added_iter = added_files.into_iter();
            let base_iter = base_files.into_iter();
            let merged = merge_iters(added_iter, base_iter, |a, b| {
                compare_level_files(level, a, b)
            })
            .into_iter()
            .filter(|f| !deleted.contains(&(level, f.file_ref)))
            .collect();

            v.files[level] = merged;
        }

        for (level, files) in v.files.iter_mut().enumerate() {
            for f in files.iter_mut() {
                if self.mark_compacted[level].contains(&f.file_ref) {
                    f.being_compacted = true;
                }
            }
        }
    }
}

fn merge_iters<
    Item,
    C: Fn(&Item, &Item) -> std::cmp::Ordering,
    I: Iterator<Item = Item>,
    J: Iterator<Item = Item>,
>(
    mut iter_a: I,
    mut iter_b: J,
    cmp: C,
) -> Vec<Item> {
    let mut a_next = iter_a.next();
    let mut b_next = iter_b.next();

    let mut out = Vec::new();

    loop {
        match (a_next, b_next) {
            (Some(a), Some(b)) => {
                let ord = cmp(&a, &b);
                if ord == std::cmp::Ordering::Less {
                    out.push(a);
                    a_next = iter_a.next();
                    b_next = Some(b);
                } else {
                    out.push(b);
                    a_next = Some(a);
                    b_next = iter_b.next();
                }
            }
            (Some(a), _) => {
                out.push(a);
                a_next = iter_a.next();
                b_next = None;
            }
            (_, Some(b)) => {
                out.push(b);
                a_next = None;
                b_next = iter_b.next();
            }
            (None, None) => {
                break;
            }
        }
    }

    out
}

fn compare_level_files<K>(
    level: usize,
    a: &TableFileMeta<K>,
    b: &TableFileMeta<K>,
) -> std::cmp::Ordering
where
    K: Ord,
{
    if level == 0 {
        // sort level 0 by min_xip in descending order
        b.min_xip.cmp(&a.min_xip)
    } else {
        // sort other levels by infimum
        a.infimum.cmp(&b.infimum)
    }
}

fn get_range<'a, K: Ord>(level: usize, files: &Vec<&'a TableFileMeta<K>>) -> (&'a K, &'a K) {
    if level == 0 {
        let mut infimum = None;
        let mut supremum = None;

        for f in files {
            if let Some(inf) = infimum {
                if &f.infimum < inf {
                    infimum = Some(&f.infimum);
                }
            } else {
                infimum = Some(&f.infimum);
            }

            if let Some(sup) = supremum {
                if sup < &f.supremum {
                    supremum = Some(&f.supremum);
                }
            } else {
                supremum = Some(&f.supremum);
            }
        }

        (infimum.unwrap(), supremum.unwrap())
    } else {
        (&files[0].infimum, &files[files.len() - 1].supremum)
    }
}

#[cfg(test)]
pub mod test_util {
    use super::*;
    use crate::{table::TableFileRef, test_util::get_temp_options, Error};
    use std::fs::{DirBuilder, OpenOptions};

    fn write_table<K, V>(
        options: &DbOptions<K, V>,
        contents: &[(K, V)],
        min_xip: Timestamp,
        file_num: usize,
    ) -> Result<TableFileMeta<K>>
    where
        K: Clone,
    {
        assert!(contents.len() > 0);

        let file_ref = TableFileRef::new(0, file_num);
        let mut file = OpenOptions::new()
            .read(false)
            .write(true)
            .create(true)
            .open(options.get_table_file_path(file_ref))?;

        let mut first_key = None;
        let mut last_key = None;
        {
            // scope for table builder
            let mut builder = options.table_factory.create_table_builder(&mut file);

            for (key, value) in contents {
                if first_key.is_none() {
                    first_key = Some(key.clone());
                }

                last_key = Some(key);

                builder.add(key, value)?;
            }

            builder.finish()?;
        }

        let size = file.metadata()?.len();

        let meta = match (first_key, last_key) {
            (Some(first_key), Some(last_key)) => TableFileMeta::new(
                file_ref,
                size as usize,
                min_xip,
                first_key,
                last_key.clone(),
            ),
            _ => unreachable!(),
        };

        Ok(meta)
    }

    macro_rules! define_table {
        ($name:ident, $($key: tt => $value: tt),* $(,)?) => {
            let $name: &[(String, String)] = &[
                $(
                    ($key.to_owned(), $value.to_owned()),
                )*
            ];
        }
    }

    pub fn make_version() -> Result<(
        Version<String, String>,
        DbOptions<String, String>,
        tempfile::TempDir,
    )> {
        let (options, db_dir) = get_temp_options::<String, String>();
        let table_cache = Arc::new(TableCache::new(options.clone()));

        let storage_path = options.get_storage_path();

        if !storage_path.exists() {
            DirBuilder::new().recursive(true).create(&storage_path)?;
        } else if !storage_path.is_dir() {
            return Err(Error::WrongObjectType(format!(
                "'{}' exists but is not a directory",
                storage_path.display()
            )));
        }

        // level 0
        define_table!(
            f3,
            "caa" => "val1",
            "cab" => "val2",
        );
        let t3 = write_table(&options, f3, Timestamp::from(30), 3)?;

        define_table!(
            f2,
            "aac" => "val4",
            "aax" => "val2",
            "aba" => "val5",
            "bab" => "val4",
            "bba" => "val5",
        );
        let t2 = write_table(&options, f2, Timestamp::from(26), 2)?;

        define_table!(
            f1,
            "aaa" => "val2",
            "aab" => "val2",
            "aac" => "val3",
            "aba" => "val4",
        );
        let t1 = write_table(&options, f1, Timestamp::from(22), 1)?;

        // level 1
        define_table!(
            f4,
            "aaa" => "val1",
            "cab" => "val2",
            "cba" => "val3",
        );
        let t4 = write_table(&options, f4, Timestamp::default(), 4)?;

        define_table!(
            f5,
            "daa" => "val2",
            "dab" => "val2",
            "dba" => "val3",
            "dbb" => "val4",
        );
        let t5 = write_table(&options, f5, Timestamp::default(), 5)?;

        define_table!(
            f6,
            "eaa" => "val1",
            "eab" => "val2",
            "fab" => "val3",
        );
        let t6 = write_table(&options, f6, Timestamp::default(), 6)?;

        // level 2
        define_table!(
            f7,
            "cab" => "val1",
            "daa" => "val2",
            "fab" => "val3",
            "fba" => "val4",
        );
        let t7 = write_table(&options, f7, Timestamp::default(), 7)?;

        define_table!(
            f8,
            "gaa" => "val1",
            "gab" => "val2",
            "gba" => "val3",
            "gca" => "val4",
            "gda" => "val5",
        );
        let t8 = write_table(&options, f8, Timestamp::default(), 8)?;

        // level 3
        define_table!(
            f9,
            "haa" => "val1",
            "hba" => "val2",
        );
        let t9 = write_table(&options, f9, Timestamp::default(), 9)?;

        define_table!(
            f10,
            "iaa" => "val1",
            "iba" => "val2",
        );
        let t10 = write_table(&options, f10, Timestamp::default(), 10)?;

        let mut version = Version::new(options.num_levels, table_cache);
        version.files[0] = vec![t3, t2, t1];
        version.files[1] = vec![t4, t5, t6];
        version.files[2] = vec![t7, t8];
        version.files[3] = vec![t9, t10];

        Ok((version, options, db_dir))
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::*;
    use super::*;

    #[test]
    fn can_pick_compaction_l0() {
        let (mut version, options, _db_dir) = make_version().unwrap();
        let table_cache = Arc::new(TableCache::new(options.clone()));
        let mut vset = VersionSet::new(options, table_cache);

        version.compaction_score = vec![2.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        vset.set_next_file_number(20);
        vset.set_current(version);

        let c = vset.pick_compaction().unwrap().unwrap();
        assert_eq!(c.level, 0);
        assert_eq!(c.inputs[0].len(), 2);
        assert_eq!(c.inputs[1].len(), 1);
        assert!(vset.current.files[0][1].being_compacted);
        assert!(vset.current.files[0][2].being_compacted);
        assert!(vset.current.files[1][0].being_compacted);
    }

    #[test]
    fn can_pick_compaction_l1() {
        let (mut version, options, _db_dir) = make_version().unwrap();
        let table_cache = Arc::new(TableCache::new(options.clone()));
        let mut vset = VersionSet::new(options, table_cache);

        version.compaction_score = vec![0.0, 2.0, 0.0, 0.0, 0.0, 0.0];
        vset.set_next_file_number(20);
        vset.set_current(version);

        let c = vset.pick_compaction().unwrap().unwrap();
        assert_eq!(c.level, 1);
        assert_eq!(c.inputs[0].len(), 1);
        assert_eq!(c.inputs[1].len(), 1);
        assert!(vset.current.files[1][1].being_compacted);
        assert!(vset.current.files[2][0].being_compacted);
    }
}
