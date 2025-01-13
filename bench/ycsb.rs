extern crate indicatif;
extern crate tempfile;
extern crate ycsb_rs;

use kagi::{Db, DbOptions, Error, Result, Transaction};

use clap::Parser;
use hdrhistogram::{sync::Recorder, Histogram};
use indicatif::{ProgressBar, ProgressStyle};
use tracing_subscriber;
use ycsb_rs::{CoreWorkload, Operation, WorkloadSpec};

use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::prelude::*,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Instant,
};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    #[clap(short, long, required = true)]
    workload: String,
    #[clap(short = 'n', long, default_value = "1")]
    ncpus: usize,
    #[clap(short = 'o', long, default_value = "results")]
    outputdir: String,
    #[clap(short = 'r', long)]
    rootdir: Option<String>,
    #[clap(short = 't', long)]
    runonly: bool,
}

struct Client<'a> {
    db: &'a Db<String, String>,
    workload: &'a CoreWorkload,
}

fn serialize_values<P: AsRef<str>>(values: &[(P, P)]) -> String {
    values
        .iter()
        .map(|(name, val)| [name.as_ref(), "=", val.as_ref()].concat())
        .collect::<Vec<_>>()
        .join(",")
}

fn deserialize_values(value: &str) -> HashMap<&str, &str> {
    value
        .split(',')
        .map(|pair| {
            let mut elts = pair.split('=');

            (elts.next().unwrap(), elts.next().unwrap())
        })
        .collect()
}

impl Client<'_> {
    fn read_txn(
        &self,
        txn: &mut Transaction<String, String>,
        recorder: &mut Recorder<u32>,
    ) -> Result<()> {
        let key = self.workload.next_transaction_key();

        // let fields = if self.workload.read_all_fields() {
        //     None
        // } else {
        //     Some(vec![self.workload.next_field_name()])
        // };

        let start = Instant::now();

        self.db.get(txn, &key)?;

        let elapsed = start.elapsed();
        recorder.record(elapsed.as_micros() as u64).unwrap();

        Ok(())
    }

    fn update_txn<'t>(
        &'t self,
        txn: &mut Transaction<'t, String, String>,
        recorder: &mut Recorder<u32>,
    ) -> Result<()> {
        let key = self.workload.next_transaction_key();

        let new_values = if self.workload.write_all_fields() {
            self.workload.build_values()
        } else {
            vec![self.workload.build_update()]
        };

        let start = Instant::now();

        self.db.update(txn, &key, &|current_value| {
            let mut current_values = deserialize_values(current_value);

            for (field, value) in &new_values {
                current_values.insert(field, value);
            }

            serialize_values(&current_values.into_iter().collect::<Vec<_>>())
        })?;

        let elapsed = start.elapsed();
        recorder.record(elapsed.as_micros() as u64).unwrap();

        Ok(())

        // let current_value = self.db.get(txn, &key)?;
        // assert!(current_value.is_some());

        // let value = match current_value {
        //     Some(current_value) => {
        //         let mut current_values = deserialize_values(current_value);

        //         for (field, value) in &new_values {
        //             current_values.insert(field, value);
        //         }

        //         serialize_values(&current_values.into_iter().collect::<Vec<_>>())
        //     }
        //     None => serialize_values(&new_values),
        // };

        // self.db.put(txn, key.to_owned(), value).map(|_| ())
    }

    fn insert_txn<'t>(
        &'t self,
        txn: &mut Transaction<'t, String, String>,
        recorder: &mut Recorder<u32>,
    ) -> Result<()> {
        let key = self.workload.next_insert_sequence();
        let values = self.workload.build_values();

        let value = serialize_values(&values);

        let start = Instant::now();

        self.db.put(txn, key, value)?;

        let elapsed = start.elapsed();
        recorder.record(elapsed.as_micros() as u64).unwrap();

        Ok(())
    }

    fn scan_txn(&self, _txn: &mut Transaction<String, String>) -> Result<()> {
        // let table = self.workload.next_table();
        // let key = self.workload.next_transaction_key();
        // let length = self.workload.next_scan_length();

        // let fields = if self.workload.read_all_fields() {
        //     None
        // } else {
        //     Some(vec![self.workload.next_field_name()])
        // };
        // self.db.scan(txn, &table, &key, length, fields).map(|_| ())

        Ok(())
    }
}

fn load_batch<'t>(
    db: &'t Db<String, String>,
    txn: &mut Transaction<'t, String, String>,
    batch: &[(String, String, Vec<(String, String)>)],
    recorder: &mut Recorder<u32>,
) -> Result<usize> {
    let batch_size = batch.len();

    for (_, key, values) in batch {
        let value = serialize_values(values);

        let start = Instant::now();
        db.put(txn, key.to_owned(), value)?;

        let elapsed = start.elapsed();
        recorder.record(elapsed.as_micros() as u64).unwrap();
    }

    Ok(batch_size)
}

fn load_db(
    db: &Db<String, String>,
    workload: &CoreWorkload,
    num_ops: usize,
    batch_size: usize,
    mut recorder: Recorder<u32>,
    mut commit_recorder: Recorder<u32>,
    pb: &ProgressBar,
    fast_commit: bool,
) -> Result<(usize, HashMap<u64, u64>)> {
    let mut total_count = 0;
    let mut tx_latency = HashMap::<u64, u64>::new();

    for b in (0..num_ops).step_by(batch_size) {
        let count = std::cmp::min(batch_size, num_ops - b);

        let batch = (0..count)
            .map(|_| {
                (
                    workload.next_table(),
                    workload.next_sequence_key(),
                    workload.build_values(),
                )
            })
            .collect::<Vec<_>>();

        loop {
            let mut txn = if fast_commit {
                db.start_transaction_fast_commit()?
            } else {
                db.start_transaction()?
            };

            match load_batch(db, &mut txn, &batch, &mut recorder) {
                Ok(count) => {
                    let txid = txn.start_timestamp().raw_timestamp();
                    total_count += count;
                    pb.inc(count as u64);
                    let start = Instant::now();
                    db.commit_transaction(txn)?;
                    let elapsed = start.elapsed().as_micros() as u64;
                    commit_recorder.record(elapsed).unwrap();
                    tx_latency.insert(txid, elapsed);
                    break;
                }
                Err(Error::TransactionAborted(_)) => {
                    db.abort_transaction(txn)?;
                    continue;
                }
                Err(e) => {
                    db.abort_transaction(txn)?;
                    return Err(e);
                }
            }
        }
    }

    Ok((total_count, tx_latency))
}

fn bench_txn(
    db: &Db<String, String>,
    workload: &CoreWorkload,
    num_ops: usize,
    mut recorders: Vec<Recorder<u32>>,
    counters: &Vec<AtomicUsize>,
    pb: &ProgressBar,
) -> Result<usize> {
    let client = Client { db, workload };
    let mut total_count = 0;

    for _ in 0..num_ops {
        let op = workload.next_operation();

        loop {
            let mut txn = db.start_transaction()?;

            let index = match op {
                Operation::Read => 0,
                Operation::Update => 1,
                Operation::Insert => 2,
                Operation::Scan => 0,
                Operation::ReadModifyWrite => 1,
            };

            let res = match op {
                Operation::Read => client.read_txn(&mut txn, &mut recorders[index]),
                Operation::Update => client.update_txn(&mut txn, &mut recorders[index]),
                Operation::Insert => client.insert_txn(&mut txn, &mut recorders[index]),
                Operation::Scan => client.scan_txn(&mut txn),
                Operation::ReadModifyWrite => client.update_txn(&mut txn, &mut recorders[index]),
            };

            match res {
                Ok(_) => {
                    total_count += 1;
                    pb.inc(1);
                    db.commit_transaction(txn)?;
                    break;
                }
                Err(Error::TransactionAborted(_)) => {
                    db.abort_transaction(txn)?;
                    counters[index].fetch_add(1, Ordering::SeqCst);
                    continue;
                }
                err => {
                    db.abort_transaction(txn)?;
                    return err.map(|_| 0);
                }
            }
        }
    }

    Ok(total_count)
}

fn quantiles<W: Write>(
    hist: &Histogram<u32>,
    mut writer: W,
    quantile_precision: usize,
    ticks_per_half: u32,
) -> Result<()> {
    writer.write_all(
        format!(
            "{},{},{},{},{}\n",
            "Value", "Percentile", "QuantileIteration", "TotalCount", "1/(1-Quantile)",
        )
        .as_ref(),
    )?;
    let mut sum = 0;
    for v in hist.iter_quantiles(ticks_per_half) {
        sum += v.count_since_last_iteration();
        if v.quantile_iterated_to() < 1.0 {
            writer.write_all(
                format!(
                    "{:12},{:1.*},{:1.*},{:10},{:14.2}\n",
                    v.value_iterated_to(),
                    quantile_precision,
                    v.quantile(),
                    quantile_precision,
                    v.quantile_iterated_to(),
                    sum,
                    1_f64 / (1_f64 - v.quantile_iterated_to())
                )
                .as_ref(),
            )?;
        } else {
            writer.write_all(
                format!(
                    "{:12},{:1.*},{:1.*},{:10},{:>14}\n",
                    v.value_iterated_to(),
                    quantile_precision,
                    v.quantile(),
                    quantile_precision,
                    v.quantile_iterated_to(),
                    sum,
                    "inf",
                )
                .as_ref(),
            )?;
        }
    }

    fn write_extra_data<T1: std::fmt::Display, T2: std::fmt::Display, W: Write>(
        writer: &mut W,
        label1: &str,
        data1: T1,
        label2: &str,
        data2: T2,
    ) -> Result<()> {
        writer.write_all(
            format!(
                "#[{:10} = {:12.2}, {:14} = {:12.2}]\n",
                label1, data1, label2, data2
            )
            .as_ref(),
        )?;

        Ok(())
    }

    write_extra_data(
        &mut writer,
        "Mean",
        hist.mean(),
        "StdDeviation",
        hist.stdev(),
    )?;
    write_extra_data(&mut writer, "Max", hist.max(), "Total count", hist.len())?;
    write_extra_data(
        &mut writer,
        "Buckets",
        hist.buckets(),
        "SubBuckets",
        hist.distinct_values(),
    )?;

    Ok(())
}

fn print_histogram(hist: &Histogram<u32>, name: Option<&str>) {
    if let Some(name) = name {
        eprint!("[{}] ", name);
    }

    eprintln!(
        "Count={}, Max={:.2}, Min={:.2}, Avg={:.2}, 90={:.2}, 99={:.2}, 99.9={:.2}, 99.99={:.2}",
        hist.len(),
        hist.max(),
        hist.min(),
        hist.mean(),
        hist.value_at_percentile(90.0),
        hist.value_at_percentile(99.0),
        hist.value_at_percentile(99.9),
        hist.value_at_percentile(99.99),
    );
}

fn save_quantiles<P: AsRef<Path> + Clone>(
    hist: &Histogram<u32>,
    output_dir: P,
    workload_name: &str,
    name: &str,
    nr_threads: usize,
) -> Result<()> {
    let mut path_buf = PathBuf::new();
    path_buf.push(output_dir.clone());
    path_buf.push(format!(
        "mvcc_{}_{}_t{}.hist",
        workload_name, name, nr_threads
    ));
    let output_file = File::create(path_buf)?;
    quantiles(hist, output_file, 4, 20)
}

pub fn run_ycsb<P: AsRef<Path> + Clone>(
    db: &Db<String, String>,
    workload_path: P,
    nr_threads: usize,
    output_dir: P,
    run_only: bool,
) -> Result<()> {
    let workload_name = workload_path.as_ref().file_stem().unwrap().to_owned();
    let mut file = File::open(workload_path)?;
    let mut json_data = String::new();
    file.read_to_string(&mut json_data)?;

    std::fs::create_dir_all(output_dir.clone())?;

    let workload_spec = serde_json::from_str::<WorkloadSpec>(&json_data)
        .map_err(|_| Error::InvalidArgument("unrecognized workload spec format".to_owned()))?;
    let record_count = workload_spec.get_record_count();
    let op_count = workload_spec.get_operation_count();
    let workload = Arc::new(
        CoreWorkload::new(workload_spec)
            .map_err(|_| Error::InvalidArgument("cannot create workload".to_owned()))?,
    );

    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:60.cyan/blue}] {pos:>7}/{len:7} {per_sec}")
        .progress_chars("##-");

    if !run_only {
        let nr_load_threads = 10;
        let batch_size = 100;
        let fast_commit = false;

        let pb = Arc::new(ProgressBar::new(
            (record_count / nr_load_threads * nr_load_threads) as u64,
        ));
        pb.set_style(sty.clone());
        pb.set_draw_delta(record_count as u64 / 1000);

        let mut threads = Vec::new();

        let mut histogram = Histogram::<u32>::new(3).unwrap().into_sync();
        let mut histogram_commit = Histogram::<u32>::new(3).unwrap().into_sync();

        for _ in 0..nr_load_threads {
            let db = db.clone();
            let workload = workload.clone();
            let pb = pb.clone();
            let recorder = histogram.recorder();
            let commit_recorder = histogram_commit.recorder();

            threads.push(thread::spawn(move || {
                load_db(
                    &db,
                    &workload,
                    record_count / nr_load_threads,
                    batch_size,
                    recorder,
                    commit_recorder,
                    &*pb,
                    fast_commit,
                )
            }));
        }

        let mut tx_latency = BTreeMap::<u64, u64>::new();

        let mut loaded: usize = 0;
        for t in threads {
            let (count, latency_map) = t.join().unwrap()?;

            for (k, v) in latency_map {
                tx_latency.insert(k, v);
            }

            loaded += count;
        }

        {
            let mut path_buf = PathBuf::new();
            path_buf.push(output_dir.clone());
            path_buf.push(format!(
                "mvcc_{}_COMMIT_LATENCY_t{}_bs{}{}.csv",
                workload_name.to_str().unwrap(),
                nr_threads,
                batch_size,
                if fast_commit { "_fc" } else { "" },
            ));
            let mut output_file = File::create(path_buf)?;

            writeln!(&mut output_file, "txid,latency")?;
            for (k, v) in tx_latency {
                writeln!(&mut output_file, "{},{}", k, v)?;
            }
        }

        histogram.refresh();
        histogram_commit.refresh();
        pb.finish();

        eprintln!("{} records loaded", loaded);
        print_histogram(&histogram, Some("PUT"));
        print_histogram(&histogram_commit, Some("COMMIT"));

        save_quantiles(
            &histogram,
            output_dir.clone(),
            workload_name.to_str().unwrap(),
            "LOAD",
            nr_load_threads,
        )?;
    }

    {
        let pb = Arc::new(ProgressBar::new(
            (op_count / nr_threads * nr_threads) as u64,
        ));
        pb.set_style(sty);
        pb.set_draw_delta(record_count as u64 / 1000);

        let start = Instant::now();

        let mut threads = Vec::new();

        let measurements = (0..3)
            .map(|_| Histogram::<u32>::new(3).unwrap().into_sync())
            .collect::<Vec<_>>();
        let abort_counters = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());

        for _ in 0..nr_threads {
            let db = db.clone();
            let workload = workload.clone();
            let pb = pb.clone();
            let recorders = measurements
                .iter()
                .map(|hist| hist.recorder())
                .collect::<Vec<_>>();
            let counters = abort_counters.clone();

            threads.push(thread::spawn(move || {
                bench_txn(
                    &db,
                    &workload,
                    op_count / nr_threads,
                    recorders,
                    &counters,
                    &*pb,
                )
            }));
        }

        let nr_txns: usize = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum();

        let elapsed = start.elapsed();

        pb.finish();

        eprintln!("{} transactions in {:?}", nr_txns, elapsed);
        println!(
            "Throughput: {:.2} TPS",
            nr_txns as f64 / elapsed.as_secs_f64()
        );

        for (name, counter) in vec!["READ", "UPDATE", "INSERT"]
            .into_iter()
            .zip(abort_counters.iter())
        {
            println!("{}-FAILED: {}", name, counter.load(Ordering::SeqCst));
        }

        for (name, mut histogram) in vec!["READ", "UPDATE", "INSERT"]
            .into_iter()
            .zip(measurements)
        {
            histogram.refresh();

            if histogram.len() == 0 {
                continue;
            }

            print_histogram(&histogram, Some(name));
            save_quantiles(
                &histogram,
                output_dir.clone(),
                workload_name.to_str().unwrap(),
                name,
                nr_threads,
            )?;
        }
    }

    Ok(())
}

fn get_temp_db() -> Result<(Db<String, String>, tempfile::TempDir)>
where
{
    let db_dir = tempfile::tempdir().unwrap();
    let options = DbOptions::new().root_path(&db_dir.path());
    let db = Db::open(options)?;

    Ok((db, db_dir))
}

pub fn main() {
    let opts: Opts = Opts::parse();

    tracing_subscriber::fmt::init();

    let (db, _db_dir) = if let Some(rootdir) = opts.rootdir {
        let options = DbOptions::new().root_path(rootdir);
        let db = Db::open(options).unwrap();
        (db, None)
    } else {
        let (db, _db_dir) = get_temp_db().unwrap();
        (db, Some(_db_dir))
    };

    run_ycsb(&db, opts.workload, opts.ncpus, opts.outputdir, opts.runonly).unwrap();

    db.shutdown();

    kagi::memdebug::report_usage();
}
