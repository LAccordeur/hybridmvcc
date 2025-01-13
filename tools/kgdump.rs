use kagi::{Db, DbOptions};

use clap::Parser;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    #[clap(short, long, required = true)]
    rootdir: String,
}

pub fn main() {
    let opts: Opts = Opts::parse();

    let options = DbOptions::new().root_path(opts.rootdir);
    let db: Db<String, String> = Db::open(options).unwrap();

    db.dump_version();
}
