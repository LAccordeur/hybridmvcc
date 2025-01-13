#!/usr/bin/env bash

RUSTFLAGS="-C target-feature=+avx2" cargo +nightly run --release --bin ycsb --features "hybrid_ts" -- -n $1 --workload $2
