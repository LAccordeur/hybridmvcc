#!/usr/bin/env bash

RUSTFLAGS="-C target-feature=+avx2" cargo +nightly build --release

valgrind --tool=callgrind --callgrind-out-file=callgrind.out target/release/ycsb -n $1 --workload $2 -r /tmp/test -t
callgrind_annotate callgrind.out > callgrind.report

gprof2dot -f callgrind callgrind.out > callgrind.dot
dot -Tpng callgrind.dot -o callgrind.png

rm callgrind.dot callgrind.out
