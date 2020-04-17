#!/usr/bin/env bash

set -e

benchmark() {
    store=$1
    n=$2
    batch=$3
    snap=$4

    ./benchmark --log-store ${store} \
                -n ${n} -batch-size ${batch} -snap-interval=${snap} \
                -chart ${store}-b${batch}-n${n}-s${snap}.png
}

benchmark_store() {
    store=$1

    benchmark $1 10000 1 5s
    benchmark $1 100000 64 5s
    benchmark $1 1000000 128 5s
}

go build -o ./benchmark .

benchmark_store "wal-nosync"
benchmark_store "wal"
benchmark_store "bolt"
