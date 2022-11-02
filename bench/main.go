// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/benmathews/bench"
)

type opts struct {
	version   string
	dir       string
	rate      int
	duration  time.Duration
	logSize   int
	batchSize int
	outFile   string
}

func main() {
	var o opts

	flag.StringVar(&o.version, "v", "wal", "version to test 'wal' or 'bolt'")
	flag.StringVar(&o.dir, "dir", "", "dir to write to. If empty will create a tmp dir.")
	flag.IntVar(&o.rate, "rate", 10, "append rate target per second")
	flag.DurationVar(&o.duration, "t", 10*time.Second, "duration of the test")
	flag.IntVar(&o.logSize, "s", 128, "size of each log entry appended")
	flag.IntVar(&o.batchSize, "n", 1, "number of logs per append batch")
	flag.StringVar(&o.outFile, "out", "bench-result.txt", "output file for HDR histogram  latency data")
	flag.Parse()

	if o.dir == "" {
		tmpDir, err := os.MkdirTemp("", "raft-wal-bench-*")
		if err != nil {
			panic(err)
		}

		defer os.RemoveAll(tmpDir)
		o.dir = tmpDir
	}

	var r bench.RequesterFactory
	switch o.version {
	case "wal":
		r = &walAppendRequesterFactory{
			Dir:       o.dir,
			LogSize:   o.logSize,
			BatchSize: o.batchSize,
		}
	default:
		fmt.Printf("invalid v: %q. 'wal' and 'bolt' are the supported types", o.version)
		os.Exit(1)
	}

	benchmark := bench.NewBenchmark(r, uint64(o.rate), 1, o.duration, 1)
	summary, err := benchmark.Run()
	if err != nil {
		panic(err)
	}

	fmt.Println(summary)
	summary.GenerateLatencyDistribution(nil, "bench-result.txt")
}
