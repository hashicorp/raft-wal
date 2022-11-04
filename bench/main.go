// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/benmathews/bench"
	"github.com/hashicorp/raft-wal/metadb"
)

type opts struct {
	// LogStore params
	version        string
	dir            string
	segSize        int
	noFreelistSync bool

	// Common params
	preLoadN int

	// Append params
	rate      int
	duration  time.Duration
	logSize   int
	batchSize int

	// Truncate params
	truncateTrailingLogs int
	truncatePeriod       time.Duration
}

func main() {
	var o opts

	flag.StringVar(&o.version, "v", "wal", "version to test 'wal' or 'bolt'")
	flag.StringVar(&o.dir, "dir", "", "dir to write to. If empty will create a tmp dir. If not empty the dir will delete any existing WAL files present!")
	flag.IntVar(&o.rate, "rate", 10, "append rate target per second")
	flag.DurationVar(&o.duration, "t", 10*time.Second, "duration of the test")
	flag.IntVar(&o.logSize, "s", 128, "size of each log entry appended")
	flag.IntVar(&o.batchSize, "n", 1, "number of logs per append batch")
	flag.IntVar(&o.segSize, "seg", 64, "segment size in MB")
	flag.IntVar(&o.truncateTrailingLogs, "trail", 10000, "number of trailing logs to leave on truncate")
	flag.DurationVar(&o.truncatePeriod, "tp", 0, "how often to head truncate back to 'trail' logs during append")
	flag.IntVar(&o.preLoadN, "preload", 0, "number of logs to append and then truncate before we start")
	flag.BoolVar(&o.noFreelistSync, "no-fl-sync", false, "used to disable freelist sync in boltdb for v=bolt")
	flag.Parse()

	if o.dir == "" {
		tmpDir, err := os.MkdirTemp("", "raft-wal-bench-*")
		if err != nil {
			panic(err)
		}

		defer os.RemoveAll(tmpDir)
		o.dir = tmpDir
	} else {
		// Delete metadb and any segment files present
		files, err := os.ReadDir(o.dir)
		if err != nil {
			panic(err)
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			if strings.HasSuffix(f.Name(), ".wal") || f.Name() == metadb.FileName || f.Name() == "raft.db" {
				os.RemoveAll(filepath.Join(o.dir, f.Name()))
			}
		}
	}
	r := &appendRequesterFactory{opts: o}
	benchmark := bench.NewBenchmark(r, uint64(o.rate), 1, o.duration, 0)
	summary, err := benchmark.Run()
	if err != nil {
		panic(err)
	}

	printHistogram("Good Append Latencies (ms)", summary.SuccessHistogram, 1_000_000)

	fmt.Println(summary)
	summary.GenerateLatencyDistribution(nil, outFileName(o, "bench-result"))
}

func outFileName(o opts, prefix string) string {
	version := o.version
	if o.version == "bolt" && o.noFreelistSync {
		version += "-nfls"
	}
	return fmt.Sprintf("%s-%s-s%d-n%d-r%d-seg%dm-pre%d-trail%d-tp%s-%s.txt", prefix,
		o.duration, o.logSize, o.batchSize, o.rate, o.segSize, o.preLoadN,
		o.truncateTrailingLogs, o.truncatePeriod, version)
}

func printHistogram(name string, h *hdrhistogram.Histogram, scale int64) {
	fmt.Printf("\n==> %s\n", name)
	fmt.Printf("  count    mean     p50     p99   p99.9     max\n")
	fmt.Printf(" %6d  %6.0f  %6d  %6d  %6d  %6d\n",
		h.TotalCount(),
		h.Mean()/float64(scale),
		h.ValueAtPercentile(50)/scale,
		h.ValueAtPercentile(99)/scale,
		h.ValueAtPercentile(99.9)/scale,
		h.Max()/scale,
	)
}
