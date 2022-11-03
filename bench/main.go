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

	"github.com/benmathews/bench"
	"github.com/hashicorp/raft-wal/metadb"
)

type opts struct {
	version   string
	dir       string
	rate      int
	duration  time.Duration
	logSize   int
	batchSize int
	segSize   int
	preLoadN  int
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
	flag.IntVar(&o.preLoadN, "preload", 0, "number of logs to append and then truncate before we start")
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
	r := &appendRequesterFactory{
		Dir:       o.dir,
		Version:   o.version,
		LogSize:   o.logSize,
		BatchSize: o.batchSize,
		SegSize:   o.segSize,
		Preload:   o.preLoadN,
	}
	benchmark := bench.NewBenchmark(r, uint64(o.rate), 1, o.duration, 1)
	summary, err := benchmark.Run()
	if err != nil {
		panic(err)
	}

	fmt.Println(summary)
	outFile := fmt.Sprintf("bench-result-%s-s%d-n%d-r%d-seg%dm-pre%d-%s.txt",
		o.duration, o.logSize, o.batchSize, o.rate, o.segSize, o.preLoadN, o.version)
	summary.GenerateLatencyDistribution(nil, outFile)
}
