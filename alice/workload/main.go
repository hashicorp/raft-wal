// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	wal "github.com/hashicorp/raft-wal"
)

type opts struct {
	dir       string
	workload  string
	init      bool
	truncType string
}

func main() {
	var o opts

	flag.StringVar(&o.dir, "dir", "./workload_dir", "path to directory for WAL files")
	flag.StringVar(&o.workload, "workload", "append", "workload to run, one of 'append', 'truncate-head', 'truncate-tail', 'truncate-all'")
	flag.BoolVar(&o.init, "init", false, "whether this is the init or actual recording")
	flag.Parse()

	var fn func(o opts) error
	var initFn func(o opts) error
	switch o.workload {
	case "append":
		fn = runAppend
	case "truncate-head":
		o.truncType = "head"
		fn = runTruncate
		initFn = runInitTruncate
	case "truncate-tail":
		o.truncType = "tail"
		fn = runTruncate
		initFn = runInitTruncate
	case "truncate-all":
		o.truncType = "all"
		fn = runTruncate
		initFn = runInitTruncate
	default:
		log.Fatalf("unsupported workload %q", o.workload)
	}

	if o.init {
		fn = initFn
	}
	if fn == nil {
		return
	}

	if err := fn(o); err != nil {
		log.Fatal(err)
	}
}

// runInitTruncate sets up a WAL with a bunch of segments ready to test
// truncations. We setup ahead of the actual alice test to limit the IOs needed
// to be explored when simulating different scenarios.
//
// The setup leaves us with a set of segments that contain the following ranges:
//
//	[1..20]
//	[21..40]
//	[41..60]
//	[61..65]
func runInitTruncate(o opts) error {
	return populate(o.dir,
		16*1024, // 16 KiB segments
		1024,    // 1KiB byte logs
		20,      // batchSize (20 * 1024 is bigger than segment size so each segment will have this many logs except the tail)
		65,      // 65 logs total
	)
}

func runTruncate(o opts) error {
	w, err := wal.Open(o.dir, wal.WithSegmentSize(8*1024))
	if err != nil {
		return err
	}

	// Output the initial commitIdx to get checker in sync!
	last, err := w.LastIndex()
	if err != nil {
		return err
	}
	fmt.Printf("commitIdx=%d\n", last)

	switch o.truncType {
	case "head":
		// Remove the first two segments
		fmt.Printf("willTruncateBefore=46\n")
		err = w.DeleteRange(0, 45)
		fmt.Printf("truncatedBefore=46\n")
	case "tail":
		// Remove the last two segments
		fmt.Printf("willTruncateAfter=34\n")
		err = w.DeleteRange(35, 100)
		fmt.Printf("truncatedAfter=34\n")
	case "all":
		fmt.Printf("willTruncateAfter=0\n")
		err = w.DeleteRange(0, 100)
		fmt.Printf("truncatedAfter=0\n")
	}
	if err != nil {
		return err
	}

	last, err = w.LastIndex()
	if err != nil {
		return err
	}

	// Now append another entry to prove we are in a good state and can't loose
	// following writes in a crash.
	err = w.StoreLog(&raft.Log{
		Index:      last + 1,
		Term:       1,
		Type:       raft.LogCommand,
		Data:       []byte("Post Truncate Entry"),
		AppendedAt: time.Now(),
	})
	if err != nil {
		return err
	}
	fmt.Printf("commitIdx=%d\n", last+1)

	return nil
}

func runAppend(o opts) error {
	// We want to limit the total disk IOs we do because ALICE takes forever to
	// explore the reordering state space if there are more than a few. To
	// exercise realistic enough code paths though we want at least a couple of
	// append batches in each segment and at least one segment rotation. We'll
	// just write large values which will be treated as a single disk op while
	// taking up more space. We'll use 16 KiB segments and write values that are
	// 4KiB each (with headers that will take us over segment limit after 4 logs
	// appended in 2 batches.). To make it easier to manually inspect hex dumps of
	// WAL files for debugging, we'll use printable chars rather than random
	// bytes, and make the deterministic so we can also confirm that we didn't
	// accidentally return the wrong payload or corrupt them too.
	return populate(o.dir,
		16*1024, // 16 KiB segments
		4096,    // 4KiB logs
		2,       // batchSize
		8,       // Add 8 logs in total
	)
}

func resetDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") || strings.HasSuffix(e.Name(), ".db") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func populate(dir string, segMentSize, logSize, batchSize, num int) error {
	if err := resetDir(dir); err != nil {
		return err
	}

	w, err := wal.Open(dir, wal.WithSegmentSize(segMentSize))
	if err != nil {
		return err
	}

	var logs []*raft.Log

	if logSize%4 != 0 {
		return fmt.Errorf("logSize must be a multiple of 4")
	}
	if num > 999 {
		return fmt.Errorf("num must be less than 999")
	}

	numRepeats := logSize / 4

	commitBatch := func() error {
		if len(logs) == 0 {
			return nil
		}
		if err := w.StoreLogs(logs); err != nil {
			return err
		}
		// Log that we expect everything up to i to be durable now so checker can
		// assert that.
		fmt.Printf("commitIdx=%d\n", logs[len(logs)-1].Index)
		logs = logs[:0]
		return nil
	}

	for i := 1; i <= num; i++ {
		logs = append(logs, &raft.Log{
			Index:      uint64(i),
			Term:       1,
			Type:       raft.LogCommand,
			Data:       bytes.Repeat([]byte(fmt.Sprintf("%03d|", i)), numRepeats),
			AppendedAt: time.Now(),
		})

		if len(logs) >= batchSize {
			if err := commitBatch(); err != nil {
				return err
			}
		}
	}

	// Commit the remainder
	return commitBatch()
}
