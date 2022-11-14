// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/raft"
	wal "github.com/hashicorp/raft-wal"
)

func main() {
	var dir string
	var workload string

	flag.StringVar(&dir, "dir", "./raft-wal", "path to directory for WAL files")
	flag.StringVar(&workload, "workload", "append", "workload to run, one of 'append' (more to come)")
	flag.Parse()

	var fn func(dir string) error
	switch workload {
	case "append":
		fn = runAppend
	default:
		log.Fatalf("unsupported workload %q", workload)
	}

	if err := fn(dir); err != nil {
		log.Fatal(err)
	}
}

func runAppend(dir string) error {
	w, err := wal.Open(dir, wal.WithSegmentSize(16*1024))
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}

	var logs []*raft.Log

	// We want to limit the total disk IOs we do because ALICE takes forever to
	// explore the reordering state space if there are more than a few. To
	// exercise realistic enough code paths though we want at least a couple of
	// append batches in each segment and at least one segment rotation. We'll
	// just write large values which will be treated as a single disk op while
	// taking up more space. We'll use 16 KiB segments (set above) and write
	// values that are 4KiB each (with headers that will take us over segment
	// limit after 4 logs appended in 2 batches.). To make it easier to manually
	// inspect hex dumps of WAL files for debugging, we'll use printable chars
	// rather than random bytes, and make the deterministic so we can also confirm
	// that we didn't accidentally return the wrong payload or corrupt them too.
	for i := 1; i <= 8; i++ {
		logs = append(logs, &raft.Log{
			Index: uint64(i),
			Term:  1,
			Type:  raft.LogCommand,
			// We want 16K/4 = 4KiB entries. Print the index numb in 3 digits and a
			// separator 1024 times!
			Data:       bytes.Repeat([]byte(fmt.Sprintf("%03d|", i)), 1024),
			AppendedAt: time.Now(),
		})

		// Commit in batches of 2 on every even batch since we commit up to and
		// including index 20 which is even.
		if i%2 == 0 {
			if err := w.StoreLogs(logs); err != nil {
				return err
			}
			logs = logs[:0]
			// Log that we expect everything up to i to be durable now so checker can
			// assert that.
			fmt.Printf("commitIdx=%d\n", i)
		}
	}

	return nil
}
