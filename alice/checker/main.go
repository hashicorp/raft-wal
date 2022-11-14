// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/hashicorp/raft"
	wal "github.com/hashicorp/raft-wal"
)

func main() {
	flag.Parse()

	if err := run(os.Args[1], os.Args[2]); err != nil {
		log.Fatal(err)
	}
}

func readStdoutFile(stdoutFile string) (uint64, error) {
	stdout, err := os.Open(stdoutFile)
	if err != nil {
		return 0, err
	}
	last := uint64(0)
	for {
		_, err := fmt.Fscanf(stdout, "commitIdx=%d\n", &last)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	return last, nil
}

func run(dir string, stdoutFile string) error {
	w, err := wal.Open(dir, wal.WithSegmentSize(32*1024))
	if err != nil {
		return err
	}

	// Find the expected committed range
	commitIdx, err := readStdoutFile(stdoutFile)
	if err != nil {
		return err
	}

	last, err := w.LastIndex()
	if err != nil {
		return err
	}

	// Did we loose committed appends
	if last < commitIdx {
		return fmt.Errorf("Only recovered up to Index %d, expected everything <= %d to be durable", last, commitIdx)
	}
	fmt.Printf("Found lastIndex=%d, commitIdx=%d\n", last, commitIdx)

	var i uint64
	var l raft.Log
	for i = 1; i <= last; i++ {
		if err := w.GetLog(i, &l); err != nil {
			return fmt.Errorf("error reading log [%d/%d] - %v", i, last, err)
		}
	}

	log.Printf("OK!\n")
	return nil
}
