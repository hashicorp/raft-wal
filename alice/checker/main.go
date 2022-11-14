// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"

	"github.com/hashicorp/raft"
	wal "github.com/hashicorp/raft-wal"
)

func main() {
	flag.Parse()

	if err := run(os.Args[1], os.Args[2]); err != nil {
		log.Fatal(err)
	}
}

var re = regexp.MustCompile("(\\w+)=(\\d+)")

type runSummary struct {
	lastCommit      uint64
	truncatedAfter  uint64
	truncatedBefore uint64
	truncated       bool
}

func readStdoutFile(stdoutFile string) (runSummary, error) {
	var sum runSummary

	stdout, err := os.Open(stdoutFile)
	if err != nil {
		return sum, err
	}
	defer stdout.Close()

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		matches := re.FindStringSubmatch(line)
		if matches != nil {
			n, err := strconv.Atoi(matches[2])
			if err != nil {
				return sum, err
			}
			switch matches[1] {
			case "commitIdx":
				sum.lastCommit = uint64(n)
			case "truncatedBefore":
				sum.truncated = true
				if n > int(sum.truncatedBefore) {
					sum.truncatedBefore = uint64(n)
					sum.truncatedAfter = sum.lastCommit
				}
			case "truncatedAfter":
				sum.truncated = true
				if int(sum.truncatedAfter) == 0 || n < int(sum.truncatedAfter) {
					sum.truncatedAfter = uint64(n)
				}
				if n < int(sum.lastCommit) {
					sum.lastCommit = uint64(n)
				}
			default:
				// skip unknown output KVs
			}
			continue
		}
		return sum, fmt.Errorf("unrecognizable output line: %s", line)
	}
	return sum, nil
}

func run(dir string, stdoutFile string) error {
	w, err := wal.Open(dir, wal.WithSegmentSize(32*1024))
	if err != nil {
		return err
	}

	// Find the expected committed range
	expect, err := readStdoutFile(stdoutFile)
	if err != nil {
		return err
	}

	first, err := w.FirstIndex()
	if err != nil {
		return err
	}
	last, err := w.LastIndex()
	if err != nil {
		return err
	}

	// Did we loose committed appends?
	if last < expect.lastCommit {
		return fmt.Errorf("Only recovered up to Index %d, expected everything <= %d to be durable",
			last, expect.lastCommit)
	}
	if expect.truncatedBefore > 0 && first < expect.truncatedBefore {
		return fmt.Errorf("Expected entry %d, earlier than %d to be truncated", first, expect.truncatedBefore)
	}
	fmt.Printf("Found first=%d last=%d expected %v\n", first, last, expect)

	var i uint64
	var l raft.Log
	for i = first; i <= last; i++ {
		if err := w.GetLog(i, &l); err != nil {
			return fmt.Errorf("error reading log [%d/%d] - %v", i, last, err)
		}
		// Verify contents match
		wantPrefix := fmt.Sprintf("%03d|", i)
		if expect.truncated && i > expect.truncatedAfter {
			wantPrefix = "Post Truncate Entry"
		}
		if !bytes.HasPrefix(l.Data, []byte(wantPrefix)) {
			return fmt.Errorf("entry %d has unepected payload. Want prefix %q, got %q",
				i, wantPrefix, string(l.Data))
		}
	}

	log.Printf("OK!\n")
	return nil
}
