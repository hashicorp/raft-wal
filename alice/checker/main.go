// Copyright IBM Corp. 2020, 2025
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

var re = regexp.MustCompile(`(\w+)=(\d+)`)

type runSummary struct {
	lastCommit                 uint64
	truncatedAfter             uint64
	truncatedBefore            uint64
	willTruncateAfter          uint64
	willTruncateBefore         uint64
	truncatedEntriesMaybeAfter uint64
	willTruncateHead           bool
	willTruncateTail           bool
	truncatedHead              bool
	truncatedTail              bool
}

func readStdoutFile(stdoutFile string) (runSummary, error) {
	var sum runSummary

	stdout, err := os.Open(stdoutFile)
	if err != nil {
		return sum, err
	}
	defer func() { _ = stdout.Close() }()

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
				sum.truncatedHead = true
				if n > int(sum.truncatedBefore) {
					sum.truncatedBefore = uint64(n)
					sum.truncatedEntriesMaybeAfter = sum.lastCommit
				}
			case "truncatedAfter":
				sum.truncatedTail = true
				if int(sum.truncatedAfter) == 0 || n < int(sum.truncatedAfter) {
					sum.truncatedAfter = uint64(n)
					sum.truncatedEntriesMaybeAfter = sum.truncatedAfter
				}
				if n < int(sum.lastCommit) {
					sum.lastCommit = uint64(n)
				}
			case "willTruncateAfter":
				sum.willTruncateAfter = uint64(n)
				sum.willTruncateTail = true
				sum.truncatedEntriesMaybeAfter = sum.willTruncateAfter
			case "willTruncateBefore":
				sum.willTruncateBefore = uint64(n)
				sum.willTruncateHead = true
				sum.truncatedEntriesMaybeAfter = sum.lastCommit
			default:
				// skip unknown output KVs
			}
			continue
		}
		return sum, fmt.Errorf("unrecognizable output line: %s", line)
	}
	return sum, nil
}

func validateFirst(first uint64, expect runSummary) error {
	switch {
	case expect.willTruncateHead:
		if expect.truncatedHead {
			// We actually completed the truncation. First must now be the new index.
			if first != expect.truncatedBefore {
				return fmt.Errorf("expected first to be %d after truncation. Got %d",
					expect.truncatedBefore, first)
			}

		} else {
			// Not sure if truncation completed or not so allow either value
			if first != 1 && first != expect.willTruncateBefore {
				return fmt.Errorf("expected first to be 1 before truncation, %d after. Got %d",
					expect.willTruncateBefore, first)
			}
		}

	case expect.willTruncateTail && expect.willTruncateAfter == 0:
		// Special case of an "everything" truncation which is modelled as a tail
		// truncation (after=0) In this case first will either be 1 before, 0 right
		// after truncation or 1 again after the next append.
		if first != 0 && first != 1 {
			return fmt.Errorf("expected first to be 1 before truncation, 0 after or 1 after the next append. Got %d",
				first)
		}

	default:
		// No head truncations can have started yet.
		if first != 1 && first != 0 {
			return fmt.Errorf("want first=1 or first=0 (if no writes yet) before any truncation. Got %d", first)
		}
	}
	return nil
}

func validateLast(last uint64, expect runSummary) error {
	switch {
	case expect.willTruncateTail:
		if expect.truncatedTail {
			// We actually completed the truncation. Last must now be the new index,
			// or the subsequent write if that's higher.
			if last != expect.truncatedAfter && last != expect.truncatedAfter+1 {
				return fmt.Errorf("expected last to be %d after truncation or %d after subsequent append. Got %d",
					expect.truncatedAfter, expect.truncatedAfter+1, last)
			}

		} else {
			// Not sure if truncation completed or not so allow any last value greater
			// than the truncate after target (since we know the workload always
			// truncates after an index lower than commitIdx).
			if last < expect.willTruncateAfter {
				return fmt.Errorf("expected last to be >= %d after before or after truncation. Got %d",
					expect.willTruncateAfter, last)
			}
		}

	default:
		// No tail truncations can have started yet. Just ensure we have everything committed.
		if last < expect.lastCommit {
			return fmt.Errorf("want last >= lastCommit. Lost committed writes! last=%d commitIdx=%d", last, expect.lastCommit)
		}
	}
	return nil
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

	if err := validateFirst(first, expect); err != nil {
		return err
	}
	if err := validateLast(last, expect); err != nil {
		return err
	}

	fmt.Printf("Found first=%d last=%d expected %v\n", first, last, expect)

	var i uint64
	var l raft.Log
	for i = first; i <= last; i++ {
		if i == 0 {
			// Everything was truncated so nothing to read!
			continue
		}
		if err := w.GetLog(i, &l); err != nil {
			return fmt.Errorf("error reading log [%d/%d] - %v", i, last, err)
		}
		// Verify contents match
		validPrefixes := []string{fmt.Sprintf("%03d|", i)}
		if (expect.willTruncateHead || expect.willTruncateTail) && i > expect.truncatedEntriesMaybeAfter {
			// If we will truncate but didn't yet either outcome is possible so
			// include both viable options.
			validPrefixes = append(validPrefixes, "Post Truncate Entry")
		}
		if (expect.truncatedTail || expect.truncatedHead) && i > expect.truncatedEntriesMaybeAfter {
			// Truncate completed so the original payload is no longer possible
			validPrefixes = validPrefixes[1:]
		}

		valid := false
		for _, vp := range validPrefixes {
			if bytes.HasPrefix(l.Data, []byte(vp)) {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("entry %d has unexpected payload. Want prefix in %q, got %q",
				i, validPrefixes, string(l.Data))
		}
	}

	log.Printf("OK!\n")
	return nil
}
