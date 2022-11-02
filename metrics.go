// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"sync/atomic"
)

var (
	// All metrics are held in an array of uint64s accessed atomically. The names
	// are defined in the list below. The order doesn't really matter for
	// backwards compatibility since it is not persisted state although it might
	// impact the order we print things. Why not a struct? This makes it easier to
	// iterate them without reflection and stops us hard coding the same list in
	// several places.
	metrics = []string{
		// COUNTERS

		// LogEntryBytesWritten counts the bytes of log entry after encoding with
		// Codec. Actual bytes written to disk might be slightly higher as it includes
		// headers and index entries.
		"log_entry_bytes_written",

		// log_entries_written counts the number of entries written.
		"log_entries_written",

		// log_appends counts the number of calls to StoreLog(s) i.e. number of
		// batches of entries appended.
		"log_appends",

		// log_entry_bytes_read counts the bytes of log entry read from segments before
		// decoding. actual bytes read from disk might be higher as it includes
		// headers and index entries and possible secondary reads for large entries
		// that don't fit in buffers.
		"log_entry_bytes_read",

		// log_entries_read counts the number of calls to get_log.
		"log_entries_read",

		// segment_rotations counts how many times we move to a new segment file.
		"segment_rotations",

		// head_truncations counts how many log entries have been truncated from the
		// head - i.e. the oldest entries. by graphing the rate of change over time
		// you can see individual truncate calls as spikes.
		"head_truncations",

		// tail_truncations counts how many log entries have been truncated from the
		// head - i.e. the newest entries. by graphing the rate of change over time
		// you can see individual truncate calls as spikes.
		"tail_truncations",

		"stable_gets",
		"stable_sets",

		// gauges

		// last_segment_age_seconds is a gauge that is set each time we rotate a segment
		// and describes the number of seconds between when that segment file was
		// first created and when it was sealed. this gives a rough estimate how
		// quickly writes are filling the disk.
		"last_segment_age_seconds",
	}

	metricIDs  map[string]int
	numMetrics int
)

func init() {
	numMetrics = len(metrics)
	metricIDs = make(map[string]int, numMetrics)
	for i, n := range metrics {
		metricIDs[n] = i
	}
}

func (w *WAL) incr(name string, delta uint64) uint64 {
	id, ok := metricIDs[name]
	if !ok {
		panic("invalid metric name: " + name)
	}
	return atomic.AddUint64(&w.metrics[id], delta)
}

func (w *WAL) setGauge(name string, val uint64) {
	id, ok := metricIDs[name]
	if !ok {
		panic("invalid metric name: " + name)
	}
	atomic.StoreUint64(&w.metrics[id], val)
}

// Metrics returns a summary of the performance counters for the WAL since
// startup. This is intentionally agnostic to any metrics collector library.
// Package users should be able to poll this and report counter, gauge or timing
// information in their native format from this.
func (w *WAL) Metrics() map[string]uint64 {
	// Copy the fields out with atomic reads
	m := make(map[string]uint64)
	for i, n := range metrics {
		m[n] = atomic.LoadUint64(&w.metrics[i])
	}
	return m
}
