// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package verifier

// Metrics summarises a set of counters maintained by the verifying LogCache
type Metrics struct {
	// COUNTERS

	CheckpointsWritten    uint64
	RangesVerified        uint64
	ReadChecksumFailures  uint64
	WriteChecksumFailures uint64
	DroppedReports        uint64
}
