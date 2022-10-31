// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package types

import "errors"

var (
	ErrNotFound = errors.New("Log entry not found")
	ErrCorrupt  = errors.New("WAL is corrupt")
	ErrSealed   = errors.New("Segment is sealed")
)

// LogEntry represents an entry that has already been encoded.
type LogEntry struct {
	Index uint64
	Data  []byte
}
