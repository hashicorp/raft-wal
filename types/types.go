// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package types

import (
	"errors"

	"github.com/hashicorp/raft"
)

var (
	ErrNotFound = raft.ErrLogNotFound
	ErrCorrupt  = errors.New("WAL is corrupt")
	ErrSealed   = errors.New("segment is sealed")
	ErrClosed   = errors.New("closed")
)

// LogEntry represents an entry that has already been encoded.
type LogEntry struct {
	Index uint64
	Data  []byte
}
