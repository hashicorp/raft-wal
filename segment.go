// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	segmentFileSuffix = ".wal"
)

type SegmentInfo struct {
	// ID uniquely identifies this segment file
	ID uint64

	// BaseIndex is the raft index of the first entry that will be written to the
	// segment.
	BaseIndex uint64

	// MinIndex is the logical lowest index that still exists in the segment. It
	// may be greater than BaseIndex if a head truncation has "deleted" a prefix
	// of the segment.
	MinIndex uint64

	// MaxIndex is the logical highest index that still exists in the segment. It
	// may be lower than the actual highest index if a tail truncation has
	// "deleted" a suffix of the segment. It is zero for unsealed segments and
	// only set one seal.
	MaxIndex uint64

	// Codec identifies the codec used to encode log entries. Codec values 0 to
	// 16k (i.e. the lower 16 bits) are reserved for internal future usage. Custom
	// codecs must be registered with an identifier higher than this which the
	// caller is responsible for ensuring uniquely identifies the specific version
	// of their codec used in any given log. uint64 provides sufficient space that
	// a randomly generated identifier is almost certainly unique.
	Codec uint64

	// BlockSize is the configured size of each indexed block at the time this
	// segment was created.
	BlockSize uint32

	// NumBlocks is the total number of blocks the file was pre-allocated with. In
	// some file-systems that don't support pre-allocation the file might not
	// actually be this long yet, but we treat it like it is.
	NumBlocks uint32

	// CreateTime records when the segment was first created.
	CreateTime time.Time

	// SealTime records when the segment was sealed. Zero indicates that it's not
	// sealed yet.
	SealTime time.Time

	// r is the logGetter for our in-memory state it's private so it won't be
	// serialized or visible to external callers like MetaStore implementations.
	r segmentLogGetter
}

func (i *SegmentInfo) fileName() string {
	return fmt.Sprintf("%020d-%016x%s", i.BaseIndex, i.ID, segmentFileSuffix)
}

func (i *SegmentInfo) preallocatedSize() uint64 {
	return uint64(i.BlockSize) * uint64(i.NumBlocks)
}

// logEntry represents an entry that has already been encoded.
type logEntry struct {
	Index uint64
	Data  []byte
}

// segmentWriter manages appending logs to the tail segment of the WAL. It's an
// interface to make testing core WAL simpler. Every segmentWriter will have
// either `init` or `recover` called once before any other methods. When either
// returns it must either return an error or be ready to accept new writes and
// reads.
type segmentWriter interface {
	io.Closer
	segmentLogGetter

	// init writes the initial file header and prepares it for writing new logs.
	init(f WritableFile, info SegmentInfo) error

	// recover is called when the WAL is opened and a file that was not yet sealed
	// exists on disk. It must decide whether the data in the log is valid and
	// recoverable and setup all internal writer state ready to append new
	// entries.
	recover(f WritableFile, info SegmentInfo) error

	// append adds one or more entries. It must not return until the entries are
	// durably stored otherwise raft's guarantees will be compromised.
	append(entries []logEntry) error

	// full returns true if the segment is considered full compared with it's
	// pre-allocated size. It is called _after_ append which is expected to have
	// worked regardless of the size of the append (i.e. the segment might have to
	// grow beyond it's pre-allocated blocks to accommodate the final append).
	full() bool

	// lastIndex returns the most recently persisted index in the log. It must
	// respond without blocking on append since it's needed frequently by read
	// paths that may call it concurrently. Typically this will be loaded from an
	// atomic int.
	lastIndex() uint64
}

// segmentLogGetter is a common interface that both readable and writable files
// need to be able to fetch logs.
type segmentLogGetter interface {
	// getLog returns the raw log entry bytes associated with idx. If the log
	// doesn't exist in this segment ErrNotFound must be returned.
	getLog(idx uint64) ([]byte, error)
}

// segmentReader wraps a ReadableFile to allow lookup of logs in an existing
// segment file. It's an interface to make testing core WAL simpler. The first
// call will always be validate which passes in the ReaderAt to be used for
// subsequent reads.
type segmentReader interface {
	io.Closer
	segmentLogGetter

	// validate checks the ReaderAt contains a valid segment file header. If it
	// returns nil, it must be ready for getLog calls - possibly concurrently.
	validate(r ReadableFile, info SegmentInfo) error
}

// findOldSegments finds the file names in files that are no longer part of the
// WAL meta in m and need to be deleted.
func findOldSegments(m meta, files []string) []string {
	toDelete := make([]string, 0, 1)

	it := m.segments.Iterator()
	segIDs := make(map[uint64]struct{})
	for !it.Done() {
		_, seg, _ := it.Next()
		segIDs[seg.ID] = struct{}{}
	}

	for _, fname := range files {
		if !strings.HasSuffix(fname, segmentFileSuffix) {
			continue
		}

		var fBase, fID uint64
		n, err := fmt.Sscanf(fname, "%020d-%016x"+segmentFileSuffix, &fBase, &fID)
		if err != nil || n != 2 {
			// misnamed file. skip it even though it has our WAL suffix just to be
			// cautious.
			continue
		}

		if _, ok := segIDs[fID]; !ok {
			// File's ID is no longer in the segment list. Delete it!
			toDelete = append(toDelete, fname)
		}
	}
	return toDelete
}
