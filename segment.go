// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"fmt"
	"io"
	"time"
)

const (
	segmentFileSuffix = ".wal"
	defaultBlockSize  = 1024 * 1024 // 1 MiB
	defaultNumBlocks  = 64          // 64 MiB segments
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
	r segmentReader
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

// segmentFiler is the interface that provides access to segments to the WAL. It
// encapsulated creating, and recovering segments and returning reader or writer
// interfaces to interact with them. It's main purpose is to abstract the core
// WAL logic both from the actual encoding layer of segment files. You can think
// of it as a layer of abstraction above the VFS which abstracts actual file
// system operations on files but knows nothing about the format. In tests for
// example we can implement a segmentFiler that is way simpler than the real
// encoding/decoding layer on top of a VFS - even an in-memory VFS which makes
// tests much simpler to write and run.
type segmentFiler interface {
	// Create adds a new segment with the given info and returns a writer or an
	// error.
	Create(info SegmentInfo) (segmentWriter, error)

	// RecoverTail is called on an unsealed segment when re-opening the WAL it
	// will attempt to recover from a possible crash. It will either return an
	// error, or return a valid segmentWriter that is ready for further appends.
	RecoverTail(info SegmentInfo) (segmentWriter, error)

	// Open an already sealed segment for reading. Open may validate the file's
	// header and return an error if it doesn't match the expected info.
	Open(info SegmentInfo) (segmentReader, error)

	// List returns the set of segment IDs currently stored. It's used by the WAL
	// on recovery to find any segment files that need to be deleted following a
	// unclean shutdown. The returned map is a map of ID -> BaseIndex. BaseIndex
	// is returned to allow subsequent Delete calls to be made.
	List() (map[uint64]uint64, error)

	// Delete removes the segment with given baseIndex and id if it exists. Note
	// that baseIndex is technically redundant since ID is unique on it's own. But
	// in practice we name files (or keys) with both so that they sort correctly.
	// This interface allows a  simpler implementation where we can just delete
	// the file if it exists without having to scan the underlying storage for a.
	Delete(baseIndex, ID uint64) error
}

// segmentWriter manages appending logs to the tail segment of the WAL. It's an
// interface to make testing core WAL simpler. Every segmentWriter will have
// either `init` or `recover` called once before any other methods. When either
// returns it must either return an error or be ready to accept new writes and
// reads.
type segmentWriter interface {
	io.Closer
	segmentReader

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
	// atomic int. If the segment is empty lastIndex should return zero.
	lastIndex() uint64
}

// segmentReader wraps a ReadableFile to allow lookup of logs in an existing
// segment file. It's an interface to make testing core WAL simpler. The first
// call will always be validate which passes in the ReaderAt to be used for
// subsequent reads.
type segmentReader interface {
	io.Closer

	// getLog returns the raw log entry bytes associated with idx. If the log
	// doesn't exist in this segment ErrNotFound must be returned.
	getLog(idx uint64) ([]byte, error)
}
