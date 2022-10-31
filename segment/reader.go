// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hashicorp/raft-wal/types"
)

// Reader allows reading logs from a segment file.
type Reader struct {
	info types.SegmentInfo
	rf   types.ReadableFile

	// tail optionally providers an interface to the writer state when this is an
	// unsealed segment so we can fetch from it's in-memory index.
	tail tailWriter
}

type tailWriter interface {
	OffsetForFrame(idx uint64) (uint32, error)
}

func openReader(info types.SegmentInfo, rf types.ReadableFile) (*Reader, error) {
	var hdr [fileHeaderLen]byte

	if _, err := rf.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}

	if err := validateFileHeader(hdr[:], info); err != nil {
		return nil, err
	}

	return &Reader{
		info: info,
		rf:   rf,
	}, nil
}

// Close implements io.Closer
func (r *Reader) Close() error {
	return r.rf.Close()
}

// GetLog returns the raw log entry bytes associated with idx. If the log
// doesn't exist in this segment types.ErrNotFound must be returned.
func (r *Reader) GetLog(idx uint64) ([]byte, error) {
	offset, err := r.findFrameOffset(idx)
	if err != nil {
		return nil, err
	}

	buf := r.makeBuffer()
	_, payload, err := r.readFrame(offset, buf)
	if err != nil {
		return nil, err
	}
	return payload, err
}

func (r *Reader) readFrame(offset uint32, buf []byte) (frameHeader, []byte, error) {
	if _, err := r.rf.ReadAt(buf, int64(offset)); err != nil {
		return frameHeader{}, nil, err
	}
	fh, err := readFrameHeader(buf)
	if err != nil {
		return fh, nil, err
	}

	if (frameHeaderLen + int(fh.len)) <= len(buf) {
		// We already have all we need read, just return it
		return fh, buf[frameHeaderLen : frameHeaderLen+fh.len], nil
	}

	// Need to read more bytes, validate that len is a sensible number
	if fh.len > MaxEntrySize {
		return fh, nil, fmt.Errorf("%w: frame header indicates a record larger than MaxEntrySize (%d bytes)", types.ErrCorrupt, MaxEntrySize)
	}

	buf = make([]byte, fh.len)
	if _, err := r.rf.ReadAt(buf[0:fh.len], int64(offset+frameHeaderLen)); err != nil {
		return fh, nil, err
	}
	return fh, buf, nil
}

func (r *Reader) makeBuffer() []byte {
	// TODO consider sync.Pool for read buffers
	return make([]byte, minBufSize)
}

func (r *Reader) findFrameOffset(idx uint64) (uint32, error) {
	if r.tail != nil {
		// This is not a sealed segment.
		return r.tail.OffsetForFrame(idx)
	}

	// Sealed segment, read from the on-disk index block.
	if r.info.IndexStart == 0 {
		return 0, fmt.Errorf("sealed segment has no index block")
	}

	if idx < r.info.MinIndex || (r.info.MaxIndex > 0 && idx > r.info.MaxIndex) {
		return 0, types.ErrNotFound
	}

	// IndexStart is the offset to the first entry in the index array. We need to
	// find the byte offset to the Nth entry
	entryOffset := (idx - r.info.BaseIndex)
	byteOffset := r.info.IndexStart + (entryOffset * 4)

	var bs [4]byte
	n, err := r.rf.ReadAt(bs[:], int64(byteOffset))
	if err == io.EOF && n == 4 {
		// Read all of it just happened to be at end of file, ignore
		err = nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read segment index: %w", err)
	}
	offset := binary.LittleEndian.Uint32(bs[:])
	return offset, nil
}
