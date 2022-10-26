// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/go-wal"
)

const (
	// MaxEntrySize is the largest we allow any single raft log entry to be. This
	// is larger than our raft implementation ever allows so seems safe to encode
	// statically for now. We could make this configurable. It's main purpose it
	// to limit allocation when reading entries back if their lengths are
	// corrupted.
	MaxEntrySize = 64 * 1024 * 1024 // 64 MiB

	// minBufSize is the size we allocate read and write buffers. Setting it
	// larger wastes more memory but increases the chances that we'll read the
	// whole frame in a single shot and not need a second allocation and trip to
	// the disk.
	minBufSize = 64 * 1024

	fileHeaderLen = 32
	version       = 0
	magic         = 0x58eb6b0d

	// Note that this must remain a power of 2 to ensure aligning to this also
	// aligns to sector boundaries.
	frameHeaderLen = 8
)

const ( // Start iota from 0
	FrameInvalid uint8 = iota
	FrameEntry
	FrameIndex
	FrameCommit
)

var (
	// ErrTooBig indicates that the caller tried to write a logEntry with a
	// payload that's larger than we are prepared to support.
	ErrTooBig = errors.New("entries larger than 64MiB are not supported")
)

/*

  File Header functions

	0      1      2      3      4      5      6      7      8
	+------+------+------+------+------+------+------+------+
	| Magic                     | Reserved           | Vsn  |
	+------+------+------+------+------+------+------+------+
	| BaseIndex                                             |
	+------+------+------+------+------+------+------+------+
	| SegmentID                                             |
	+------+------+------+------+------+------+------+------+
	| Codec                                                 |
	+------+------+------+------+------+------+------+------+

*/

// writeFileHeader writes a file header into buf for the given file metadata.
func writeFileHeader(buf []byte, info wal.SegmentInfo) error {
	if len(buf) < fileHeaderLen {
		return io.ErrShortBuffer
	}

	binary.LittleEndian.PutUint32(buf[0:4], magic)
	// Explicitly zero Reserved bytes just in case
	buf[4] = 0
	buf[5] = 0
	buf[6] = 0
	buf[7] = version
	binary.LittleEndian.PutUint32(buf[8:12], info.BlockSize)
	binary.LittleEndian.PutUint32(buf[12:16], info.NumBlocks)
	binary.LittleEndian.PutUint64(buf[16:24], info.BaseIndex)
	binary.LittleEndian.PutUint64(buf[24:32], info.ID)
	return nil
}

// readFileHeader reads a file header into	 buf for the given file metadata.
func readFileHeader(buf []byte) (*wal.SegmentInfo, error) {
	if len(buf) < fileHeaderLen {
		return nil, io.ErrShortBuffer
	}

	var i wal.SegmentInfo
	m := binary.LittleEndian.Uint64(buf[0:8])
	if m != magic {
		return nil, wal.ErrCorrupt
	}
	if buf[7] != version {
		return nil, wal.ErrCorrupt
	}
	i.BlockSize = binary.LittleEndian.Uint32(buf[8:12])
	i.NumBlocks = binary.LittleEndian.Uint32(buf[12:16])
	i.BaseIndex = binary.LittleEndian.Uint64(buf[16:24])
	i.ID = binary.LittleEndian.Uint64(buf[24:32])
	return &i, nil
}

func validateFileHeader(buf []byte, expect wal.SegmentInfo) error {
	got, err := readFileHeader(buf)
	if err != nil {
		return err
	}

	if got.BaseIndex != expect.BaseIndex ||
		got.ID != expect.ID ||
		got.BlockSize != expect.BlockSize ||
		got.NumBlocks != expect.NumBlocks {
		return wal.ErrCorrupt
	}
	return nil
}

/*
	  Frame Functions

		0      1      2      3      4      5      6      7      8
		+------+------+------+------+------+------+------+------+
		| Type | FrameLen           | EntryLen/CRC              |
		+------+------+------+------+------+------+------+------+
*/

type frameHeader struct {
	typ           uint8
	len           uint32
	entryLenOrCRC uint32
}

func isIndexableFrame(fh frameHeader) bool {
	return fh.typ == FrameFull || fh.typ == FrameFirst
}

func writeFrame(buf []byte, h frameHeader, payload []byte) error {
	if len(buf) < encodedFrameSize(int(h.len)) || h.len > 1<<24 {
		return io.ErrShortBuffer
	}
	if err := writeFrameHeader(buf, h); err != nil {
		return err
	}
	copy(buf[frameHeaderLen:], payload[:h.len])
	// Explicitly write null bytes for padding
	padBytes := padLen(int(h.len))
	for i := 0; i < padBytes; i++ {
		buf[frameHeaderLen+int(h.len)+i] = 0x0
	}
	return nil
}

func writeFrameHeader(buf []byte, h frameHeader) error {
	if len(buf) < frameHeaderLen {
		return io.ErrShortBuffer
	}
	buf[0] = h.typ
	buf[1] = byte(h.len)       // FrameLen least significant byte
	buf[2] = byte(h.len >> 8)  // FrameLen
	buf[3] = byte(h.len >> 16) // FrameLen most significant byte
	binary.LittleEndian.PutUint32(buf[4:8], h.entryLenOrCRC)
	return nil
}

var zeroHeader [frameHeaderLen]byte

func readFrameHeader(buf []byte) (frameHeader, error) {
	var h frameHeader
	if len(buf) < frameHeaderLen {
		return h, io.ErrShortBuffer
	}

	switch buf[0] {
	default:
		return h, fmt.Errorf("%w: corrupt frame header with unknown type %d", wal.ErrCorrupt, buf[0])

	case FrameInvalid:
		// Check if the whole header is zero and return a zero frame as this could
		// just indicate we've read right off the end of the written data during
		// recovery.
		if bytes.Equal(buf[:frameHeaderLen], zeroHeader[:]) {
			return h, nil
		}
		return h, fmt.Errorf("%w: corrupt frame header with type 0 but non-zero other fields", wal.ErrCorrupt)

	case FrameFull, FrameFirst, FrameMiddle, FrameLast, FrameIndex, FrameCommit:
		h.typ = buf[0]
	}

	h.len = uint32(buf[1]) | uint32(buf[2])<<8 | uint32(buf[3])<<16
	h.entryLenOrCRC = binary.LittleEndian.Uint32(buf[4:8])
	return h, nil
}

// padLen returns how many bytes of padding should be added to a frame of length
// n to ensure it is a multiple of headerLen. We ensure frameHeaderLen is a
// power of two so that it's always a multiple of a typical sector size (e.g.
// 512 bytes) to reduce the risk that headers are torn by being written across
// sector boundaries. It will return an int in the range [0, 7].
func padLen(n int) int {
	// This looks a bit awful but it's just doing (n % 8) and subtracting that
	// from 8 to get the number of bytes extra needed to get up to the next 8-byte
	// boundary. The extra & 7 is to handle the case where n is a multiple of 8
	// already and so n%8 is 0 and 8-0 is 8. By &ing 8 (0b1000) with 7 (0b111) we
	// effectively wrap it back around to 0. This only works as long as
	// frameHeaderLen is a power of 2 but that's necessary per comment above.
	return (frameHeaderLen - (n % frameHeaderLen)) & (frameHeaderLen - 1)
}

func encodedFrameSize(payloadLen int) int {
	return frameHeaderLen + payloadLen + padLen(payloadLen)
}

// maxIndexSize returns the limit on how many entries we want to be able to
// index per block. Having this fixed allows us to allocate enough memory up
// front and not have to extend the backing array as we go which makes
// concurrency much harder to reason about. This heuristic was chosen after a
// lot of thought. Here's the rationale:
//
// Firstly, the absolute limit for this encoding assuming that a block was
// filled entirely with frames with zero byte payloads and zero commit frames
// (which don't consume index space), that is still 8 bytes of frame header and
// 4 bytes for the index entry for each one. So even ignoring the trailer and
// index frame header, there is an absolute upper bound of BlockSize/12 entries.
// For 1MiB blocks that would be ~87k entries consuming ~350KiB of index. Just
// always allocating 350KiB is not a huge deal especially since it's only one
// tail not one per block etc.
//
// But that scenario is not very realistic. Even if Raft were storing only empty
// payloads (which itself is extremely unlikely) our current BinaryCodec would
// need something like 18 bytes on average even for the first block of the log
// with smallest indexes. (Mostly due to the time.Time taking 12 bytes), So the
// maximum entries is more like BlockSize/(12+18). If we also take the liberty
// of assuming that there are at least 2 bytes of payload on average then it's
// BlockSize/32. This is still extremely conservative although it's technically
// possible that with a custom codec and unrealistic write conditions you could
// get smaller. But the worst that can happen if we run out of index entries in
// a block before we run out of space is that we waste a few KiB of space. We're
// already way more efficient than a B-tree on space which tends to only half
// fill each page etc. So this seems unnecessary to optimize away at cost of
// memory allocations we will never use in practice.
//
// So we pick the BlockSize/32 as the max number of entries. For 1MiB blocks
// that means ~32k entries or 128KiB of index needed. That seems fine! This also
// doesn't seem unreasonable if we later want to try larger blocks up to our max
// of 16MiB which would need 2MiB of index.
func maxIndexSize(blockSize int) int {
	return blockSize / 32
}

// initIndexSize returns a suitable value to use when allocating index arrays.
// It's not critical since they can be safely re-allocated and copied if we need
// more room, but we hope that won't be too common! Assume that each entry on
// average is at least 64 bytes encoded (that's about 32 bytes of raw user data
// with current encoding). We can't store more than this number in a block
// anyway (ignoring index, trailers and padding overhead as this is just a rough
// guess)
func initIndexSize(blockSize int) int {
	return blockSize / 64
}

func indexFrameSize(numEntries int) int {
	// Index frames are completely unnecessary if the whole block is a
	// continuation with no new entries.
	if numEntries == 0 {
		return 0
	}
	// Index frames don't need padding as the next record is always the block
	// trailer and is written in the same write.
	return frameHeaderLen + (4 * numEntries)
}

func writeIndexFrame(buf []byte, offsets []uint32) error {
	if len(buf) < indexFrameSize(len(offsets)) {
		return io.ErrShortBuffer
	}
	fh := frameHeader{
		typ: FrameIndex,
		len: uint32(len(offsets) * 4),
	}
	if err := writeFrameHeader(buf, fh); err != nil {
		return err
	}
	cursor := frameHeaderLen
	for _, o := range offsets {
		binary.LittleEndian.PutUint32(buf[cursor:], o)
		cursor += 4
	}
	// No padding needed as there is no header to be written after
	return nil
}

/*

  Block Functions

	Block trailer:

	0      1      2      3      4      5      6      7      8
	+------+------+------+------+------+------+------+------+
	| FirstIndex                                            |
	+------+------+------+------+------+------+------+------+
	| BatchStart                | IndexStart                |
	+------+------+------+------+------+------+------+------+
	| NumEntries                | CRC                       |
	+------+------+------+------+------+------+------+------+

*/

type blockTrailer struct {
	firstIndex uint64
	batchStart uint32
	indexStart uint32
	numEntries uint32
	crc        uint32
}

func writeBlockTrailer(buf []byte, t blockTrailer) error {
	if len(buf) < blockTrailerLen {
		return io.ErrShortBuffer
	}
	binary.LittleEndian.PutUint64(buf[0:8], t.firstIndex)
	binary.LittleEndian.PutUint32(buf[8:12], t.batchStart)
	binary.LittleEndian.PutUint32(buf[12:16], t.indexStart)
	binary.LittleEndian.PutUint32(buf[16:20], t.numEntries)
	binary.LittleEndian.PutUint32(buf[20:24], t.crc)
	return nil
}

// readBlockTrailer from buffer. buffer is assumed to be the last N bytes of the
// block _ending_ with a block trailer. It might be only the trailer but if it's
// longer it's assumed the trailer is at the end. This allows readers to read a
// chunk of the end of the block and then if the index is within that chunk they
// don't need a second read.
func readBlockTrailer(buf []byte) (blockTrailer, error) {
	var t blockTrailer
	if len(buf) < blockTrailerLen {
		return t, io.ErrShortBuffer
	}

	trailerOffset := len(buf) - blockTrailerLen

	t.firstIndex = binary.LittleEndian.Uint64(buf[trailerOffset : trailerOffset+8])
	t.batchStart = binary.LittleEndian.Uint32(buf[trailerOffset+8 : trailerOffset+12])
	t.indexStart = binary.LittleEndian.Uint32(buf[trailerOffset+12 : trailerOffset+16])
	t.numEntries = binary.LittleEndian.Uint32(buf[trailerOffset+16 : trailerOffset+20])
	t.crc = binary.LittleEndian.Uint32(buf[trailerOffset+20 : trailerOffset+24])
	return t, nil
}

// nextBlockStart returns the offset at which the next block of blockSize starts
// that is _strictly greater_ than offset. If offset is a block boundary already
// it will return the next one.
func nextBlockStart(blockSize, offset uint32) uint32 {
	// There are sneaky ways to speed this up given than blockSize is a power of
	// two but this is not performance critical!
	return ((offset / blockSize) + 1) * blockSize
}

// blockIDFromOffset returns the blockID that the given offset falls inside
func blockIDFromOffset(blockSize, offset uint32) uint32 {
	return offset / blockSize
}
