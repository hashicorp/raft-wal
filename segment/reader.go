// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hashicorp/go-wal"
)

// Reader allows reading logs from a segment file.
type Reader struct {
	info wal.SegmentInfo
	rf   wal.ReadableFile

	// tail optionally providers an interface to the writer state when this is an
	// unsealed segment and we may need to read from the tail block that's not
	// fully on disk yet.
	tail tailWriter
}

type tailWriter interface {
	LastIndex() uint64
	tail() *tailIndex
}

func openReader(info wal.SegmentInfo, rf wal.ReadableFile) (*Reader, error) {
	var hdr [fileHeaderLen]byte

	if _, err := rf.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}

	gotInfo, err := readFileHeader(hdr[:])
	if err != nil {
		return nil, err
	}

	// Validate info matches
	if info.ID != gotInfo.ID {
		return nil, fmt.Errorf("%w: segment header ID %x doesn't match metadata %x",
			wal.ErrCorrupt, gotInfo.ID, info.ID)
	}
	if info.BaseIndex != gotInfo.BaseIndex {
		return nil, fmt.Errorf("%w: segment header BaseIndex %d doesn't match metadata %d",
			wal.ErrCorrupt, gotInfo.BaseIndex, info.BaseIndex)
	}
	if info.BlockSize != gotInfo.BlockSize {
		return nil, fmt.Errorf("%w: segment header BlockSize %d doesn't match metadata %d",
			wal.ErrCorrupt, gotInfo.BlockSize, info.BlockSize)
	}
	// Note that we don't strictly check NumBlocks because the NumBlocks in the
	// header is the number it was pre-allocated with but it might have actually
	// extended past that before it was sealed. We can at least check the meta
	// data isn't _smaller_ though!
	if info.NumBlocks < gotInfo.NumBlocks {
		return nil, fmt.Errorf("%w: segment header NumBlocks %d larger than metadata %d",
			wal.ErrCorrupt, gotInfo.NumBlocks, info.NumBlocks)
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
// doesn't exist in this segment ErrNotFound must be returned.
func (r *Reader) GetLog(idx uint64) ([]byte, error) {
	offset, err := r.findFrameOffset(idx)
	if err != nil {
		return nil, err
	}

	buf := r.makeBuffer()
	fh, payload, err := r.readFrame(offset, buf)
	if err != nil {
		return nil, err
	}

	if fh.typ == FrameFull {
		// We're done!
		return payload, nil
	}

	// We need to read the rest of the frames from later blocks. Note that payload
	// should be the size of the whole entry with only the first frame's worth
	// filled so far.
	nextOffset := nextBlockStart(r.info.BlockSize, offset)
	readLen := fh.len
	for {
		// The next frame will be at the next block boundary after offset since a
		// first frame must have filled the block. Read directly into our payload
		// slice which was allocated to be the full capacity needed in the first
		// readFrame.
		fh, _, err := r.readFrame(nextOffset, payload[readLen:])
		if err != nil {
			return nil, err
		}
		if fh.typ != FrameMiddle && fh.typ != FrameLast {
			return nil, fmt.Errorf("%w: expected continuation frame got type=%d", wal.ErrCorrupt, fh.typ)
		}
		if fh.typ == FrameLast {
			// Done!
			return payload, nil
		}

		// If there is more it must be at the start of the next block!
		nextOffset = nextBlockStart(r.info.BlockSize, nextOffset)
		readLen += fh.len
	}
}

func (r *Reader) readFrame(offset uint32, buf []byte) (frameHeader, []byte, error) {
	// Don't read more than the end of the block!
	maxLen := nextBlockStart(r.info.BlockSize, offset) - offset
	if int(maxLen) > len(buf) {
		// Clamp it to the length of our buffer too!
		maxLen = uint32(len(buf))
	}

	if _, err := r.rf.ReadAt(buf[:maxLen], int64(offset)); err != nil {
		return frameHeader{}, nil, err
	}
	fh, err := readFrameHeader(buf)
	if err != nil {
		return fh, nil, err
	}

	// Note on fh.typ == FrameFull: even if we read the whole frame, if this is a
	// First frame, re-allocate the correct size and repeat the read since we'll
	// need to copy the first frame into the eventual buffer anyway to build the
	// whole thing.
	if (frameHeaderLen+int(fh.len)) <= len(buf) && fh.typ == FrameFull {
		// We already have all we need read, just return it
		return fh, buf[frameHeaderLen : frameHeaderLen+fh.len], nil
	}

	// Need to read more bytes, validate that len is a sensible number
	if fh.len > maxFrameLen {
		return fh, nil, wal.ErrCorrupt
	}
	// If this is the first of a fragmented entry, actually allocate enough space
	// for the whole thing!
	allocLen := fh.len
	if fh.typ == FrameFirst {
		allocLen = fh.entryLenOrCRC
		// Also validate that is not wild before we allocate
		if allocLen > MaxEntrySize {
			return fh, nil, wal.ErrCorrupt
		}
	}
	if int(allocLen) > cap(buf) {
		buf = make([]byte, allocLen)
	}
	if _, err := r.rf.ReadAt(buf[0:fh.len], int64(offset+frameHeaderLen)); err != nil {
		return fh, nil, err
	}
	return fh, buf[0:allocLen], nil
}

func (r *Reader) makeBuffer() []byte {
	// TODO consider sync.Pool for read buffers
	bufLen := minBufSize
	if r.info.BlockSize < minBufSize {
		bufLen = int(r.info.BlockSize)
	}
	return make([]byte, bufLen)
}

func (r *Reader) findFrameOffset(idx uint64) (uint32, error) {
	numCompleteBlocks := r.info.NumBlocks

	if r.tail != nil {
		// This is not a sealed segment.

		// FIRST load the current committed index. We do this first to ensure that
		// the tail we load right after definitely contains the most recent commit
		last := r.tail.LastIndex()
		if idx > last {
			return 0, wal.ErrNotFound
		}

		// Now load tail info (atomically) and see if the log we want is in there
		tail := r.tail.tail()
		if len(tail.offsets) > 0 && tail.firstIndex <= idx {
			// The log we want is committed and in the tail block, read the offset
			// from the in-memory array.
			indexOffset := idx - tail.firstIndex
			if indexOffset >= uint64(len(tail.offsets)) {
				// Can't happen (tm), just want to be sure we never access an array
				// index greater than numEntries as that's potentially racey.
				return 0, wal.ErrNotFound
			}
			return tail.offsets[indexOffset], nil
		}
		// Not in tail, fall through to searching the already complete disk blocks
		if tail.blockID == 0 {
			// The tail block is the first block in the segment so there is nothing
			// else to search!
			return 0, wal.ErrNotFound
		}
		// Note blocks are zero indexed so the ID of the tail is the number of
		// previous complete blocks.
		numCompleteBlocks = tail.blockID
	}
	// Not in tail, check if it's in the sealed blocks of this segment
	if idx < r.info.MinIndex || (r.info.MaxIndex > 0 && r.info.MaxIndex < idx) {
		return 0, wal.ErrNotFound
	}

	// Binary search those blocks on disk to find the right one!
	buf := r.makeBuffer()
	var foundTrailer *blockTrailer
	var bufStart int64
	_, err := blockSearch(int(numCompleteBlocks), func(i int) (bool, bool, error) {
		// Read block i's trailer
		startedAt, t, err := r.readBlocktrailer(buf, i)
		if err != nil {
			return false, false, err
		}
		bufStart = startedAt

		// If the block is empty something fishy is going on - this should be a
		// complete block but trailer is missing! There is no possible case where
		// all of these can be zero legitimately even if crc just happened to
		// collide with 0 because the block with firstIndex 0 must be the first
		// block in a segment which means that at very least the batchStart and
		// indexStart can't be zero because that's the file header!
		if t.firstIndex == 0 && t.crc == 0 && t.batchStart == 0 {
			return false, false, fmt.Errorf("%w: zero block trailer for complete block %d", wal.ErrCorrupt, i)
		}

		// Did we find what we were looking for?
		//
		// Note the special case for blocks that don't contain any new records of
		// their own only continuations. In this cast firstIndex may be equal to the
		// idx we are looking for, but we should still treat it like it's later
		// since the _start_ of the record we want is in an earlier block.
		if t.firstIndex <= idx && t.numEntries > 0 {
			lastIdx := t.firstIndex + uint64(t.numEntries) - 1
			if idx <= lastIdx {
				// This is the one! Stop so buf can be used to find the index entry
				foundTrailer = t
				return true, true, nil
			}
			// The block we want must be after this so we are in the prefix that
			// should return false.
			return false, false, nil
		}

		// blocks first index is greater so it's part of the suffix after the block
		// we want.
		return true, false, nil
	})

	if err != nil {
		return 0, err
	}
	if foundTrailer == nil {
		return 0, wal.ErrNotFound
	}

	if idx >= (foundTrailer.firstIndex + uint64(foundTrailer.numEntries)) {
		// Shouldn't be possible but prevent out-of-bounds read!
		return 0, wal.ErrNotFound
	}

	// We found the block! See if we already have the whole index in buf
	var offset uint32

	// Work out how many bytes past the indexStart (which is the start of the
	// index frame _header_) the actual value we need will be. There will be 4
	// bytes for every entry
	offsetFromIndexStart := frameHeaderLen + ((idx - foundTrailer.firstIndex) * 4)

	if foundTrailer.indexStart >= uint32(bufStart) {
		// Find the offset into buf where the actual data of the index frame starts
		startInBuf := foundTrailer.indexStart - uint32(bufStart)

		// Just to make things clearer, lets slice buf down so it starts where the
		// index frame does
		buf = buf[startInBuf:]

		// We can now decode directly into offset
		offset = binary.LittleEndian.Uint32(buf[offsetFromIndexStart : offsetFromIndexStart+4])
	} else {
		// We didn't read whole index. We only need to both reading the 4 bytes and
		// we know where they are!
		var bs [4]byte
		if _, err := r.rf.ReadAt(bs[:], int64(foundTrailer.indexStart+uint32(offsetFromIndexStart))); err != nil {
			return 0, err
		}

		offset = binary.LittleEndian.Uint32(bs[:])
	}

	return offset, nil
}

func (r *Reader) readBlocktrailer(buf []byte, blockID int) (int64, *blockTrailer, error) {
	blockEnd := int64(blockID+1) * int64(r.info.BlockSize)

	// Read the last len(buf) bytes from the block
	bufStart := blockEnd - int64(len(buf))
	n, err := r.rf.ReadAt(buf, bufStart)
	if err == io.EOF && n == len(buf) {
		// We read the whole thing but it just happened to be the last bytes of
		// the file, ignore - it wasn't really an error if we read it all!
		err = nil
	}
	if err != nil {
		return 0, nil, err
	}

	t, err := readBlockTrailer(buf)
	if err != nil {
		return 0, nil, err
	}
	return bufStart, &t, nil
}

// findFirstPartialBlock returns the ID of the first block that doesn't have a
// complete trailer. It may be that the file is complete and blockID is one past
// the end and will just hit EOF when we try to read it. It's also possible for
// the last partial block to be greater than info.NumBlocks in the case that the
// MetaData was out of sync with the actual file.
func (r *Reader) findFirstPartialBlock() (uint32, error) {
	// Assume the file is as large indicated in the metadata. It's possible it's
	// not since not all file systems support preallocation which means we need to
	// handle EOF as gracefully as we can.
	var buf [blockTrailerLen]byte
	blockID, err := blockSearch(int(r.info.NumBlocks), func(i int) (bool, bool, error) {
		complete, err := r.isBlockComplete(buf[:], uint32(i))
		if err != nil {
			return false, false, err
		}
		// search expects a prefix to return false while finding the first that
		// returns true. In this case complete blocks are the prefix of of the log.
		// So we'll actually find the first incomplete block.
		return !complete, false, err
	})
	if err != nil {
		return 0, err
	}

	if blockID == int(r.info.NumBlocks) {
		// All known blocks are complete. It's possible there are more complete or
		// incomplete blocks though. The final append to a file typically extends it
		// with one or more additional blocks beyond it's initial allocated size. If
		// this completes but then the system crashes before the new NumBlocks is
		// persisted to the meta store, then we will have only found the last
		// complete block in the original size and there are still later ones that
		// may be either complete or incomplete.
		//
		// We can't binary search any more but it's only a single append batch at
		// most so assume it's never more than a handful of blocks to scan.
		for {
			complete, err := r.isBlockComplete(buf[:], uint32(blockID))
			if err != nil {
				return 0, err
			}
			if !complete {
				// This is the actual last partial block! It might be partial because we
				// hit EOF but that's OK, we still have to work out if it has any
				// committed records in.
				return uint32(blockID), nil
			}
			blockID++
		}
	}

	return uint32(blockID), err
}

func (r *Reader) isBlockComplete(buf []byte, blockID uint32) (bool, error) {
	// Read block i's trailer
	_, t, err := r.readBlocktrailer(buf, int(blockID))
	if err == io.EOF {
		// Treat EOF specially in this case. As noted above some file systems
		// can't preallocate so the file might be shorter than the metadata makes
		// it seem. Just treat this as unwritten tail/incomplete block.
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Is the block zero? This combination can never occur in a valid written
	// block.
	if t.firstIndex == 0 && t.crc == 0 && t.numEntries == 0 && t.batchStart == 0 {
		return false, nil
	}

	return true, nil
}

// blockSearch is copied from sort.Search but modified to allow us to terminate
// the search early when we find the right block to avoid pointless additional
// file IO.
func blockSearch(n int, f func(int) (bool, bool, error)) (int, error) {
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		r, done, err := f(h)
		if done {
			return h, nil
		}
		if err != nil {
			return -1, err
		}
		if !r {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i, nil
}
