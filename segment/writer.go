// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"fmt"
	"hash/crc32"
	"io"
	"sync/atomic"

	"github.com/hashicorp/go-wal"
)

// Writer allows appending logs to a segment file as well as reading them back.
type Writer struct {
	// commitIdx is updated after an append batch is fully persisted to disk to
	// allow readers to read the new value. Note that readers must not read values
	// larger than this even if they are available in tailIndex as they are not
	// yet committed to disk!
	commitIdx uint64

	// tailBlock stores information about the current tail block. It's atomic so
	// that writers can move to a new tail block with an atomic swap leaving
	// concurrent readers to read the old tail without blocking. See tailIndex for
	// details on the data.
	tailBlock atomic.Value // *tailIndex

	// writer state is accessed only on the (serial) write path so doesn't need
	// synchronization.
	writer struct {
		// commitBuf stores the pending frames waiting to be flushed to the current
		// tail block.
		commitBuf []byte

		// batchStart stores the file offset of the first frame in the current
		// batch (which may be in an earlier block than the current tail).
		batchStart uint32

		// writeOffset is the absolute file offset up to which we've written data to
		// the file. The contents of commitBuf will be written at this offset when
		// it commits or we reach the end of the block, whichever happens first.
		writeOffset uint32
	}

	info wal.SegmentInfo
	wf   wal.WritableFile
	r    wal.SegmentReader
}

type tailIndex struct {
	// firstIndex is immutable and is the raft index associated with the first log
	// entry that will be recorded in this block.
	firstIndex uint64

	// blockID is immutable and represents which block in the file this index
	// covers.
	blockID uint32

	// offsets is the index offset. The first element corresponds to the
	// firstIndex. It is accessed concurrently by readers and the single writer
	// without locks! This is race-free via the following invariants:
	//  - the slice here is never mutated only copied though it may still refer to
	//    the same backing array.
	//  - readers only ever read up to len(offsets) in the atomically accessed
	//    index. Those elements of the backing array are immutable and will never
	//    be modified once they are accessible to readers.
	//  - readers and writers synchronize on atomic access to this tailIndex
	//  - serial writer will only append to the end which either mutates the
	//    shared backing array but at an index greater than the len any reader has
	//    seen, or a new backing array is allocated and the old one copied into it
	//    which also will never mutate the entries readers can already "see" via
	//    the old slice.
	offsets []uint32
}

func validateInfo(info wal.SegmentInfo) error {
	if info.BlockSize < (2 * minFragmentLen) {
		// This is probably only going to happen in test code as this is an
		// unreasonably small block size for real usage.
		return fmt.Errorf("BlockSize can't be smaller than %d", 2*minFragmentLen)
	}
	return nil
}

func createFile(info wal.SegmentInfo, wf wal.WritableFile) (*Writer, error) {
	if err := validateInfo(info); err != nil {
		return nil, err
	}

	// Write header and sync
	var hdr [fileHeaderLen]byte
	if err := writeFileHeader(hdr[:], info); err != nil {
		return nil, err
	}
	if _, err := wf.WriteAt(hdr[:], 0); err != nil {
		return nil, err
	}
	if err := wf.Sync(); err != nil {
		return nil, err
	}

	r, err := openReader(info, wf)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		info: info,
		wf:   wf,
		r:    r,
	}
	r.tail = w
	w.initEmpty()
	return w, nil
}

func recoverFile(info wal.SegmentInfo, wf wal.WritableFile) (*Writer, error) {
	if err := validateInfo(info); err != nil {
		return nil, err
	}

	// Read header
	var hdr [fileHeaderLen]byte
	if _, err := wf.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}
	if err := validateFileHeader(hdr[:], info); err != nil {
		return nil, err
	}
	r, err := openReader(info, wf)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		info: info,
		wf:   wf,
		r:    r,
	}
	r.tail = w

	// Recover file!

	// First, find the last complete block trailer with binary search.
	tailBlockID, err := r.findFirstPartialBlock()
	if err != nil {
		return nil, fmt.Errorf("recovery failed to find last partial block: %w", err)
	}

	// See if we can recover the tail block.
	discard, partial, err := w.attemptRecoverTail(tailBlockID, info.BlockSize)
	if err != nil {
		return nil, err
	}

	// From this point forward, ensure that after recovery we attempt to set the
	// tail block based on the tailIndex information we uncovered.
	// attemptRecoverTail can't always do this correctly so we always do it in
	// this one place instead.
	defer w.completeRecovery()

	if !discard && !partial {
		// We recovered just fine!
		return w, nil
	}

	// The whole of the last incomplete block was empty or part of an incomplete
	// write so we pretend it never happened and is empty (since it can't have
	// been acknowledged). We still need to find the last block with actual
	// committed frames though so we can recover it to a good state.
	for discard {
		// Try again with the block before
		if tailBlockID == 0 {
			// We got back to the start of the log. It's essentially empty. Initialize
			// as an empty segment.
			w.initEmpty()
			return w, nil
		}
		tailBlockID--
		discard, partial, err = w.attemptRecoverTail(tailBlockID, info.BlockSize)
		if err != nil {
			return nil, err
		}
		// Loop until we read one where discard == false, then continue below
	}

	if !partial {
		// We found the tail block and it contained the whole of the last commit wo
		// we're done!
		return w, nil
	}
	if tailBlockID == 0 {
		// The first block is a special case because we never allow batches to span
		// across segments so even if we didn't find the previous commit point in
		// the block we know the start of the block must have been the start of the
		// append batch. In other words, we can treat this as a non-partial block
		// and we're done!
		return w, nil
	}

	// Found the tail block and recovered it, but it only has part of the final
	// commit. We need to go back and validate that everything else written during
	// that commit is present and correct before we can call it good.

	// For that we need to read the trailer of the block before which must have
	// been written as part of that commit, and from the BatchStart field we'll
	// know how far back we need to validate. We'll need at least some maybe all
	// of the block in memory to validate the last commit so just read whole
	// blocks here to save fiddling with multiple reads.
	buf := make([]byte, info.BlockSize)
	lastCompleteBlockID := tailBlockID - 1
	_, trailer, err := r.readBlockTrailer(buf, int(lastCompleteBlockID))
	if err != nil {
		return nil, err
	}

	// Every block between the one in batchStart and lastCompleteBlockID was
	// modified during the last append batch. We need to validate those writes are
	// all complete before we can safely recover the tail.
	batchStartBlockID := blockIDFromOffset(info.BlockSize, trailer.batchStart)

	// Since we already have the last of these read into memory lets check that
	// and then if needed check the others.
	crcOK := w.checkBlockCRC(buf, lastCompleteBlockID, trailer.crc, trailer.batchStart)

	// Assuming that was OK, walk through the other blocks we need too
	bid := batchStartBlockID
	for crcOK && bid < lastCompleteBlockID {
		_, trailer, err := r.readBlockTrailer(buf, int(bid))
		if err != nil {
			return nil, err
		}
		crcOK = w.checkBlockCRC(buf, bid, trailer.crc, trailer.batchStart)
		bid++
	}

	if crcOK {
		// Hurrah, every block that was written to in the last append is intact. We
		// are done!
		return w, nil
	}

	// Something is missing in that last append _somewhere_ note it might be
	// anywhere because we only fsync after each whole batch and the OS is free to
	// re-order actual disk writes as much as it likes between fsyncs.
	//
	// At this point we have to call that whole last append broken but since we
	// must never have acknowledged it, we can just ignore it and treat
	// trailer.batchStart as the actual tail of the segment. We do need to reset
	// the tail state we setup during recover though.
	maxLen := trailer.batchStart - (batchStartBlockID * info.BlockSize)
	_, _, err = w.attemptRecoverTail(batchStartBlockID, maxLen)
	if err != nil {
		return nil, err
	}

	// We don't care about partial of discard since we already recovered from
	// partial append and because even if the block was empty and discard
	// returned, it was setup to append to again which is what we need now we know
	// this is the right tail!
	return w, nil
}

func (w *Writer) initEmpty() {
	// We just wrote the header to the file so the next write needs to go after
	// that. Initialize the writeCursor.
	w.writer.writeOffset = fileHeaderLen
	w.writer.batchStart = fileHeaderLen

	// Initialize the tail
	tail := &tailIndex{
		firstIndex: w.info.BaseIndex,
		blockID:    0,
		offsets:    make([]uint32, 0, initIndexSize(int(w.info.BlockSize))),
	}
	w.tailBlock.Store(tail)
}

func (w *Writer) checkBlockCRC(buf []byte, blockID, wantCRC, batchStart uint32) bool {
	// buf contains the whole block, work out how much of it we need.
	blockStart := w.info.BlockSize * blockID
	crcStartInBlock := uint32(0)
	if batchStart > blockStart {
		// If the transaction started after the start of the block, we adjust
		// where we start reading to compare only the bytes written in that batch.
		crcStartInBlock = batchStart - blockStart
	}
	gotCrc := crc32.Checksum(buf[crcStartInBlock:len(buf)-4], castagnoliTable)
	return wantCRC == gotCrc
}

// attemptRecoverTail reads records from the indicated block until it finds no
// more entries. It attempts to find a complete set with a commit frame at the
// end. There are a few cases we need to cover:
//  1. There is no commit frame: indicates this was an incomplete write and the
//     acknowledged tail must be in a previous block. In this case discard is
//     trie and the caller should re-attempt recovery on previous block treating
//     this one as empty space.
//  2. There is one commit frame with or without subsequent frames. Everything
//     after the last commit frame is considered uncommitted and ignored.
//     Everything from the start of the block to the commit frame must validate
//     against the checksum in the commit frame or it's considered junk and
//     discard set to true. In this case because we only found the final commit,
//     we don't know how many frames in previous blocks were also part of that
//     final append batch so discard is false because there is potentially good
//     data here, but partial is also true because the caller needs to look back
//     through previous block to find the rest of the last batch before we know
//     it if all made it to disk safely.
//  3. There is more than one commit frame. In this case we need to find the
//     last one, and check that all data between previous commit frame and that
//     commit frame validate against the commit frame checksum. If such a frame
//     is found this is the new tail of the log. If not then the previous commit
//     frame must be the tail. Either way discard and partial are both false and
//     the recovery is complete.
//
// If discard is true then the Writer's tail information was not changed and the
// caller must do so. If discard is false, the tailBlock state was rebuilt
// during the call so that we're ready to continue if this is really the tail.
//
// maxLen is used when recovering a tail block that was written completely but
// where some part of the written data was missing. We should pretend that the
// block is empty space after this point and only recover records up to maxLen.
// If we want to use the whole block just pass maxLen as BlockSize.
//
// Note that w.commitIndex is never set by this method because it doesn't have
// all the information it needs in some cases.
func (w *Writer) attemptRecoverTail(blockID, maxLen uint32) (discard, partial bool, err error) {
	blockStart := blockID * w.info.BlockSize
	bufSize := w.info.BlockSize
	readStart := blockStart
	if blockID > 0 {
		// We need to read the block trailer of the last complete block before this
		// one to know what the firstIndex of this tail block should be.
		bufSize += blockTrailerLen
		readStart -= blockTrailerLen
	}
	buf := make([]byte, bufSize)
	n, err := w.wf.ReadAt(buf, int64(readStart))
	if err == io.EOF {
		// Don't treat this as an error since the file might not have been
		// preallocated on some filesystems so might only be as long as the entries
		// written. Just make sure we don't read past n
		if n < int(maxLen) {
			maxLen = uint32(n)
		}
		err = nil
	}
	if err != nil {
		return false, false, fmt.Errorf("failed to read tail block %d: %w", blockID, err)
	}

	// If we need to read the previous block's trailer to work out firstIndex
	firstIndex := w.info.BaseIndex
	if blockID > 0 {
		trailer, err := readBlockTrailer(buf[:blockTrailerLen])
		if err != nil {
			return false, false, fmt.Errorf("failed to read last complete block's trail for block %d: %w", blockID-1, err)
		}

		// This one is really subtle, but we have slightly different invariants
		// in-memory vs on disk for what "firstIndex" means on the tail block. On
		// disk we know by the time we write out the trailer whether there were
		// actually any new entries started in that block and so firstIndex on a
		// block with no new entries in (only middle frames) will be the same as the
		// block before. But in memory, in general we don't know until the end of
		// the block whether firstIndex should be the same as the last entry of the
		// block before or one higher so the steady state code maintains it as "the
		// next indexable entry that would start in that block if there is one" and
		// only corrects that at then end of the block. For this reason, if the
		// previous block header we just read had zero indexable entries then we
		// need to actually set firstIndex to the _next_ index to mirror the
		// expectations of the in-memory code.
		if trailer.numEntries == 0 {
			firstIndex = trailer.firstIndex + 1
		} else {
			firstIndex = trailer.firstIndex + uint64(trailer.numEntries)
		}

		// reset buf back to just the block we want!
		buf = buf[blockTrailerLen:]
	}

	// Rebuild tail index as we go
	tail := tailIndex{
		firstIndex: firstIndex,
		blockID:    blockID,
		offsets:    make([]uint32, 0, initIndexSize(int(w.info.BlockSize))),
	}
	// We can store this now even though we will mutate it again because recovery
	// only happens before there are any readers so this can't race!
	w.tailBlock.Store(&tail)

	if maxLen == 0 {
		// There block is effectively (or actually!) empty. We mark it as needing to
		// be discarded because we can't say for sure whether the last write
		// completed or not based on this block. It _might_ still be the correct
		// tail block, but we don't know that from context here only the caller
		// does. If it was good, we'll leave the writer in a state ready to append to
		// it so all is good!
		w.writer.writeOffset = blockStart
		return true, false, nil
	}

	// Work out where to start reading frames. Almost always at the start of the
	// block unless it's the first one then we need to skip the file header.
	offset := uint32(0)
	if blockID == 0 {
		offset = fileHeaderLen
	}

	type commitSummary struct {
		hdr        frameHeader
		offset     uint32
		crcStart   uint32
		numEntries int
	}
	var prevCommit, finalCommit *commitSummary
	commitFrames := 0
READ:
	for offset < maxLen {
		fh, err := readFrameHeader(buf[offset:])
		if err != nil {
			return false, false, fmt.Errorf("failed to read frame %d from tail block %d: %w", len(tail.offsets), blockID, err)
		}
		if fh.typ == FrameInvalid {
			// Zero type byte indicates either incomplete writes (i.e. a zero sector
			// that didn't make it to disk though later ones might have) or the end of
			// written data (zeros). Either way we've read all we can recover.
			break READ
		}

		// Check we can actually read all the rest of the frame.
		if (offset + frameHeaderLen + fh.len) > maxLen {
			// Shouldn't be possible - it's most likely a bug if we hit this. Could be
			// arbitrary corruption but we don't attempt to handle that for now even
			// if this is the uncommitted tail.
			return false, false, fmt.Errorf("%w: frame too long", wal.ErrCorrupt)
		}

		switch {
		case fh.typ == FrameIndex:
			// If we hit an index frame then this was a complete block written as part
			// of a transaction that continued into later blocks but was incomplete
			// there. Either way we're done and can ignore the rest.
			break READ

		case fh.typ == FrameCommit:
			summary := commitSummary{
				hdr:        fh,
				offset:     offset,
				numEntries: len(tail.offsets),
			}
			if finalCommit != nil {
				// We already saw at least one other commit frame. Copy it over.
				prevCommit = finalCommit
				summary.crcStart = prevCommit.offset + frameHeaderLen
			} else if blockID == 0 {
				// Crc start for the first commit frame is the start of the block unless
				// it was block zero and there was a file header.
				summary.crcStart = fileHeaderLen
			}
			finalCommit = &summary
			commitFrames++

		case isIndexableFrame(fh):
			tail.offsets = append(tail.offsets, blockStart+offset)
		}

		offset += uint32(encodedFrameSize(int(fh.len)))
	}

	// Did we find any commit frames? Implement the cases spelled out in the doc
	// comment.
	if commitFrames == 0 {
		// We didn't find any commits so all of the data here (if any) was part of
		// an interrupted write and should be treated as empty space. It's not
		// "partial" because that indicates that it's intact but not the whole write
		// but this is not even intact within this block since this is meant to be
		// the tail block.
		return true, false, nil
	}

	// Commit frames are just a header so move the write cursor to just after the
	// last commit frame. We'll move it again later if we find this commit was
	// incomplete.
	w.writer.writeOffset = blockStart + finalCommit.offset + frameHeaderLen

	// In all other cases, any frames added _after_ the last commit frame in this
	// tail (probably but not necessarily incomplete) segment can't be part of a
	// valid commit so we truncate them out of the recovered index. BUT they do
	// signal that the last commit frame's commit must have been persisted because
	// we would not have appended further frames until fsync returned. So we know
	// without validating anything else that everything up to that last commit
	// frame must have made it through at least on fsync which is all we are
	// trying to validate here.
	if finalCommit.numEntries < len(tail.offsets) {
		// Truncate the uncommitted frames and we're done.
		tail.offsets = tail.offsets[:finalCommit.numEntries]
		return false, false, nil
	}

	// So now we know that the last frame found is a commit frame. We need to
	// determine whether that entire commit made it to disk (with or without
	// fsync). We could be in either the 1 commitFrame case or the 2 or more case
	// in the doc comment. Either way we have to CRC check all the data that was
	// written to this block in that commit is present and correct.
	gotCRC := crc32.Checksum(buf[finalCommit.crcStart:finalCommit.offset], castagnoliTable)

	if gotCRC == finalCommit.hdr.entryLenOrCRC {
		// CRC is OK so the last commit (or at least the part of it that was in this
		// block) is all accounted for. We return discard=false. Partial is true if
		// and only if we saw more than one commit and so the thing we CRCed was
		// less than the entire block.
		return false, commitFrames == 1, nil
	}

	// At this point the CRC check failed for the last commit. If this was the
	// only commit frame in the block then the whole block is part of an
	// incomplete write and needs to be discarded.
	if commitFrames == 1 {
		return true, false, nil
	}

	// Final case, there were multiple commits in this last block. The final one
	// didn't validate CRC so we consider it incomplete or "torn". That means the
	// _previous_ commit frame marks the true committed tail of the log. We need
	// to recover index state back to _that_ frame and then we're done.
	w.writer.writeOffset = blockStart + prevCommit.offset + frameHeaderLen
	tail.offsets = tail.offsets[:prevCommit.numEntries]
	return false, false, nil
}

// completeRecovery attempts to set internal state based on the recovered tail
// (if there is one). It's called even when recovery fails so should not assume
// a valid tail state.
func (w *Writer) completeRecovery() {
	tail := w.tail()
	if tail == nil {
		return
	}

	// Some verification that helps during testing. These invariants should never
	// be broken.
	start, end := w.tailBlockBounds()
	if w.writer.writeOffset < start || w.writer.writeOffset > end {
		panic("writeOffset is outside of tail block!")
	}

	// Make sure batchStart is set correctly
	w.writer.batchStart = w.writer.writeOffset

	// If we have zero entries and the tail is block 0 then there we can leave it
	// at zero.
	if tail.blockID == 0 && len(tail.offsets) == 0 {
		return
	}

	// Otherwise it's the last entry in the tail block's index
	commitIdx := tail.firstIndex + uint64(len(tail.offsets)) - 1

	// Technically we don't need to access this atomically here since we're still
	// recovering and there can't be any concurrent readers yet. But it's simpler
	// to reason about if we just always do!
	atomic.StoreUint64(&w.commitIdx, commitIdx)
}

// Close implements io.Closer
func (w *Writer) Close() error {
	return w.r.Close()
}

// GetLog implements wal.SegmentReader
func (w *Writer) GetLog(idx uint64) ([]byte, error) {
	return w.r.GetLog(idx)
}

// Append adds one or more entries. It must not return until the entries are
// durably stored otherwise raft's guarantees will be compromised.
func (w *Writer) Append(entries []wal.LogEntry) error {
	// Iterate entries and append each one
	for _, e := range entries {
		if err := w.appendEntry(e); err != nil {
			return err
		}
	}

	// Write the commit frame
	if err := w.appendCommit(); err != nil {
		return err
	}

	// Commit in-memory
	atomic.StoreUint64(&w.commitIdx, entries[len(entries)-1].Index)
	return nil
}

func (w *Writer) tail() *tailIndex {
	return w.tailBlock.Load().(*tailIndex)
}

func (w *Writer) appendEntry(e wal.LogEntry) error {

	tail := w.tail()
	if w.info.BaseIndex == 0 && tail.blockID == 0 && tail.firstIndex == 0 && len(tail.offsets) == 0 {
		// This is the first append to an empty log. Whatever it's index we need to
		// update firstIndex for the block so that it accurately reflects the
		// indexes of the records being added from here.
		newTail := *tail
		newTail.firstIndex = e.Index
		w.tailBlock.Store(&newTail)
	}

	continuation := false
	remaining := e.Data
	var err error
	for {
		remaining, err = w.appendEntryFrame(remaining, continuation)
		if err != nil {
			return err
		}
		if len(remaining) == 0 {
			return nil
		}
		continuation = true
	}
}

func (w *Writer) appendCommit() error {
	// Calculate CRC over all the data in buffer so far. This is all data that was
	// written to this block during this commit.
	crc := crc32.Checksum(w.writer.commitBuf[:], castagnoliTable)
	fh := frameHeader{
		typ:           FrameCommit,
		len:           0,
		entryLenOrCRC: crc,
	}
	avail := w.blockBytesAvailable(false)
	// A commit frame is just a header with no payload
	if avail < frameHeaderLen {
		if err := w.finishBlock(); err != nil {
			return err
		}
	}
	if err := w.appendFrame(fh, nil); err != nil {
		return err
	}
	// Flush all writes to the current block
	if err := w.sync(); err != nil {
		return err
	}

	// Finally, update batchStart so that by the time we write the next trailer
	// we'll know where the append batch started.
	w.writer.batchStart = w.writer.writeOffset
	return nil
}

func (w *Writer) appendEntryFrame(data []byte, continuation bool) ([]byte, error) {
	if len(data) > MaxEntrySize {
		return nil, ErrTooBig
	}

	// Work out how much fits
	fh, fragment, remainder := w.nextEntryFrame(data, continuation)

	if fh == nil {
		// No space for any fragment in the current block. Roll the block and try
		// again.
		if err := w.finishBlock(); err != nil {
			return nil, err
		}
		fh, fragment, remainder = w.nextEntryFrame(data, continuation)
		if fh == nil {
			// Shouldn't be possible!
			return nil, fmt.Errorf("frame doesn't fit in empty block!")
		}
	}

	return remainder, w.appendFrame(*fh, fragment)
}

// tailBlockBounds return the bounds of the current tail block as absolute file
// offsets. That is [startOffset, endOffset)
func (w *Writer) tailBlockBounds() (uint32, uint32) {
	tail := w.tail()
	return tail.blockID * w.info.BlockSize, (tail.blockID + 1) * w.info.BlockSize
}

// blockBytesAvailable returns how many bytes are available in the current block
// with the current index size. If needIndex is true then we calculate available
// bytes assuming that the frame also needs an additional index entry and reduce
// the available space accordingly.
func (w *Writer) blockBytesAvailable(needsIndex bool) int {
	tail := w.tail()
	_, end := w.tailBlockBounds()

	absOffset := w.writer.writeOffset + uint32(len(w.writer.commitBuf))
	totalBytesFree := end - absOffset
	num := len(tail.offsets)
	if needsIndex {
		// This frame will need another entry in the index too.
		num += 1
	}
	return int(totalBytesFree-blockTrailerLen) - indexFrameSize(num)
}

// nextEntryFrame decides which type of frame the next chunk of entry data needs
// to be stored in based on the available space in the block. There are broadly
// three possibilities the caller should be prepared to handle:
//   - nil frameHeader and slices are returned if there is no space for any
//     fragment of data in the current block.
//   - an appropriate frameHeader, fragment slice and nil remainder slice are
//     returned if the data fits entirely in the current block with no additional
//     frames needed.
//   - as above but with a non-nil remainder slice if there is remaining data to
//     appended to the next block(s).
func (w *Writer) nextEntryFrame(data []byte, continuation bool) (*frameHeader, []byte, []byte) {

	frameBytesAvailable := w.blockBytesAvailable(!continuation)

	encodedLen := encodedFrameSize(len(data))
	if encodedLen <= int(frameBytesAvailable) {
		// It will fit with no remainder
		fh := &frameHeader{
			typ: FrameFull,
			len: uint32(len(data)),
		}
		if continuation {
			// This wasn't the first fragment, but no more so it must be the last
			fh.typ = FrameLast
		}
		return fh, data, nil
	}

	// Can we fit a meaningful amount of data in to make it worth fragmenting this
	// entry?
	if frameBytesAvailable < encodedFrameSize(minFragmentLen) {
		return nil, nil, data
	}

	// Setup the fragment we can fit

	// Note that because we don't _need_ padding after an index frame, the index
	// frame might end up being a multiple of 4 not 8. That means that
	// frameBytesAvailable is also not a multiple of 8 so if we size the fragment
	// to exactly that, it will then need to be padded again when it's written and
	// so won't fit again! So ensure frameBytesAvailable is rounded down to
	// nearest multiple of 8.
	frameBytesAvailable -= (frameBytesAvailable % frameHeaderLen)
	fh := &frameHeader{
		typ: FrameMiddle,
		len: uint32(frameBytesAvailable) - frameHeaderLen,
	}
	if !continuation {
		// Not a continuation so this is a first frame. Set the type and also set
		// the original full data length to avoid multiple copies when decoding.
		fh.typ = FrameFirst
		fh.entryLenOrCRC = uint32(len(data))
	}

	return fh, data[0:fh.len], data[fh.len:]
}

func (w *Writer) ensureBufCap(extraLen int) {
	if cap(w.writer.commitBuf) < (cap(w.writer.commitBuf) + extraLen) {
		// Grow the buffer, lets just double it to amortize cost
		newSize := cap(w.writer.commitBuf) * 2
		if newSize < minBufSize {
			newSize = minBufSize
		}
		newBuf := make([]byte, minBufSize)
		oldLen := len(w.writer.commitBuf)
		copy(newBuf, w.writer.commitBuf)
		w.writer.commitBuf = newBuf[:oldLen]
	}
}

// appendFrame appends the given frame to the current block. The frame must fit
// already otherwise an error will be returned.
func (w *Writer) appendFrame(fh frameHeader, data []byte) error {
	// Encode frame header into current block buffer
	l := encodedFrameSize(len(data))
	w.ensureBufCap(l)

	bufOffset := len(w.writer.commitBuf)
	if err := writeFrame(w.writer.commitBuf[bufOffset:bufOffset+l], fh, data); err != nil {
		return err
	}
	// Update len of commitBuf since we resliced it for the write
	w.writer.commitBuf = w.writer.commitBuf[:bufOffset+l]

	// If frame is Full or First, update block index
	if isIndexableFrame(fh) {
		tail := w.tail()
		// Make a new shallow copy of tail. We are making a new offsets slice but
		// pointing to the same backing array. As long as we don't mutate anything
		// below numEntries we're safe since readers will never read beyond that.
		newTail := *tail
		// Add the index entry. Note this is safe despite mutating the same backing
		// array as tail because it's beyond the limit current readers will access
		// until we do the atomic update below. Even if append re-allocates the
		// backing array, it will only read the indexes smaller than numEntries from
		// the old array to copy them into the new one and we are not mutating the
		// same memory locations. Old readers might still be looking at the old
		// array (lower than numEntries) through the current tail.offsets slice but
		// we are not touching that at least below numEntries.
		newTail.offsets = append(newTail.offsets, w.writer.writeOffset+uint32(bufOffset))
		// Now we can make it available to readers. Note that readers still
		// shouldn't read it until we actually commit to disk (and increment
		// commitIdx) but it's race free for them to now!
		w.tailBlock.Store(&newTail)
	}
	return nil
}

func (w *Writer) finishBlock() error {
	// Work out how much space is left in the block
	bufOffset := len(w.writer.commitBuf)
	absCursor := w.writer.writeOffset + uint32(bufOffset)
	remainingSpace := nextBlockStart(w.info.BlockSize, absCursor) - absCursor

	tail := w.tail()
	idxFrameLen := indexFrameSize(len(tail.offsets))

	// Safety check!
	if remainingSpace < (uint32(idxFrameLen) + blockTrailerLen) {
		return fmt.Errorf("%w: not enough space left in block for index and trailer", io.ErrShortBuffer)
	}
	w.ensureBufCap(int(remainingSpace))

	// Reslice commitBuf to be the whole rest of the block so we don't have to
	// keep updating it's len.
	w.writer.commitBuf = w.writer.commitBuf[:bufOffset+int(remainingSpace)]

	// Write index frame to buffer if there is one!
	indexStart := uint32(0)
	if idxFrameLen > 0 {
		indexStart = absCursor
		if err := writeIndexFrame(w.writer.commitBuf[bufOffset:], tail.offsets); err != nil {
			return err
		}
		bufOffset += idxFrameLen
		remainingSpace -= uint32(idxFrameLen)
	}

	// Write any padding bytes needed before trailer
	padBytes := remainingSpace - blockTrailerLen
	for i := 0; i < int(padBytes); i++ {
		w.writer.commitBuf[bufOffset+i] = 0x0
	}
	bufOffset += int(padBytes)

	// Work out the correct firstIndex. We set it up presuming there would be at
	// least one indexable entry, but if there wasn't we should subtract one.
	firstIdx := tail.firstIndex
	if firstIdx > 0 && idxFrameLen == 0 {
		// No indexable entries so the last index of the last block was the right
		// value.
		firstIdx--
	}

	// Encode trailer
	t := blockTrailer{
		firstIndex: firstIdx,
		batchStart: w.writer.batchStart,
		indexStart: indexStart,
		numEntries: uint32(len(tail.offsets)),
		// We update CRC after writing trailer since it covers the trailer too.
	}
	if err := writeBlockTrailer(w.writer.commitBuf[bufOffset:], t); err != nil {
		return err
	}

	// Checksum over all data written to this block in the current append batch
	// (i.e. whole buffer) minus the last 4 bytes which are where the checksum is
	// stored.
	crc := crc32.Checksum(w.writer.commitBuf[0:len(w.writer.commitBuf)-4], castagnoliTable)
	t.crc = crc
	if err := writeBlockTrailer(w.writer.commitBuf[bufOffset:], t); err != nil {
		return err
	}

	// Write to file
	if err := w.flush(); err != nil {
		return err
	}

	newTail := &tailIndex{
		firstIndex: tail.firstIndex + uint64(len(tail.offsets)),
		blockID:    tail.blockID + 1,
		// We can't re-use the old array because we need to mutate early entries
		// which may now be being read by readers.
		offsets: make([]uint32, 0, initIndexSize(int(w.info.BlockSize))),
	}
	w.tailBlock.Store(newTail)

	return nil
}

func (w *Writer) flush() error {
	// Write to file
	n, err := w.wf.WriteAt(w.writer.commitBuf, int64(w.writer.writeOffset))
	if err == io.EOF && n == len(w.writer.commitBuf) {
		// Writer may return EOF even if it wrote all bytes if it wrote right up to
		// the end of the file. Ignore that case though.
		err = nil
	}
	if err != nil {
		return err
	}

	// Reset writer state ready for next writes
	w.writer.writeOffset += uint32(len(w.writer.commitBuf))
	w.writer.commitBuf = w.writer.commitBuf[:0]
	return nil
}

func (w *Writer) sync() error {
	// Write out current buffer to file
	if err := w.flush(); err != nil {
		return err
	}

	// Sync file
	if err := w.wf.Sync(); err != nil {
		return err
	}

	// Update commitIdx atomically
	tail := w.tail()
	atomic.StoreUint64(&w.commitIdx, tail.firstIndex+uint64(len(tail.offsets))-1)
	return nil
}

// Full returns true if the segment is considered full compared with it's
// pre-allocated size. It is called _after_ append which is expected to have
// worked regardless of the size of the append (i.e. the segment might have to
// grow beyond it's pre-allocated blocks to accommodate the final append).
func (w *Writer) Full() bool {
	// Is tail block beyond the last pre-allocated block in the file?

	// TODO we need to track the actual numBlock separately from the segment info
	// maybe atomically for when we appended. Figure that out when we fix the
	// segment interface with a Seal method so we can report back actual num
	// blocks at seal time.
	tail := w.tail()
	return tail.blockID >= w.info.NumBlocks
}

// LastIndex returns the most recently persisted index in the log. It must
// respond without blocking on append since it's needed frequently by read
// paths that may call it concurrently. Typically this will be loaded from an
// atomic int. If the segment is empty lastIndex should return zero.
func (w *Writer) LastIndex() uint64 {
	return atomic.LoadUint64(&w.commitIdx)
}
