// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alecthomas/atomic"

	"github.com/benbjohnson/immutable"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

var (
	_ raft.LogStore    = &WAL{}
	_ raft.StableStore = &WAL{}

	ErrNotFound = errors.New("Log entry not found")
)

// WAL is a write-ahead log suitable for github.com/hashicorp/raft.
type WAL struct {
	dir    string
	codec  Codec
	vfs    VFS
	metaDB MetaStore
	log    hclog.Logger

	// readerFn/writerFn allows stubbing out segment readers and writers to make
	// testing core WAL logic simpler and decoupled from the actual file format.
	readerFn func() segmentReader
	writerFn func() segmentWriter

	// s is the current state of the WAL files. It is an immutable snapshot that
	// can be accessed without a lock when reading. We only support a single
	// writer so all methods that mutate either the WAL state or append to the
	// tail of the log must hold the writeMu until they complete all changes.
	s atomic.Value[meta]

	// writeMu must be held when modifying s or while appending to the tail.
	// Although we take care never to let readers block writer, we still only
	// allow a single writer to be updating the meta state at once. The mutex must
	// be held before s is loaded until all modifications to s or appends to the
	// tail are complete.
	writeMu sync.Mutex
}

type walOpt func(*WAL)

// Open attempts to open the WAL stored in dir. If there are no existing WAL
// files a new WAL will be initialized there. The dir must already exist and be
// readable and writable to the current process. If existing files are found,
// recovery is attempted. If recovery is not possible an error is returned,
// otherwise the returned *WAL is in a state ready for use.
func Open(dir string, opts ...walOpt) (*WAL, error) {

	w := &WAL{}
	// Apply options
	for _, opt := range opts {
		opt(w)
	}
	if err := w.applyDefaultsAndValidate(); err != nil {
		return nil, err
	}

	// List first because this is guaranteed to error if the dir doesn't exist.
	files, err := w.vfs.ListDir(w.dir)
	if err != nil {
		return nil, err
	}

	// Load or create metaDB
	persisted, err := w.metaDB.Load(w.dir)
	if err != nil {
		return nil, err
	}

	// Build the state
	segMap := immutable.NewSortedMap[uint64, SegmentInfo](nil)
	for _, si := range persisted.Segments {
		// Validate that this segment file exists
		f, err := w.vfs.OpenReader(w.dir, si.fileName())
		if err != nil {
			return nil, err
		}

		// Verify we can decode the entries.
		// TODO: support multiple decoders to allow rotating codec.
		if si.Codec != w.codec.ID() {
			return nil, fmt.Errorf("segment file %s: uses an unknown codec", si.fileName())
		}

		// Create segmentReader and validate
		sr := w.readerFn()
		if err := sr.validate(f, si); err != nil {
			return nil, fmt.Errorf("invalid segment file %s: %w", si.fileName(), err)
		}
		// Store the open reader to get logs from
		si.r = sr

		// Build the state
		segMap = segMap.Set(si.BaseIndex, si)
	}

	newMeta := meta{
		nextSegmentID: persisted.NextSegmentID,
		segments:      segMap,
	}

	// Recover tail segment (if there is one to recover)
	it := segMap.Iterator()
	it.Last()
	_, tailSI, ok := it.Next()
	if ok {
		if !tailSI.SealTime.IsZero() {
			// We should never have committed a state where we didn't have an unsealed
			// tail segment. While it's technically possible to recover from here, we
			// shouldn't ever need to unless user has tampered with the meta db
			// somehow which is not supported - they could just tamper it right and
			// also unset the seal time if they want to do that!
			return nil, fmt.Errorf("invalid WAL state: tail segment is already sealed")
		}

		// Verify we can encode/decode entries the same way. TODO: support multiple
		// decoders to allow rotating codec. That means this will have to support
		// recovering a file with an older codec before switching to the new one for
		// later segments or something.
		if tailSI.Codec != w.codec.ID() {
			return nil, fmt.Errorf("segment file %s: uses an unknown codec", tailSI.fileName())
		}

		// Recover the partial tail from the reader.
		wf, err := w.vfs.OpenWriter(w.dir, tailSI.fileName())
		if err != nil {
			return nil, err
		}
		newMeta.tail = w.writerFn()
		if err := newMeta.tail.recover(wf, tailSI); err != nil {
			return nil, err
		}
	} else {
		// We have an empty log, create a new segment. We use baseIndex of 0 even
		// though the first append might be much higher - we'll allow that since we
		// know we have no records yet and so lastIndex will also be 0.
		seg := w.newSegment(newMeta.nextSegmentID, 0)
		newMeta.nextSegmentID++
		newMeta.segments = newMeta.segments.Set(0, seg)

		// Persist the new meta to "commit" it even before we create the file so we
		// don't attempt to recreate files with duplicate IDs on a later failure.
		if err := w.metaDB.CommitState(newMeta.Persistent()); err != nil {
			return nil, err
		}

		// Create the new segment file
		wf, err := w.vfs.Create(w.dir, seg.fileName(), seg.preallocatedSize())
		if err != nil {
			return nil, err
		}
		newMeta.tail = w.writerFn()
		if err := newMeta.tail.init(wf, seg); err != nil {
			return nil, err
		}
	}

	// Store the in-memory state (it was already persisted if we modified it
	// above)
	w.s.Store(newMeta)

	// Delete any unused segment files left over after a crash. We rely on the
	// files being sorted lexicographically.
	toDelete := findOldSegments(newMeta, files)

	// TODO possibly make this async? Typically it's a no-op and simpler to reason
	// about and test if it's sync though.
	w.deleteFiles(toDelete)

	return w, nil
}

func (w *WAL) applyDefaultsAndValidate() error {
	// Check if an external codec has been used that it's not using a reserved ID.
	if w.codec != nil && w.codec.ID() < FirstExternalCodecID {
		return fmt.Errorf("codec is using a reserved ID (below %d)", FirstExternalCodecID)
	}

	// Defaults
	w.log = hclog.Default().Named("wal")
	// TODO

	return nil
}

// newSegment creates a SegmentInfo with the passed ID and baseIndex, filling in
// the segment parameters based on the current WAL configuration.
func (w *WAL) newSegment(ID, baseIndex uint64) SegmentInfo {
	return SegmentInfo{
		ID:        ID,
		BaseIndex: baseIndex,
		MinIndex:  baseIndex,
		MaxIndex:  0, // Zero until sealed

		// TODO make these configurable
		Codec:      CodecBinaryV1,
		BlockSize:  1024 * 1024, // 1 MiB
		NumBlocks:  64,          // 64 MiB per segment
		CreateTime: time.Now(),
	}
}

// FirstIndex returns the first index written. 0 for no entries.
func (w *WAL) FirstIndex() (uint64, error) {
	return w.s.Load().firstIndex(), nil
}

// LastIndex returns the last index written. 0 for no entries.
func (w *WAL) LastIndex() (uint64, error) {
	return w.s.Load().lastIndex(), nil
}

// GetLog gets a log entry at a given index.
func (w *WAL) GetLog(index uint64, log *raft.Log) error {

	raw, err := w.s.Load().getLog(index)
	if err != nil {
		return err
	}

	// Decode the log
	return w.codec.Decode(raw, log)
}

// StoreLog stores a log entry.
func (w *WAL) StoreLog(log *raft.Log) error {
	return w.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (w *WAL) StoreLogs(logs []*raft.Log) error {
	if len(logs) < 1 {
		return nil
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	s := w.s.Load()

	// Verify monotonicity since we assume it
	lastIdx := s.lastIndex()

	// Encode logs
	encoded := make([]logEntry, len(logs))
	// TODO consider pooling these?
	var buf bytes.Buffer
	for i, l := range logs {
		if l.Index != (lastIdx + 1) {
			return fmt.Errorf("non-monotonic log entries: tried to append index %d after %d", logs[0].Index, lastIdx)
		}
		if err := w.codec.Encode(l, &buf); err != nil {
			return err
		}
		encoded[i].Data = buf.Bytes()
		encoded[i].Index = l.Index
		lastIdx = l.Index
		buf.Reset()
	}
	if err := s.tail.append(encoded); err != nil {
		return err
	}
	// Check if we need to roll logs
	if s.tail.full() {
		if err := w.rotateSegmentLocked(); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange deletes a range of log entries. The range is inclusive.
// Implements raft.LogStore. Note that we only support deleting ranges that are
// a suffix or prefix of the log.
func (w *WAL) DeleteRange(min uint64, max uint64) error {
	if min >= max {
		// Empty inclusive range.
		return nil
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	s := w.s.Load()

	// Work out what type of truncation this is.
	first, last := s.firstIndex(), s.lastIndex()
	switch {
	// |min----max|
	//               |first====last|
	// or
	//                |min----max|
	// |first====last|
	case max < first || min > last:
		// None of the range exists at all so a no-op
		return nil

	// |min----max|
	//      |first====last|
	// or
	// |min--------------max|
	//      |first====last|
	// or
	//   |min--max|
	//   |first====last|
	case min <= first: // max >= first implied by the first case not matching
		// Note we allow head truncations where max > last which effectively removes
		// the entire log.
		return w.truncateHeadLocked(max)

	//    |min----max|
	// |first====last|
	// or
	//  |min--------------max|
	// |first====last|
	case max >= last: // min <= last implied by first case not matching
		return w.truncateTailLocked(min)

	//    |min----max|
	// |first========last|
	default:
		// Everything else is a neither a suffix nor prefix so unsupported.
		return fmt.Errorf("only suffix or prefix ranges may be deleted from log")
	}
}

// Set implements raft.StableStore
func (w *WAL) Set(key []byte, val []byte) error {
	return w.metaDB.SetStable(key, val)
}

// Get implements raft.StableStore
func (w *WAL) Get(key []byte) ([]byte, error) {
	return w.metaDB.GetStable(key)
}

// SetUint64 implements raft.StableStore. We assume the same key space as Set
// and Get so the caller is responsible for ensuring they don't call both Set
// and SetUint64 for the same key.
func (w *WAL) SetUint64(key []byte, val uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], val)
	return w.Set(key, buf[:])
}

// GetUint64 implements raft.StableStore. We assume the same key space as Set
// and Get. We assume that the key was previously set with `SetUint64` and
// returns an undefined value (possibly with nil error) if not.
func (w *WAL) GetUint64(key []byte) (uint64, error) {
	raw, err := w.Get(key)
	if err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		// Not set, return zero per interface contract
		return 0, nil
	}
	// At least a tiny bit of checking is possible
	if len(raw) != 8 {
		return 0, fmt.Errorf("GetUint64 called on a non-uint64 key")
	}
	return binary.LittleEndian.Uint64(raw), nil
}

func (w *WAL) rotateSegmentLocked() error {
	s := w.s.Load()

	// Mark current tail as sealed in segments
	tail := s.getTailInfo()
	if tail == nil {
		// Can't happen
		return fmt.Errorf("no tail found during rotate")
	}

	// Note that tail is a copy since it's a value type not a pointer stored in
	// segments map, so we can mutate it safely and update the immutable state
	// with our version.
	tail.SealTime = time.Now()
	tail.MaxIndex = s.tail.lastIndex()

	// This is a shallow copy but we'll update segments on mutations anyway since
	// it's immutable.
	newState := s

	// Update the old tail with the seal time etc.
	newState.segments = newState.segments.Set(tail.ID, *tail)

	if err := w.createNextSegment(&newState); err != nil {
		return err
	}

	// Finally we are all done. Update in-memory state
	w.s.Store(newState)
	return nil
}

// createNextSegment is passes a mutable copy of the new state ready to have a
// new segment appended. newState must be a copy, taken under write lock which
// is still held by the caller and its segments map must contain all non-tail
// segments that should be in the log, all must be sealed at this point. The new
// segment's baseIndex will be the current last-segment's MaxIndex (or 0 if
// non).
func (w *WAL) createNextSegment(newState *meta) error {
	// Find existing sealed tail
	tail := newState.getTailInfo()

	// If there is no tail, next baseIndex is 0
	maxSealedIndex := uint64(0)
	if tail != nil {
		maxSealedIndex = tail.MaxIndex
	}

	// Create a new segment
	newTail := w.newSegment(newState.nextSegmentID, maxSealedIndex)
	newState.nextSegmentID++
	newState.segments = newState.segments.Set(newTail.ID, newTail)
	newState.nextSegmentID++

	// Commit the new meta to disk before we create new files so that we never
	// leave a file with ID >= the persisted nextSegmentID lying around.
	if err := w.metaDB.CommitState(newState.Persistent()); err != nil {
		return err
	}

	// Now create the new segment file and open it ready for writing.
	f, err := w.vfs.Create(w.dir, newTail.fileName(), newTail.preallocatedSize())
	if err != nil {
		return err
	}

	// Create the writer
	newState.tail = w.writerFn()
	if err := newState.tail.init(f, newTail); err != nil {
		return err
	}

	// Also cache the reader/log getter which is also the writer. We don't bother
	// reopening read only since we assume we have exclusive access anyway and
	// only use this read-only interface once the segment is sealed.
	newTail.r = newState.tail

	// We need to re-insert it since newTail is a copy not a reference
	newState.segments = newState.segments.Set(newTail.ID, newTail)

	return nil
}

func (w *WAL) truncateHeadLocked(newMin uint64) error {
	s := w.s.Load()

	// Shallow copy to update
	newState := s

	// Iterate the segments to find any that are entirely deleted.
	toDelete := make([]string, 0, 1)
	it := newState.segments.Iterator()

	for !it.Done() {
		_, seg, _ := it.Next()

		if seg.MaxIndex >= newMin {
			// We're done
			break
		}

		toDelete = append(toDelete, seg.fileName())
		newState.segments = newState.segments.Delete(seg.ID)
	}

	// Load the tail which may have just changed!
	tail := newState.getTailInfo()
	if tail != nil {
		// Update the MinIndex on the (possibly new) tail
		tail.MinIndex = newMin
		newState.segments = newState.segments.Set(tail.ID, *tail)
	}

	// Commit updates to meta
	if err := w.metaDB.CommitState(newState.Persistent()); err != nil {
		return err
	}

	// Atomically update in-memory state
	w.s.Store(newState)

	// Delete any files if we need to
	w.deleteFiles(toDelete)

	return nil
}

func (w *WAL) truncateTailLocked(newMax uint64) error {
	s := w.s.Load()

	// Shallow copy to update
	newState := s

	// Reverse iterate the segments to find any that are entirely deleted.
	toDelete := make([]string, 0, 1)
	it := newState.segments.Iterator()
	it.Last()

	for !it.Done() {
		_, seg, _ := it.Prev()

		if seg.BaseIndex <= newMax {
			// We're done
			break
		}

		toDelete = append(toDelete, seg.fileName())
		newState.segments = newState.segments.Delete(seg.ID)
	}

	tail := newState.getTailInfo()
	if tail != nil {
		// Check that the tail is sealed (it won't be if we didn't need to remove
		// the actual partial tail above).
		if tail.SealTime.IsZero() {
			tail.SealTime = time.Now()
		}
		// Update the MaxIndex
		tail.MaxIndex = newMax

		// And update the tail in the new state
		newState.segments = newState.segments.Set(tail.ID, *tail)
	}

	// Create the new tail segment
	if err := w.createNextSegment(&newState); err != nil {
		return err
	}

	// Delete any old files.
	w.deleteFiles(toDelete)

	return nil
}

func (w *WAL) deleteFiles(toDelete []string) {
	for _, fname := range toDelete {
		if err := w.vfs.Delete(w.dir, fname); err != nil {
			// This is not fatal. We can continue just old files might need manual
			// cleanup somehow.
			w.log.Error("failed to delete old segment", "file", fname, "err", err)
		}
	}
}
