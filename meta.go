// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package wal

import (
	"github.com/benbjohnson/immutable"
)

// MetaStore is the interface we need to some persistent, crash safe backend. We
// implement it with BoltDB for real usage but the interface allows alternatives
// to be used, or tests to mock out FS access.
type MetaStore interface {
	// Load loads the existing persisted state. If there is no existing state
	// implementations are expected to create initialize new storage and return an
	// empty state.
	Load(dir string) (PersistentState, error)

	// CommitState must atomically replace all persisted metadata in the current
	// store with the set provided. It must not return until the data is persisted
	// durably and in a crash-safe way otherwise the guarantees of the WAL will be
	// compromised. The WAL will only ever call this in a single thread at one
	// time and it will never be called concurrently with Load however it may be
	// called concurrently with Get/SetStable operations.
	CommitState(PersistentState) error

	// GetStable returns a value from stable store or nil if it doesn't exist. May
	// be called concurrently by multiple threads.
	GetStable(key []byte) ([]byte, error)

	// SetStable stores a value from stable store or nil if it doesn't exist. May
	// be called concurrently with GetStable.
	SetStable(key, value []byte) error
}

// PersistentState represents the WAL file metadata we need to store reliably to
// recover on restart.
type PersistentState struct {
	NextSegmentID uint64
	Segments      []SegmentInfo
}

// meta is an immutable snapshot of the state of the log. Modifications must be
// made by copying and modifying the copy. This is easy enough because segments
// is an immutable map so changing and re-assigning to the clone won't impact
// the original map, and tail is just a pointer that can be mutated in the
// shallow clone. Note that methods called on the tail segmentWriter may mutate
// it's state so must only be called while holding the WAL's writeLock.
type meta struct {
	nextSegmentID uint64
	segments      *immutable.SortedMap[uint64, SegmentInfo]
	tail          segmentWriter
}

// Commit converts the in-memory state into a PersistentState.
func (s *meta) Persistent() PersistentState {
	segs := make([]SegmentInfo, 0, s.segments.Len())
	it := s.segments.Iterator()
	for !it.Done() {
		_, s, _ := it.Next()
		segs = append(segs, s)
	}
	return PersistentState{
		NextSegmentID: s.nextSegmentID,
		Segments:      segs,
	}
}

func (s meta) getLog(index uint64) ([]byte, error) {
	// Check the tail writer first
	if s.tail != nil {
		raw, err := s.tail.getLog(index)
		if err != nil && err != ErrNotFound {
			// Return actual errors since they might mask the fact that index really
			// is in the tail but failed to read for some other reason.
			return nil, err
		}
		if err == nil {
			// No error means we found it and just need to decode.
			return raw, nil
		}
		// Not in the tail segment, fall back to searching previous segments.
	}

	seg, err := s.findSegmentReader(index)
	if err != nil {
		return nil, err
	}

	return seg.getLog(index)
}

// findSegmentReader searches the segment tree for the segment that contains the
// log at index idx. It may return the tail segment which may not in fact
// contain idx if idx is larger than the last written index. Typically this is
// called after already checking with the tail writer whether the log is in
// there which means the caller can be sure it's not going to return the tail
// segment.
func (s meta) findSegmentReader(idx uint64) (segmentReader, error) {

	if s.segments.Len() == 0 {
		return nil, ErrNotFound
	}

	// Search for a segment with baseIndex.
	it := s.segments.Iterator()

	// The baseIndex we want is the first one lower or equal to idx. Seek gets us
	// to the first result equal or greater so we are either at it (if equal) or
	// on the one _after_ the one we need. We step back since that's most likely
	it.Seek(idx)
	// The first call to Next/Prev actually returns the node the iterator is
	// currently on (which is probably the one after the one we want) but in some
	// edge cases we might actually want this one. Rather than reversing back and
	// coming forward again, just check both this and the one before it.
	_, seg, ok := it.Prev()
	if ok && seg.BaseIndex > idx {
		_, seg, ok = it.Prev()
	}

	// We either have the right segment or it doesn't exist.
	if ok && seg.MinIndex <= idx && (seg.MaxIndex == 0 || seg.MaxIndex >= idx) {
		return seg.r, nil
	}

	return nil, ErrNotFound
}

func (s meta) getTailInfo() *SegmentInfo {
	it := s.segments.Iterator()
	it.Last()
	_, tail, ok := it.Next()
	if !ok {
		return nil
	}
	return &tail
}

func (s *meta) append(entries []logEntry) error {
	return s.tail.append(entries)
}

func (s meta) firstIndex() uint64 {
	it := s.segments.Iterator()
	_, seg, ok := it.Next()
	if !ok {
		return 0
	}
	return seg.MinIndex
}

func (s meta) lastIndex() uint64 {
	tailIdx := s.tail.lastIndex()
	if tailIdx > 0 {
		return tailIdx
	}
	// Current tail is empty, so the largest log is the MaxIndex of the previous
	// segment which must be the same as the tail's BaseIndex - 1 or zero.
	tailSeg := s.getTailInfo()
	if tailSeg == nil || tailSeg.BaseIndex == 0 {
		return 0
	}
	return tailSeg.BaseIndex - 1
}
