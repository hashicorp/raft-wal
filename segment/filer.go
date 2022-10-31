// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"fmt"
	"strings"

	"github.com/hashicorp/raft-wal/types"
)

const (
	segmentFileSuffix      = ".wal"
	segmentFileNamePattern = "%020d-%016x" + segmentFileSuffix
)

// Filer implements the abstraction for managing a set of segment files in a
// directory. It uses a VFS to abstract actual file system operations for easier
// testing.
type Filer struct {
	dir string
	vfs types.VFS
}

// NewFiler creates a Filer ready for use.
func NewFiler(dir string, vfs types.VFS) *Filer {
	return &Filer{
		dir: dir,
		vfs: vfs,
	}
}

// FileName returns the formatted file name expected for this segment.
// SegmentFiler implementations could choose to ignore this but it's here to
func FileName(i types.SegmentInfo) string {
	return fmt.Sprintf(segmentFileNamePattern, i.BaseIndex, i.ID)
}

// Create adds a new segment with the given info and returns a writer or an
// error.
func (f *Filer) Create(info types.SegmentInfo) (types.SegmentWriter, error) {
	if info.BaseIndex == 0 {
		return nil, fmt.Errorf("BaseIndex must be greater than zero")
	}
	fname := FileName(info)

	wf, err := f.vfs.Create(f.dir, fname, uint64(info.SizeLimit))
	if err != nil {
		return nil, err
	}

	return createFile(info, wf)
}

// RecoverTail is called on an unsealed segment when re-opening the WAL it
// will attempt to recover from a possible crash. It will either return an
// error, or return a valid segmentWriter that is ready for further appends.
func (f *Filer) RecoverTail(info types.SegmentInfo) (types.SegmentWriter, error) {
	fname := FileName(info)

	wf, err := f.vfs.OpenWriter(f.dir, fname)
	if err != nil {
		return nil, err
	}

	return recoverFile(info, wf)
}

// Open an already sealed segment for reading. Open may validate the file's
// header and return an error if it doesn't match the expected info.
func (f *Filer) Open(info types.SegmentInfo) (types.SegmentReader, error) {
	fname := FileName(info)

	rf, err := f.vfs.OpenReader(f.dir, fname)
	if err != nil {
		return nil, err
	}

	return openReader(info, rf)
}

// List returns the set of segment IDs currently stored. It's used by the WAL
// on recovery to find any segment files that need to be deleted following a
// unclean shutdown. The returned map is a map of ID -> BaseIndex. BaseIndex
// is returned to allow subsequent Delete calls to be made.
func (f *Filer) List() (map[uint64]uint64, error) {
	segs := make(map[uint64]uint64)

	files, err := f.vfs.ListDir(f.dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file, segmentFileSuffix) {
			continue
		}
		// Parse BaseIndex and ID from the file name
		var bIdx, id uint64
		n, err := fmt.Sscanf(file, segmentFileNamePattern, &bIdx, &id)
		if err != nil {
			return nil, types.ErrCorrupt
		}
		if n != 2 {
			// Misnamed segment files with the right suffix indicates a bug or
			// tampering, we can't be sure what's happened to the data.
			return nil, types.ErrCorrupt
		}
		segs[id] = bIdx
	}

	return segs, nil
}

// Delete removes the segment with given baseIndex and id if it exists. Note
// that baseIndex is technically redundant since ID is unique on it's own. But
// in practice we name files (or keys) with both so that they sort correctly.
// This interface allows a  simpler implementation where we can just delete
// the file if it exists without having to scan the underlying storage for a.
func (f *Filer) Delete(baseIndex uint64, ID uint64) error {
	fname := fmt.Sprintf(segmentFileNamePattern, baseIndex, ID)
	return f.vfs.Delete(f.dir, fname)
}
