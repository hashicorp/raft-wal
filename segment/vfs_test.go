// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package segment

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/raft-wal/types"
)

// testVFS implements types.VFS for testing.
type testVFS struct {
	dir   string
	files map[string]*testWritableFile
	trash map[string]*testWritableFile

	listErr   error
	createErr error
	deleteErr error
	openErr   error
}

func newTestVFS() *testVFS {
	return &testVFS{
		files: make(map[string]*testWritableFile),
		trash: make(map[string]*testWritableFile),
	}
}

// ListDir returns a list of all files in the specified dir in lexicographical
// order. If the dir doesn't exist, it must return an error. Empty array with
// nil error is assumed to mean that the directory exists and was readable,
// but contains no files.
func (fs *testVFS) ListDir(dir string) ([]string, error) {
	if fs.listErr != nil {
		return nil, fs.listErr
	}
	if err := fs.setDir(dir); err != nil {
		return nil, err
	}

	files := make([]string, 0, len(fs.files))
	for name := range fs.files {
		files = append(files, name)
	}
	sort.Strings(files)
	return files, nil
}

func (fs *testVFS) setDir(dir string) error {
	if fs.dir == "" {
		fs.dir = dir
		return nil
	}
	if fs.dir != dir {
		return fmt.Errorf("VFS called for different dir. Prev=%s Current=%s", fs.dir, dir)
	}
	return nil
}

// Create creates a new file with the given name. If a file with the same name
// already exists an error is returned. If a non-zero size is given,
// implementations should make a best effort to pre-allocate the file to be
// that size. The dir must already exist and be writable to the current
// process.
func (fs *testVFS) Create(dir string, name string, size uint64) (types.WritableFile, error) {
	if fs.createErr != nil {
		return nil, fs.createErr
	}
	if err := fs.setDir(dir); err != nil {
		return nil, err
	}
	_, ok := fs.files[name]
	if ok {
		return nil, fmt.Errorf("file already exists")
	}
	f := newTestWritableFile(int(size))
	fs.files[name] = f
	return f, nil
}

// Delete indicates the file is no longer required. Typically it should be
// deleted from the underlying system to free disk space.
func (fs *testVFS) Delete(dir string, name string) error {
	if fs.deleteErr != nil {
		return fs.deleteErr
	}
	if err := fs.setDir(dir); err != nil {
		return err
	}
	tf, ok := fs.files[name]
	if !ok {
		return nil
	}
	fs.trash[name] = tf
	delete(fs.files, name)
	return nil
}

// OpenReader opens an existing file in read-only mode. If the file doesn't
// exist or permission is denied, an error is returned, otherwise no checks
// are made about the well-formedness of the file, it may be empty, the wrong
// size or corrupt in arbitrary ways.
func (fs *testVFS) OpenReader(dir string, name string) (types.ReadableFile, error) {
	if fs.openErr != nil {
		return nil, fs.openErr
	}
	if err := fs.setDir(dir); err != nil {
		return nil, err
	}
	f, ok := fs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return f, nil
}

// OpenWriter opens a file in read-write mode. If the file doesn't exist or
// permission is denied, an error is returned, otherwise no checks are made
// about the well-formedness of the file, it may be empty, the wrong size or
// corrupt in arbitrary ways.
func (fs *testVFS) OpenWriter(dir string, name string) (types.WritableFile, error) {
	if fs.openErr != nil {
		return nil, fs.openErr
	}
	if err := fs.setDir(dir); err != nil {
		return nil, err
	}
	f, ok := fs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return f, nil
}

// testFileFor is a helper for reaching inside our interface types to access
// the underlying "file".
func testFileFor(t *testing.T, r types.SegmentReader) *testWritableFile {
	t.Helper()

	switch v := r.(type) {
	case *Reader:
		return v.rf.(*testWritableFile)
	case *Writer:
		return v.wf.(*testWritableFile)
	default:
		t.Fatalf("Invalid SegmentReader implementation passed: %t", r)
		return nil
	}
}

type testWritableFile struct {
	buf           atomic.Value // []byte
	maxWritten    int
	lastSyncStart int
	closed, dirty bool
}

func newTestWritableFile(size int) *testWritableFile {
	wf := &testWritableFile{}
	wf.buf.Store(make([]byte, 0, size))
	return wf
}

func (f *testWritableFile) getBuf() []byte {
	return f.buf.Load().([]byte)
}

func (f *testWritableFile) Dump() string {
	var buf bytes.Buffer
	d := hex.Dumper(&buf)
	max := f.maxWritten
	if max < 128 {
		max = 128
	}
	bs := f.getBuf()
	_, err := d.Write(bs[:max])
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func (f *testWritableFile) WriteAt(p []byte, off int64) (n int, err error) {
	if !f.dirty {
		f.lastSyncStart = int(off)
	}
	f.dirty = true
	maxOffset := int(off) + len(p)
	buf := f.getBuf()
	if maxOffset > len(buf) {
		// re-allocate to simulate appending additional bytes to end of a
		// pre-allocated file.
		nb := make([]byte, maxOffset)
		copy(nb, buf)
		buf = nb
	} else if off < int64(len(buf)) {
		// If this write is to an offset that was already visible to readers (less
		// than len(buf)) we can't write because that's racey, need to copy whole
		// buffer to mutate it safely.
		nb := make([]byte, len(buf), cap(buf))
		copy(nb, buf)
		buf = nb
	}
	copy(buf[off:], p)
	if maxOffset > f.maxWritten {
		f.maxWritten = maxOffset
	}
	// Atomically replace the slice to allow readers to see the new appended data
	// or new backing array if we reallocated.
	f.buf.Store(buf)
	return len(p), nil
}

func (f *testWritableFile) ReadAt(p []byte, off int64) (n int, err error) {
	buf := f.getBuf()
	// Note we treat the whole cap of buf as "in" the file
	if int(off) >= cap(buf) {
		return 0, io.EOF
	}
	if off >= int64(len(buf)) {
		// Offset is within capacity of "file" but after the maximum visible byte so
		// just return empty bytes.
		for i := 0; i < len(p); i++ {
			p[i] = 0
		}
		return len(p), nil
	}
	copy(p, buf[off:])
	return len(p), nil
}

func (f *testWritableFile) Close() error {
	f.closed = true
	return nil
}

func (f *testWritableFile) Sync() error {
	f.dirty = false
	return nil
}
