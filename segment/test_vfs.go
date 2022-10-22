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
	"testing"

	"github.com/hashicorp/go-wal"
)

// testVFS implements wal.VFS for testing.
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
func (fs *testVFS) Create(dir string, name string, size uint64) (wal.WritableFile, error) {
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
func (fs *testVFS) OpenReader(dir string, name string) (wal.ReadableFile, error) {
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
func (fs *testVFS) OpenWriter(dir string, name string) (wal.WritableFile, error) {
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
func testFileFor(t *testing.T, r wal.SegmentReader) *testWritableFile {
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
	buf           []byte
	maxWritten    int
	closed, dirty bool
}

func newTestWritableFile(size int) *testWritableFile {
	return &testWritableFile{
		buf: make([]byte, size),
	}
}

func (f *testWritableFile) Dump() string {
	var buf bytes.Buffer
	d := hex.Dumper(&buf)
	max := f.maxWritten
	if max < 128 {
		max = 128
	}
	_, err := d.Write(f.buf[:max])
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func (f *testWritableFile) WriteAt(p []byte, off int64) (n int, err error) {
	f.dirty = true
	maxOffset := int(off) + len(p)
	if maxOffset > len(f.buf) {
		// re-allocate to simulate appending additional bytes to end of a
		// pre-allocated file.
		nb := make([]byte, maxOffset)
		copy(nb, f.buf)
		f.buf = nb
	}
	copy(f.buf[off:], p)
	if maxOffset > f.maxWritten {
		f.maxWritten = maxOffset
	}
	return len(p), nil
}

func (f *testWritableFile) ReadAt(p []byte, off int64) (n int, err error) {
	if int(off) >= len(f.buf) {
		return 0, io.EOF
	}
	copy(p, f.buf[off:])
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
