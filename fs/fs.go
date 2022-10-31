// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package fs

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"

	"github.com/coreos/etcd/pkg/fileutil"

	"github.com/hashicorp/go-wal"
)

// FS implements the wal.VFS interface using GO's built in OS
// Filesystem (and a few helpers).
type FS struct{}

func New() *FS {
	return &FS{}
}

// ListDir returns a list of all files in the specified dir in lexicographical
// order. If the dir doesn't exist, it must return an error. Empty array with
// nil error is assumed to mean that the directory exists and was readable,
// but contains no files.
func (fs *FS) ListDir(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(files))
	for i, f := range files {
		if f.IsDir() {
			continue
		}
		names[i] = f.Name()
	}
	return names, nil
}

// Create creates a new file with the given name. If a file with the same name
// already exists an error is returned. If a non-zero size is given,
// implementations should make a best effort to pre-allocate the file to be
// that size. The dir must already exist and be writable to the current
// process.
func (fs *FS) Create(dir string, name string, size uint64) (wal.WritableFile, error) {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		return nil, err
	}
	// We just created the file. Preallocate it's size.
	if size > 0 {
		if size > math.MaxInt32 {
			return nil, fmt.Errorf("maximum file size is %d bytes", math.MaxInt32)
		}
		if err := fileutil.Preallocate(f, int64(size), true); err != nil {
			f.Close()
			return nil, err
		}
	}
	// Fsync that thing to make sure it's real and it's metadata if we
	// preallocated will service a crash.
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}

	// We also need to fsync the parent dir otherwise a crash might loose the new
	// file or it's updated size.
	if err := fs.syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// Delete indicates the file is no longer required. Typically it should be
// deleted from the underlying system to free disk space.
func (fs *FS) Delete(dir string, name string) error {
	if err := os.Remove(filepath.Join(dir, name)); err != nil {
		return err
	}
	// Make sure parent directory metadata is fsynced too before we call this
	// "done".
	return fs.syncDir(dir)
}

// OpenReader opens an existing file in read-only mode. If the file doesn't
// exist or permission is denied, an error is returned, otherwise no checks
// are made about the well-formedness of the file, it may be empty, the wrong
// size or corrupt in arbitrary ways.
func (fs *FS) OpenReader(dir string, name string) (wal.ReadableFile, error) {
	return os.OpenFile(filepath.Join(dir, name), os.O_RDONLY, os.FileMode(0644))
}

// OpenWriter opens a file in read-write mode. If the file doesn't exist or
// permission is denied, an error is returned, otherwise no checks are made
// about the well-formedness of the file, it may be empty, the wrong size or
// corrupt in arbitrary ways.
func (fs *FS) OpenWriter(dir string, name string) (wal.WritableFile, error) {
	return os.OpenFile(filepath.Join(dir, name), os.O_RDWR, os.FileMode(0644))
}

func (fs *FS) syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
