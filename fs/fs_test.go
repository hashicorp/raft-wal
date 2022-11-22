// Copyright (c) HashiCorp, Inc
// SPDX-License-Identifier: MPL-2.0

package fs

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFS(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-wal-fs-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fs := New()

	// List should return nothing
	files, err := fs.ListDir(tmpDir)
	require.NoError(t, err)
	require.Len(t, files, 0)

	// Create a new file
	wf, err := fs.Create(tmpDir, "00001-abcd1234.wal", 512*1024)
	require.NoError(t, err)
	defer wf.Close()

	// Should be pre-allocated (on supported file systems).
	// TODO work out if this is reliable in CI or if we can detect supported FSs?)
	info, err := os.Stat(filepath.Join(tmpDir, "00001-abcd1234.wal"))
	require.NoError(t, err)
	require.Equal(t, int64(512*1024), info.Size())

	// Should be able to write data in any order
	n, err := wf.WriteAt(bytes.Repeat([]byte{'2'}, 1024), 1024)
	require.NoError(t, err)
	require.Equal(t, 1024, n)

	n, err = wf.WriteAt(bytes.Repeat([]byte{'1'}, 1024), 0)
	require.NoError(t, err)
	require.Equal(t, 1024, n)

	// And past the preallocated end.
	n, err = wf.WriteAt(bytes.Repeat([]byte{'3'}, 1024), 512*1024)
	require.NoError(t, err)
	require.Equal(t, 1024, n)

	// And sync them
	require.NoError(t, wf.Sync())

	// And read them back
	rf, err := fs.OpenReader(tmpDir, "00001-abcd1234.wal")
	require.NoError(t, err)
	defer rf.Close()

	var buf [1024]byte
	n, err = rf.ReadAt(buf[:], 1024)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, byte('2'), buf[0])

	n, err = rf.ReadAt(buf[:], 0)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, byte('1'), buf[0])

	n, err = rf.ReadAt(buf[:], 512*1024)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, byte('3'), buf[0])

	// Read off end is an error
	_, err = rf.ReadAt(buf[:], 513*1024)
	require.ErrorIs(t, err, io.EOF)

	// Should also be able to re-open writable file.
	wf.Close()
	wf, err = fs.OpenWriter(tmpDir, "00001-abcd1234.wal")
	require.NoError(t, err)

	// And write more
	n, err = wf.WriteAt(bytes.Repeat([]byte{'4'}, 1024), 2048)
	require.NoError(t, err)
	require.Equal(t, 1024, n)
	require.NoError(t, wf.Sync())

	// And read back prior and new data through the writer. Read across the old
	// and new data written - first byte is old data rest is new.
	n, err = wf.ReadAt(buf[:], 2047)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, byte('2'), buf[0])
	require.Equal(t, byte('4'), buf[1])

	// The already open reader should also be able to read that newly written data
	n, err = rf.ReadAt(buf[:], 2048)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, byte('4'), buf[0])

	// List should return file now
	files, err = fs.ListDir(tmpDir)
	require.NoError(t, err)
	require.Equal(t, []string{"00001-abcd1234.wal"}, files)

	// Delete should work
	require.NoError(t, fs.Delete(tmpDir, "00001-abcd1234.wal"))

	files, err = fs.ListDir(tmpDir)
	require.NoError(t, err)
	require.Equal(t, []string{}, files)
}

func TestRealFSNoDir(t *testing.T) {
	fs := New()

	_, err := fs.ListDir("/not-a-real-dir")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such file or directory")

	_, err = fs.Create("/not-a-real-dir", "foo", 1024)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such file or directory")

	_, err = fs.OpenReader("/not-a-real-dir", "foo")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such file or directory")

	_, err = fs.OpenWriter("/not-a-real-dir", "foo")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such file or directory")
}
