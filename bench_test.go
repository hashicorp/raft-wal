package raftwal

import (
	"io/ioutil"
	"os"
	"testing"

	raftbench "github.com/hashicorp/raft/bench"
	"github.com/stretchr/testify/require"
)

func BenchmarkWALStore_FirstIndex(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.FirstIndex(b, store)
}

func BenchmarkWALStore_LastIndex(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.LastIndex(b, store)
}

func BenchmarkWALStore_GetLog(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.GetLog(b, store)
}

func BenchmarkWALStore_StoreLog(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.StoreLog(b, store)
}

func BenchmarkWALStore_StoreLogs(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.StoreLogs(b, store)
}

func BenchmarkWALStore_DeleteRange(b *testing.B) {
	b.SkipNow()

	store := testWALStore(b)
	defer store.Close()

	raftbench.DeleteRange(b, store)
}

func BenchmarkWALStore_Set(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.Set(b, store)
}

func BenchmarkWALStore_Get(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.Get(b, store)
}

func BenchmarkWALStore_SetUint64(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.SetUint64(b, store)
}

func BenchmarkWALStore_GetUint64(b *testing.B) {
	store := testWALStore(b)
	defer store.Close()

	raftbench.GetUint64(b, store)
}

func testWALStore(b *testing.B) *wal {
	dir, err := ioutil.TempDir("", "raftwaltests-")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(dir) })

	wal, err := NewWAL(dir, LogConfig{
		NoSync: true,
	})
	require.NoError(b, err)

	return wal
}
