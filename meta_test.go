package raftwal

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreate_CreatesWork(t *testing.T) {
	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	wal := &wal{}
	wal.createMetaPage(f.Name())

	expectedSize := meta_header_size + 2*meta_page_size
	fi, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, expectedSize, int(fi.Size()))

	fbytes, err := ioutil.ReadAll(f)
	require.NoError(t, err)

	require.Len(t, fbytes, expectedSize)

	require.Equal(t, meta_magic_header, fbytes[:len(meta_magic_header)])

	metaA, err := loadMetaFromBytes(fbytes[meta_header_size : meta_header_size+meta_page_size])
	require.NoError(t, err)
	require.Equal(t, wal.meta, metaA)

	_, err = loadMetaFromBytes(fbytes[meta_header_size+meta_page_size:])
	require.Error(t, err)
	require.Contains(t, err.Error(), "mismatch checksum")
}

func testMeta(t *testing.T, targetVersion int) (*wal, *os.File, func()) {
	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)

	w := &wal{}
	w.createMetaPage(f.Name())

	w.SetUint64([]byte("bar"), 10)
	w.setFirstIndexLocked(321)

	for i := 2; i < targetVersion; i++ {
		v := fmt.Sprintf("val%d", i+1)
		w.Set([]byte("foo"), []byte(v))
	}

	return w, f, func() { os.Remove(f.Name()) }
}

// Test restore when slot B is the latest
func TestMeta_DoubleBuffering_Even(t *testing.T) {
	targetVersion := 4

	t.Run("restore works", func(t *testing.T) {
		w, f, cleanup := testMeta(t, targetVersion)
		defer cleanup()

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.NoError(t, err)

		require.Equal(t, 4, int(nw.meta.Version))
		require.Equal(t, w.meta, nw.meta)
	})

	// simulate when we failed during commiting a newer update
	// in slot A - so falls back to previous meta in slot B
	t.Run("metaA corrupt", func(t *testing.T) {
		w, f, cleanup := testMeta(t, targetVersion)
		defer cleanup()

		f.WriteAt([]byte("randomcorruption"), meta_header_size+2)

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.NoError(t, err)

		em := &meta{}
		*em = *w.meta

		em.Version--
		em.BytesStore = map[string][]byte{
			"foo": []byte("val3"),
		}
		require.Equal(t, 3, int(nw.meta.Version))
		require.Equal(t, em, nw.meta)
	})

	// simulate a failure in commiting this latest update
	// falls back to previous state in slot A
	t.Run("metaB corrupt", func(t *testing.T) {
		w, f, cleanup := testMeta(t, targetVersion)
		defer cleanup()

		f.WriteAt([]byte("randomcorruption"), meta_header_size+meta_page_size+2)

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.NoError(t, err)

		em := &meta{}
		*em = *w.meta

		em.BytesStore = map[string][]byte{
			"foo": []byte("val4"),
		}
		require.Equal(t, 4, int(nw.meta.Version))
		require.Equal(t, em, nw.meta)
	})

}

// Test restore when slot A is the latest
func TestMeta_DoubleBuffering_Odd(t *testing.T) {
	targetVersion := 5

	t.Run("restore works", func(t *testing.T) {
		w, f, cleanup := testMeta(t, targetVersion)
		defer cleanup()

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.NoError(t, err)

		require.Equal(t, 5, int(nw.meta.Version))
		require.Equal(t, w.meta, nw.meta)
	})

	// simulate a failure in commiting this latest update
	// falls back to previous state in slot B
	t.Run("metaA corrupt", func(t *testing.T) {
		w, f, cleanup := testMeta(t, targetVersion)
		defer cleanup()

		f.WriteAt([]byte("randomcorruption"), meta_header_size+2)

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.NoError(t, err)

		em := &meta{}
		*em = *w.meta

		em.BytesStore = map[string][]byte{
			"foo": []byte("val5"),
		}
		require.Equal(t, 5, int(nw.meta.Version))
		require.Equal(t, em, nw.meta)
	})

	// simulate a failure in commiting this latest update
	// falls back to previous state in slot A
	t.Run("metaB corrupt", func(t *testing.T) {
		w, f, cleanup := testMeta(t, targetVersion)
		defer cleanup()

		f.WriteAt([]byte("randomcorruption"), meta_header_size+meta_page_size+2)

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.NoError(t, err)

		em := &meta{}
		*em = *w.meta

		em.Version--
		em.BytesStore = map[string][]byte{
			"foo": []byte("val4"),
		}
		require.Equal(t, 4, int(nw.meta.Version))
		require.Equal(t, em, nw.meta)
	})
}

// Test restore corruptions at different states
func TestMeta_Corruption(t *testing.T) {
	// simulate a failure in commiting this latest update
	// falls back to previous state in slot B
	t.Run("both are corrupt fails", func(t *testing.T) {
		_, f, cleanup := testMeta(t, 5)
		defer cleanup()

		f.WriteAt([]byte("randomcorruption"), meta_header_size+2)
		f.WriteAt([]byte("randomcorruption"), meta_header_size+meta_page_size+2)

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid meta pages")
	})

	t.Run("unexpected header fails", func(t *testing.T) {
		_, f, cleanup := testMeta(t, 5)
		defer cleanup()

		f.WriteAt([]byte("corruption"), 2)

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected header")
	})

	t.Run("incompete initial creation suppressed", func(t *testing.T) {
		_, f, cleanup := testMeta(t, 5)
		defer cleanup()

		f.Truncate(meta_header_size - 2)

		nw := &wal{}
		err := nw.restoreMetaPage(f.Name())
		require.NoError(t, err)

		require.Equal(t, newMeta(), nw.meta)
	})
}
