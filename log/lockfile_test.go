package log

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransaction_Txn_Flow(t *testing.T) {
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// create a new lock file
	f, err := newLockFile(filepath.Join(dir, "entry_test"), true)
	require.NoError(t, err)
	defer f.Close()

	c := command{Type: cmdTruncatingTail, Index: 321}
	err = f.startTransaction(c)
	require.NoError(t, err)

	txnCmd, err := f.currentTransaction()
	require.NoError(t, err)
	require.NotNil(t, txnCmd)
	require.Equal(t, c, *txnCmd)

	err = f.commit()
	require.NoError(t, err)

	txnCmd, err = f.currentTransaction()
	require.NoError(t, err)
	require.Nil(t, txnCmd)
}

func TestTransaction_PostRecovery(t *testing.T) {
	dir, err := ioutil.TempDir("", "testsegment")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// create a new lock file
	f, err := newLockFile(filepath.Join(dir, "entry_test"), true)
	require.NoError(t, err)
	defer f.Close()

	c := command{Type: cmdTruncatingTail, Index: 321}
	err = f.startTransaction(c)
	require.NoError(t, err)
	f.Close()

	nf, err := newLockFile(filepath.Join(dir, "entry_test"), true)
	require.NoError(t, err)

	txnCmd, err := nf.currentTransaction()
	require.NoError(t, err)
	require.NotNil(t, txnCmd)
	require.Equal(t, c, *txnCmd)

	err = nf.commit()
	require.NoError(t, err)

	txnCmd, err = nf.currentTransaction()
	require.NoError(t, err)
	require.Nil(t, txnCmd)
}
