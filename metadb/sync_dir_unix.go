//go:build !windows

package metadb

import "os"

func syncDir(dir string) error {
	dirF, err := os.Open(dir)
	if err != nil {
		return err
	}

	err = dirF.Sync()
	closeErr := dirF.Close()
	if err != nil {
		return err
	}
	return closeErr
}
