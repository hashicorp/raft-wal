//go:build !windows

package fs

import "os"

func syncDir(dir string) error {
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
