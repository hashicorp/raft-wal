//go:build windows

package metadb

import "golang.org/x/sys/windows"

func syncDir(dir string) error {
	path, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return err
	}
	dirHandle, err := windows.CreateFile(
		path,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return err
	}
	err = windows.FlushFileBuffers(dirHandle)
	closeErr := windows.CloseHandle(dirHandle)
	if err != nil {
		return err
	}
	return closeErr
}
