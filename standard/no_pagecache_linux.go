//go:build linux

package standard

import (
	"syscall"
)

const (
	FADV_DONTNEED = 0x4
)

// posixFadviseFunc 包装 posix_fadvise 系统调用
func posixFadviseFunc(fd int, offset int64, length int64, advice int) error {
	_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), uintptr(offset), uintptr(length), uintptr(advice), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func posixFadvise(fd int) error {
	return posixFadviseFunc(fd, 0, 0, FADV_DONTNEED)
}
