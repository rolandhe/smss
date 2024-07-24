//go:build !linux

package standard

func posixFadvise(fd int) error {
	return nil
}
