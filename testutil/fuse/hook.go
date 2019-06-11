package fuse

import (
	"fmt"
	"syscall"
)

type Hook interface {
	PreRename(oldPatgh string, newPath string) (hooked bool, err error)
	PostRename(oldPatgh string, newPath string) (hooked bool, err error)
}

type TestRenameHook struct {
	EmptyHook // Add the Empty hook so the that this struct implements the hook interface.
}

// These will take precedence over the `testutil.EmptyHook`
func (h TestRenameHook) PreRename(oldPatgh string, newPath string) (hooked bool, err error) {
	fmt.Printf("renamed file from %s to %s \n", oldPatgh, newPath)
	return true, syscall.EIO
}
func (h TestRenameHook) PostRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}

type EmptyHook struct{}

func (h EmptyHook) PreRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}
func (h EmptyHook) PostRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}
