// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fuse

import "syscall"

// Hook is the base interface for user-written hooks.
// You have to implement XXXHooK(e.g. `FailingRenameHook`, ..).
type Hook interface {
	// PreRename is called before real `rename()` method.
	PreRename(oldPatgh string, newPath string) (hooked bool, err error)

	// PostRename is called after the real `rename()` method.
	PostRename(oldPatgh string, newPath string) (hooked bool, err error)

	// PreOpen is called before real `open()` method.
	PreOpen(path string, flags uint32) (hooked bool, err error)

	// PostOpen is called after the real `open()` method.
	PostOpen(realRetCode int32) (hooked bool, err error)

	// PreRead is called before real `read()` method.
	PreRead(path string, length int64, offset int64) (hooked bool, err error)

	// PostRead is called after real `read()` method.
	PostRead(realRetCode int32, realBuf []byte) (hooked bool, err error)

	// PreWrite is called before real `write()` method.
	PreWrite(path string, buf []byte, offset int64) (hooked bool, err error)

	// PostWrite is called after real `write()` method.
	PostWrite(realRetCode int32) (hooked bool, err error)

	// PreMkdir is called before real `mkdir()` method.
	PreMkdir(path string, mode uint32) (hooked bool, err error)

	// PostMkdir is called after real `mkdir()` method.
	PostMkdir(realRetCode int32) (hooked bool, err error)

	// PreRmdir is called before real `rmdir()` method.
	PreRmdir(path string) (hooked bool, err error)

	// PostRmdir is called after real `rmdir()` method.
	PostRmdir(realRetCode int32) (hooked bool, err error)

	// PreOpenDir is called before real `opendir()` method.
	PreOpenDir(path string) (hooked bool, err error)

	// PostOpenDir is called after real `opendir()` method.
	PostOpenDir(realRetCode int32) (hooked bool, err error)

	// PreFsync is called before real `fsync()` method.
	PreFsync(path string, flags uint32) (hooked bool, err error)

	// PostFsync is called after real `fsync()` method.
	PostFsync(realRetCode int32) (hooked bool, err error)
}

// EmptyHook implements a Hook that returns false for every operation.
type EmptyHook struct{}

// PreRename is called before real `rename()` method, it provides a default implement.
func (h EmptyHook) PreRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}

// PostRename is called after real `rename()` method, it provides a default implement.
func (h EmptyHook) PostRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}

// PreOpen is called after the real `open()` method, it provides a default implement.
func (h EmptyHook) PreOpen(path string, flags uint32) (hooked bool, err error) {
	return false, nil
}

// PostOpen is called after the real `open()` method, it provides a default implement.
func (h EmptyHook) PostOpen(realRetCode int32) (hooked bool, err error) {
	return false, nil
}

// PreRead is called before real `read()` method, it provides a default implement.
func (h EmptyHook) PreRead(path string, length int64, offset int64) (hooked bool, err error) {
	return false, nil
}

// PostRead is called after real `read()` method, it provides a default implement.
func (h EmptyHook) PostRead(realRetCode int32, realBuf []byte) (hooked bool, err error) {
	return false, nil
}

// PreWrite is called before real `write()` method, it provides a default implement.
func (h EmptyHook) PreWrite(path string, buf []byte, offset int64) (hooked bool, err error) {
	return false, nil
}

// PostWrite is called after real `write()` method, it provides a default implement.
func (h EmptyHook) PostWrite(realRetCode int32) (hooked bool, err error) {
	return false, nil
}

// PreMkdir is called before real `mkdir()` method, it provides a default implement.
func (h EmptyHook) PreMkdir(path string, mode uint32) (hooked bool, err error) {
	return false, nil
}

// PostMkdir is called after real `mkdir()` method, it provides a default implement.
func (h EmptyHook) PostMkdir(realRetCode int32) (hooked bool, err error) {
	return false, nil
}

// PreRmdir is called before real `rmdir()` method, it provides a default implement.
func (h EmptyHook) PreRmdir(path string) (hooked bool, err error) {
	return false, nil
}

// PostRmdir is called after real `rmdir()` method, it provides a default implement.
func (h EmptyHook) PostRmdir(realRetCode int32) (hooked bool, err error) {
	return false, nil
}

// PreOpenDir is called before real `opendir()` method, it provides a default implement.
func (h EmptyHook) PreOpenDir(path string) (hooked bool, err error) {
	return false, nil
}

// PostOpenDir is called after real `opendir()` method, it provides a default implement.
func (h EmptyHook) PostOpenDir(realRetCode int32) (hooked bool, err error) {
	return false, nil
}

// PreFsync is called before real `fsync()` method, it provides a default implement.
func (h EmptyHook) PreFsync(path string, flags uint32) (hooked bool, err error) {
	return false, nil
}

// PostFsync is called after real `fsync()` method, it provides a default implement.
func (h EmptyHook) PostFsync(realRetCode int32) (hooked bool, err error) {
	return false, nil
}

// FailingRenameHook implements the hook interface and fails on renaming operations.
type FailingRenameHook struct {
	// EmptyHook is embedded so that the FailingRenameHook fully implements the hook interface.
	EmptyHook
}

// PreRename fails for pre rename operation.
func (h FailingRenameHook) PreRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return true, syscall.EIO
}

// PostRename fails for post rename operation.
func (h FailingRenameHook) PostRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}
