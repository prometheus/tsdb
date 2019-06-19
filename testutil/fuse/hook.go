// Copyright 2019 The qiffang Authors
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

import (
	"syscall"
)

// Hook is the base interface for user-written hooks.
// You have to implement XXXHooK(e.g. `TestRenameHook`, ..).
type Hook interface {
	// If hooked is true, the real `rename()` would not be called.
	PreRename(oldPatgh string, newPath string) (hooked bool, err error)

	// If hooked is true, it will triggered after the real `rename()`.
	PostRename(oldPatgh string, newPath string) (hooked bool, err error)
}

// TestRenameHook is called on rename.
type TestRenameHook struct {
	// Add the Empty hook so the that this struct implements the hook interface.
	EmptyHook
}

// These will take precedence over the `EmptyHook.PreRename()`.
func (h TestRenameHook) PreRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return true, syscall.EIO
}

// These will take precedence over the `EmptyHook.PostRename()`.
func (h TestRenameHook) PostRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}

// EmptyHook implements a Hook that returns false for every operation.
type EmptyHook struct{}

// PreRename is called on before real `rename()` method.
func (h EmptyHook) PreRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}

// PostRename is called on after real `rename()` method.
func (h EmptyHook) PostRename(oldPatgh string, newPath string) (hooked bool, err error) {
	return false, nil
}
