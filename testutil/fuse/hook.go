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
