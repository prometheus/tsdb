// +build linux darwin

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
	"github.com/prometheus/tsdb/testutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type loopBackFs struct {
	pathfs.FileSystem
}

// A FUSE filesystem that shunts all request to an underlying file system.
// Its main purpose is to provide test coverage.
type hookFs struct {
	original   string
	mountpoint string
	fsName     string
	loopBackFs
	hook Hook
}

func newHookFs(original string, mountpoint string, hook Hook) (*hookFs, error) {
	hookfs := &hookFs{
		original:   original,
		mountpoint: mountpoint,
		fsName:     "hookfs",
		loopBackFs: loopBackFs{pathfs.NewLoopbackFileSystem(original)},
		hook:       hook,
	}
	return hookfs, nil
}

// String implements hanwen/go-fuse/fuse/pathfs.FileSystem. You are not expected to call h manually.
func (h *hookFs) String() string {
	return fmt.Sprintf("HookFs{Original=%s, Mountpoint=%s, FsName=%s, Underlying fs=%s, hook=%s}",
		h.original, h.mountpoint, h.fsName, h.loopBackFs.String(), h.hook)
}

// Rename implements hanwen/go-fuse/fuse/pathfs.FileSystem. You are not expected to call h manually.
func (h *hookFs) Rename(oldName string, newName string, context *fuse.Context) fuse.Status {
	preHooked, err := h.hook.PreRename(oldName, newName)
	if preHooked {
		if err != nil {

			return fuse.ToStatus(err)
		}
	}

	status := h.loopBackFs.Rename(oldName, newName, context)

	postHooked, err := h.hook.PostRename(oldName, newName)
	if postHooked {
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	return status
}

func (h *hookFs) newServer() (*fuse.Server, error) {
	opts := &nodefs.Options{
		NegativeTimeout: time.Second,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
	}
	pathFsOpts := &pathfs.PathNodeFsOptions{ClientInodes: true}
	pathFs := pathfs.NewPathNodeFs(h, pathFsOpts)
	conn := nodefs.NewFileSystemConnector(pathFs.Root(), opts)
	originalAbs, _ := filepath.Abs(h.original)
	mOpts := &fuse.MountOptions{
		AllowOther: true,
		Name:       h.fsName,
		FsName:     originalAbs,
	}
	server, err := fuse.NewServer(conn.RawFS(), h.mountpoint, mOpts)
	if err != nil {
		return nil, err
	}

	return server, nil
}

// NewServer creates a fuse server and attaches it to the given `mountpoint` directory.
// It returns a function to `unmount` the given `mountpoint` directory.
func NewServer(t *testing.T, original, mountpoint string, hook Hook) (clean func()) {
	fs, err := newHookFs(original, mountpoint, hook)
	testutil.Ok(t, err)

	server, err := fs.newServer()
	testutil.Ok(t, err)

	// Async start fuse server, and it will be stopped when calling `fuse.Unmount()` method.
	go func() {
		server.Serve()
	}()

	testutil.Ok(t, server.WaitMount())

	return func() {
		if err = server.Unmount(); err != nil {
			testutil.Ok(t, err)
		}

		testutil.Ok(t, os.RemoveAll(mountpoint))
		testutil.Ok(t, os.RemoveAll(original))
	}
}
