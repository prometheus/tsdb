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

type LoopBackFs struct {
	pathfs.FileSystem
}

type HookFs struct {
	Original   string
	Mountpoint string
	FsName     string
	LoopBackFs
	hook Hook
}

func NewHookFs(original string, mountpoint string, hook Hook) (*HookFs, error) {
	hookfs := &HookFs{
		Original:   original,
		Mountpoint: mountpoint,
		FsName:     "hookfs",
		LoopBackFs: LoopBackFs{pathfs.NewLoopbackFileSystem(original)},
		hook:       hook,
	}
	return hookfs, nil
}

func (h *HookFs) String() string {
	return fmt.Sprintf("HookFs{Original=%s, Mountpoint=%s, FsName=%s, Underlying fs=%s, hook=%s}",
		h.Original, h.Mountpoint, h.FsName, h.LoopBackFs.String(), h.hook)
}

func (h *HookFs) Rename(oldName string, newName string, context *fuse.Context) fuse.Status {
	preHooked, err := h.hook.PreRename(oldName, newName)
	if preHooked {
		if err != nil {

			return fuse.ToStatus(err)
		}
	}

	status := h.LoopBackFs.Rename(oldName, newName, context)

	postHooked, err := h.hook.PostRename(oldName, newName)
	if postHooked {
		if err != nil {
			return fuse.ToStatus(err)
		}
	}
	return status
}

func (h *HookFs) NewServe() (*fuse.Server, error) {
	server, err := newHookServer(h)
	if err != nil {
		return nil, err
	}

	return server, nil
}

// Tests will want to run this in a  goroutine.
func (h *HookFs) Start(server *fuse.Server) {
	server.Serve()
}

func newHookServer(hookfs *HookFs) (*fuse.Server, error) {
	opts := &nodefs.Options{
		NegativeTimeout: time.Second,
		AttrTimeout:     time.Second,
		EntryTimeout:    time.Second,
	}
	pathFsOpts := &pathfs.PathNodeFsOptions{ClientInodes: true}
	pathFs := pathfs.NewPathNodeFs(hookfs, pathFsOpts)
	conn := nodefs.NewFileSystemConnector(pathFs.Root(), opts)
	originalAbs, _ := filepath.Abs(hookfs.Original)
	mOpts := &fuse.MountOptions{
		AllowOther: true,
		Name:       hookfs.FsName,
		FsName:     originalAbs,
	}
	server, err := fuse.NewServer(conn.RawFS(), hookfs.Mountpoint, mOpts)
	if err != nil {
		return nil, err
	}

	return server, nil
}

type Server struct {
	server     *fuse.Server
	original   string
	mountpoint string
}

func NewServer(t *testing.T, original, mountpoint string, hook Hook) (clean func()) {
	fs, err := NewHookFs(original, mountpoint, hook)
	testutil.Ok(t, err)

	server, err := fs.NewServe()
	testutil.Ok(t, err)

	// Async start fuse server, and it will be stopped when calling fuse.Unmount method.
	go func() {
		fs.Start(server)
	}()

	testutil.Ok(t, server.WaitMount())

	return func() {
		if err = server.Unmount(); err != nil {
			testutil.Ok(t, err)
		}

		testutil.Ok(t, os.RemoveAll(mountpoint))
		testutil.Ok(t, os.RemoveAll(original))
		return
	}
}
