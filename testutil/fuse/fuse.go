// +build linux darwin

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

import (
	"fmt"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type loopBackFs struct {
	pathfs.FileSystem
}

// hookFs implements the fuse FileSystem interface and allows injecting hooks for different operations.
// Its main purpose is to provide a way to inject file system errors.
type hookFs struct {
	original   string
	mountpoint string
	fsName     string
	loopBackFs
	hook Hook
}

// String returns the string representation of the mounted points.
func (h *hookFs) String() string {
	return fmt.Sprintf("HookFs{Original=%s, Mountpoint=%s, FsName=%s, Underlying fs=%s, hook=%s}",
		h.original, h.mountpoint, h.fsName, h.loopBackFs.String(), h.hook)
}

// Rename calls the injected rename hooks if those exist or the underlying loopback fs Rename api.
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
	return fuse.NewServer(conn.RawFS(), h.mountpoint, mOpts)
}

// Server is a fuse server proxy.
type Server struct {
	server     *fuse.Server
	original   string
	mountpoint string
}

// NewServer creates a fuse server and attaches it to the given `mountpoint` directory.
// The caller should not forget to close the server to release the mountpoints.
func NewServer(original, mountpoint string, hook Hook) (*Server, error) {
	fs := &hookFs{
		original:   original,
		mountpoint: mountpoint,
		fsName:     "hookfs",
		loopBackFs: loopBackFs{pathfs.NewLoopbackFileSystem(original)},
		hook:       hook,
	}

	server, err := fs.newServer()
	if err != nil {
		return nil, err
	}

	// Async start fuse server, and it will be stopped when calling `fuse.Unmount()` method.
	go func() {
		server.Serve()
	}()

	return &Server{
		server:     server,
		mountpoint: mountpoint,
		original:   original,
	}, server.WaitMount()
}

// Close releases the mountpoints and return false if the unmount fails.
// When the mountpoint has open files it tries to force unmount.
func (s *Server) Close() error {
	err := s.server.Unmount()
	if err != nil {
		return s.forceUnmount()
	}

	return nil
}

// forceUnmount forces to unmount even when there are still open files.
func (s *Server) forceUnmount() (err error) {
	delay := time.Duration(0)
	for try := 0; try < 5; try++ {
		err = syscall.Unmount(s.mountpoint, unmountFlag())
		if err == nil {
			break
		}

		// Sleep for a bit. This is not pretty, but there is
		// no way we can be certain that the kernel thinks all
		// open files have already been closed.
		delay = 2*delay + 10*time.Millisecond
		time.Sleep(delay)
	}

	return err
}

// unmountFlag returns platform dependent force unmount flag.
func unmountFlag() int {
	if runtime.GOOS == "darwin" {
		return -1
	}

	return 0x1
}
