// +build linux darwin

// Copyright 2017 The Prometheus Authors
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
package testutil

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/prometheus/common/log"
	"os"
	"syscall"
	"testing"
)

func NewFuseServer(t *testing.T, original, mountpoint string, hook Hook) (interface{}, error) {
	createDirIfAbsent(original)
	createDirIfAbsent(mountpoint)
	fs, err := NewHookFs(original, mountpoint, hook)
	if err != nil {
		return nil, err
	}

	server, err := fs.NewServe()
	if err != nil {
		return nil, err
	}

	//async start fuse server
	go func() {
		fs.Start(server)
	}()

	return server, nil
}

func CleanUp(s interface{}, mountpoint string, original string) {
	server := s.(*fuse.Server)
	server.Unmount()
	syscall.Unmount(mountpoint, -1)

	os.RemoveAll(mountpoint)
	os.RemoveAll(original)
	log.Info("Done")
}

func createDirIfAbsent(name string) {
	_, err := os.Stat(name)
	if err != nil {
		os.Mkdir(name, os.ModePerm)
	}
}
