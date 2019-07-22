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

package main

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/testutil"
)

func TestCLI(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(tmpdir))
	}()

	safeDBOptions := *tsdb.DefaultOptions
	safeDBOptions.RetentionDuration = 0

	testutil.CreateBlock(t, tmpdir, testutil.GenSeries(1, 1, 0, 1))
	db, err := tsdb.Open(tmpdir, nil, nil, &safeDBOptions)
	defer func() {
		testutil.Ok(t, db.Close())
	}()
	hr := true

	printBlocks(db.Blocks(), &hr)
}
