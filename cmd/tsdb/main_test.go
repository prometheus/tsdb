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

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/prometheus/tsdb"
	testutildb "github.com/prometheus/tsdb/testutil/db"
)

func createTestRODBWithBlock(t *testing.T) (*tsdb.DBReadOnly, func()) {
	tmpdir, err := ioutil.TempDir("", "test")
	if err != nil {
		os.RemoveAll(tmpdir)
		t.Error(err)
	}

	safeDBOptions := *tsdb.DefaultOptions
	safeDBOptions.RetentionDuration = 0

	testutildb.CreateBlock(nil, tmpdir, testutildb.GenSeries(1, 1, 0, 1))
	db, err := tsdb.Open(tmpdir, nil, nil, &safeDBOptions)
	if err != nil {
		os.RemoveAll(tmpdir)
		t.Error(err)
	}
	if err = db.Close(); err != nil {
		t.Error(err)
	}

	dbRO, err := tsdb.OpenDBReadOnly(tmpdir, nil)
	if err != nil {
		t.Error(err)
	}

	return dbRO, func() {
		os.RemoveAll(tmpdir)
	}
}

func TestCLIPrintBlocks(t *testing.T) {
	db, closeFn := createTestRODBWithBlock(t)
	defer closeFn()

	var b bytes.Buffer
	hr := false
	tw := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	// Set table header
	_, err := fmt.Fprintln(&b, printBlocksTableHeader)
	if err != nil {
		t.Error(err)
	}

	// Test table header
	actual := b.String()
	expected := fmt.Sprintln(printBlocksTableHeader)
	if expected != actual {
		t.Errorf("expected (%#v) != actual (%#v)", expected, actual)
	}

	// Set table contents
	blocks, err := db.Blocks()
	if err != nil {
		t.Error(err)
	}
	meta := blocks[0].Meta()

	_, err = fmt.Fprintf(&b,
		"%v\t%v\t%v\t%v\t%v\t%v\n",
		meta.ULID,
		getFormatedTime(meta.MinTime, &hr),
		getFormatedTime(meta.MaxTime, &hr),
		meta.Stats.NumSamples,
		meta.Stats.NumChunks,
		meta.Stats.NumSeries,
	)

	if err != nil {
		t.Error(err)
	}

	// Test table contents
	var actualStdout bytes.Buffer
	blocks, err = db.Blocks()
	if err != nil {
		t.Error(err)
	}
	printBlocks(&actualStdout, blocks, &hr)

	actual = actualStdout.String()
	actual = strings.Replace(actual, " ", "", -1)
	actual = strings.Replace(actual, "\t", "", -1)
	actual = strings.Replace(actual, "\n", "", -1)

	expected = b.String()
	expected = strings.Replace(expected, " ", "", -1)
	expected = strings.Replace(expected, "\t", "", -1)
	expected = strings.Replace(expected, "\n", "", -1)

	if expected != actual {
		t.Errorf("expected (%#v) != actual (%#v)", expected, actual)
	}
}
