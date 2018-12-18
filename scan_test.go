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

package tsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/tsdb/testutil"
)

func TestScanning(t *testing.T) {
	// Create some blocks to work with.
	tmp := testutil.NewTemporaryDirectory("scanTest", t)
	defer tmp.Close()
	block := createPopulatedBlock(t, tmp.Path(), 1, 1, 10)
	defer block.Close()
	b := createPopulatedBlock(t, tmp.Path(), 1, 10, 20)
	defer b.Close()

	scanner, err := NewDBScanner(tmp.Path(), log.NewLogfmtLogger(os.Stderr))
	testutil.Ok(t, err)

	// Test that the scanner reports all current blocks as healthy.
	corr, err := scanner.Index()
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(corr))

	corr, err = scanner.Tombstones()
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(corr))

	corr, err = scanner.Meta()
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(corr))

	corrO, err := scanner.Overlapping()
	testutil.Ok(t, err)
	testutil.Equals(t, 0, len(corrO))

	// Corrupt the block Meta file and check that the scanner reports it.
	f, err := os.OpenFile(filepath.Join(block.Dir(), metaFilename), os.O_WRONLY, 0666)
	testutil.Ok(t, err)
	_, err = f.Write([]byte{0})
	testutil.Ok(t, err)
	f.Close()

	corr, err = scanner.Meta()
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(corr))
	for _, blocks := range corr {
		for _, fname := range blocks {
			testutil.Equals(t, block.Dir(), fname)
		}
	}

	// Corrupt the block Index and check that the scanner reports it.
	f, err = os.OpenFile(filepath.Join(block.Dir(), indexFilename), os.O_WRONLY, 0666)
	testutil.Ok(t, err)
	_, err = f.Write([]byte{0})
	testutil.Ok(t, err)
	f.Close()

	corr, err = scanner.Index()
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(corr))
	for _, blocks := range corr {
		for _, fname := range blocks {
			testutil.Equals(t, block.Dir(), fname)
		}
	}

	// Corrupt the block Tombstone and check that the scanner reports it.
	f, err = os.OpenFile(filepath.Join(block.Dir(), tombstoneFilename), os.O_WRONLY, 0666)
	testutil.Ok(t, err)
	_, err = f.Write([]byte{0})
	testutil.Ok(t, err)
	testutil.Ok(t, f.Close())

	corr, err = scanner.Tombstones()
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(corr))
	for _, blocks := range corr {
		for _, fname := range blocks {
			testutil.Equals(t, block.Dir(), fname)
		}
	}

	// Create an overlapping block and check that the scanner reports it.
	overlapExp := createPopulatedBlock(t, tmp.Path(), 1, 15, 20)
	defer overlapExp.Close()
	corrO, err = scanner.Overlapping()
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(corrO))
	for overlap, blocks := range corrO {
		for _, overlapAct := range blocks[1:] { // Skip the original block that overlaps.
			testutil.Equals(t, overlapExp.Dir(), overlapAct.Dir())
			testutil.Equals(t, overlapExp.Meta().MinTime, overlap.Min)
			testutil.Equals(t, overlapExp.Meta().MaxTime, overlap.Max)
		}
	}
}
