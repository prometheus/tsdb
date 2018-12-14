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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/testutil"
)

func TestRepairIndex(t *testing.T) {

	defaultBlockMinTime := int64(1)
	defaultBlockMaxTime := DefaultOptions.BlockRanges[0]
	tests := []struct {
		IndexStatsBad  IndexStats
		IndexStatsGood IndexStats
	}{
		{
			IndexStatsBad: IndexStats{
				ChunksTotal:            5,
				ChunksOverlapingTotal:  1,
				ChunksPartialOutsiders: 1,
				ChunksEntireOutsiders:  1,
				MinTime:                defaultBlockMinTime,
				MaxTime:                defaultBlockMaxTime,
			},
			IndexStatsGood: IndexStats{
				ChunksTotal:            3,
				ChunksOverlapingTotal:  0,
				ChunksPartialOutsiders: 0,
				ChunksEntireOutsiders:  0,
				MinTime:                defaultBlockMinTime,
				MaxTime:                defaultBlockMaxTime,
			},
		},
	}

	for _, test := range tests {
		tmpdir, err := ioutil.TempDir("", "test_scanner")
		testutil.Ok(t, err)
		defer os.RemoveAll(tmpdir)
		testutil.Ok(t, fileutil.CopyDirs(filepath.Join("testdata", "repair_index_chunks"), tmpdir))
		dbCopy, err := Open(tmpdir, nil, nil, DefaultOptions)
		testutil.Ok(t, err)

		blocks := dbCopy.Blocks()
		dbCopy.Close() // Close the db so that the scanner can read/write/delete blocks under windows.

		// Read the block index to make sure it includes invalid chunks.
		testutil.Equals(t, true, len(dbCopy.blocks) > 0)
		for _, block := range blocks {
			stats, err := indexStats(block.Dir())
			testutil.Ok(t, err)
			stats.BlockDir = "" // Reset so that it matches with the expected stats.
			testutil.Equals(t, test.IndexStatsBad, stats)
		}

		// Repair the index.
		scanner, err := NewDBScanner(dbCopy.dir, level.NewFilter(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), level.AllowError()))
		testutil.Ok(t, err)
		unrepairable, _, err := scanner.Indexes()
		testutil.Ok(t, err)
		testutil.Assert(t, len(unrepairable) == 0, "expected 0 unrepairable indexes but got: %+v", unrepairable)

		// Check that all invalid chunks have been removed.
		testutil.Ok(t, dbCopy.reload())
		for _, block := range dbCopy.blocks {
			stats, err := indexStats(block.Dir())
			testutil.Ok(t, err)
			stats.BlockDir = "" // Blockdir will be different so lets set it to "" and compare the rest of the values.
			testutil.Equals(t, test.IndexStatsGood, stats)
		}
	}
}
