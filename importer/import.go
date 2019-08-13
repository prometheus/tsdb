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

package importer

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"

	"github.com/go-kit/kit/log"
	"github.com/otiai10/copy"
	prom_labels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/tsdb"
	tsdb_labels "github.com/prometheus/tsdb/labels"
)

// Implementing the error interface to create a
// constant, which cannot be overridden.
// https://dave.cheney.net/2016/04/07/constant-errors
type Error string

func (e Error) Error() string {
	return string(e)
}

// This error is thrown when we try to merge/add blocks to an existing TSDB instance,
// and the new blocks have a time overlap with the current blocks.
const OverlappingBlocksError = Error("blocks overlap with blocks currently in DB")

// Duration of a block in milliseconds
const BlockDuration = 2 * 60 * 60 * 1000

type timestamp = int64

type metricSample struct {
	TimestampMs timestamp
	Value       float64
	Labels      tsdb_labels.Labels
}

// ImportFromFile imports data from a file formatted according to the Prometheus exposition format,
// converts it into block(s), and places the newly created block(s) in the
// TSDB DB directory, where it is treated like any other block.
func ImportFromFile(filePath string, contentType string, dbPath string, skipTimestampCheck bool, logger log.Logger) error {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	tmpDbDir, err := ioutil.TempDir("", "importer")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDbDir)

	dbMint, dbMaxt, err := getDbTimeLimits(dbPath)
	if err != nil {
		return err
	}

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	blockPaths, err := pushMetrics(bytes, contentType, tmpDbDir, dbMint, dbMaxt, skipTimestampCheck, logger)
	if err != nil {
		return err
	}

	level.Info(logger).Log("msg", "blocks created", "blockPaths", blockPaths)

	err = copyToDatabase(tmpDbDir, dbPath)
	if err != nil {
		return err
	}

	return nil
}

// pushMetrics parses metrics formatted in the Prometheus exposition format,
// and creates corresponding blocks.
// Returns paths to the newly created blocks, and error.
func pushMetrics(b []byte, contentType string, dbPath string, dbMint, dbMaxt timestamp, skipTimestampCheck bool, logger log.Logger) ([]string, error) {
	var minValidTimestamp timestamp
	minValidTimestamp = math.MaxInt64
	var maxValidTimestamp timestamp
	maxValidTimestamp = math.MinInt64

	var blockPaths []string
	var currentBucket []*metricSample
	var startTime time.Time
	var currentTime time.Time
	var err error
	parser := textparse.New(b, contentType)
	for {
		var ent textparse.Entry
		if ent, err = parser.Next(); err != nil {
			// Error strings are just different enough across packages,
			// hence this catch-all that just looks for "EOF" in the error
			// string, and if it finds one, it means that the parsing is complete.
			if strings.Contains(strings.ToLower(err.Error()), "eof") {
				err = nil
				break
			}
			// In case the error that we see is not related to the EOF.
			return nil, err
		}
		switch ent {
		case textparse.EntryType:
			continue
		case textparse.EntryHelp:
			continue
		case textparse.EntryUnit:
			continue
		case textparse.EntryComment:
			continue
		default:
		}
		_, currentTimestampMicroS, val := parser.Series()

		// The text parser converts all timestamps to microseconds.
		// TSDB looks for timestamps in milliseconds.
		var currentTimestampMs timestamp
		if currentTimestampMicroS == nil {
			currentTimestampMs = 0
		} else {
			currentTimestampMs = *currentTimestampMicroS / 1e3
		}

		minValidTimestamp = minInt(minValidTimestamp, currentTimestampMs)
		maxValidTimestamp = maxInt(maxValidTimestamp, currentTimestampMs)

		var lset prom_labels.Labels
		_ = parser.Metric(&lset)

		tsdbLabels := tsdb_labels.FromMap(lset.Map())

		currentTime = time.Unix(currentTimestampMs/1000, 0)
		currentSample := &metricSample{TimestampMs: currentTimestampMs, Value: val, Labels: tsdbLabels}

		if startTime.IsZero() {
			startTime = currentTime
			currentBucket = append(currentBucket, currentSample)
			continue
		}

		timeDelta := currentTime.Sub(startTime)
		if timeDelta.Seconds()*1000 >= BlockDuration {
			startTime = currentTime

			start := int64(startTime.Second() * 1000)
			end := int64(currentTime.Second() * 1000)
			blockPath, err := pushToDisk(currentBucket, dbPath, dbMint, dbMaxt, start, end, skipTimestampCheck, logger)
			if err != nil {
				if err == OverlappingBlocksError {
					level.Warn(logger).Log("msg", fmt.Sprintf("could not merge with range %d to %d as it overlaps with target DB", start, end))
				} else {
					return nil, err
				}
			}
			blockPaths = append(blockPaths, blockPath)
			currentBucket = []*metricSample{currentSample}
		} else {
			currentBucket = append(currentBucket, currentSample)
		}
	}
	// Last bucket to be added
	blockPath, err := pushToDisk(currentBucket, dbPath, dbMint, dbMaxt, int64(startTime.Second()*1000), int64(currentTime.Second()*1000), skipTimestampCheck, logger)
	if err != nil {
		if err == OverlappingBlocksError {
			level.Warn(logger).Log("msg", fmt.Sprintf("could not merge with range %d to %d as it overlaps with target DB", int64(startTime.Second()*1000), int64(currentTime.Second()*1000)))
		} else {
			return nil, err
		}
	}
	blockPaths = append(blockPaths, blockPath)
	return blockPaths, nil
}

// pushToDisk verifies the sample is compatible with the target TSDB instance, and then creates a new block
// from the sample data.
func pushToDisk(samples []*metricSample, dbDir string, dbMint, dbMaxt, startTime, endTime timestamp, skipTimestampCheck bool, logger log.Logger) (string, error) {
	if !skipTimestampCheck {
		err := verifyIntegration(dbMint, dbMaxt, startTime, endTime)
		if err != nil {
			return "", OverlappingBlocksError
		}
	}
	return createBlock(samples, dbDir, logger)
}

// createHead creates a TSDB writer head to write the sample data to.
func createHead(samples []*metricSample, chunkRange int64, logger log.Logger) (*tsdb.Head, error) {
	head, err := tsdb.NewHead(nil, logger, nil, chunkRange)
	if err != nil {
		return nil, err
	}
	app := head.Appender()
	for _, sample := range samples {
		_, err = app.Add(sample.Labels, sample.TimestampMs, sample.Value)
		if err != nil {
			return nil, err
		}
	}
	err = app.Commit()
	if err != nil {
		return nil, err
	}
	return head, nil
}

// createBlock creates a 2h block from the samples passed to it, and writes it to disk.
func createBlock(samples []*metricSample, dir string, logger log.Logger) (string, error) {
	// 2h head block
	head, err := createHead(samples, BlockDuration, logger)
	if err != nil {
		return "", err
	}
	compactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, logger, tsdb.DefaultOptions.BlockRanges, nil)
	if err != nil {
		return "", err
	}

	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return "", err
	}

	ulid, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime()+1, nil)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, ulid.String()), nil
}

// copyToDatabase copies the snapshot created to the TSDB DB directory.
// TSDB operates such that it automatically picks up the newly created
// snapshot(s) and treats them as it would any other block.
func copyToDatabase(snapshotPath string, dbPath string) error {
	return copy.Copy(snapshotPath, dbPath)
}

// verifyIntegration returns an error if the any of the blocks in the DB intersect with
// the provided time range.
func verifyIntegration(dbmint, dbmaxt, mint, maxt timestamp) error {
	if dbmaxt >= mint && dbmaxt <= maxt {
		return OverlappingBlocksError
	}
	if dbmint >= mint && dbmint <= maxt {
		return OverlappingBlocksError
	}
	return nil
}

// getDbTimeLimits returns the first and last timestamps of the target TSDB instance.
func getDbTimeLimits(dbPath string) (timestamp, timestamp, error) {
	mint := int64(math.MinInt64)
	maxt := int64(math.MaxInt64)
	// If we try to open a regular RW handle on an active TSDB instance,
	// it will fail. Hence, we open a RO handle.
	db, err := tsdb.OpenDBReadOnly(dbPath, nil)
	if err != nil {
		return mint, maxt, err
	}
	defer db.Close()
	blocks, err := db.Blocks()
	if err != nil {
		if err.Error() != "no blocks found" {
			return mint, maxt, err
		}
	}
	for idx, block := range blocks {
		bmint, bmaxt := block.Meta().MinTime, block.Meta().MaxTime
		if idx == 0 {
			mint, maxt = bmint, bmaxt
		} else {
			mint = minInt(mint, bmint)
			maxt = maxInt(maxt, bmaxt)
		}
	}
	return mint, maxt, nil
}
