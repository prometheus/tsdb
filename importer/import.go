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
// converts it into a snapshot/block, and places the newly created snapshot/block in the
// TSDB DB directory, where it is treated like any other block.
func ImportFromFile(filePath string, contentType string, dbPath string, skipTimestampCheck bool, logger log.Logger) error {
	if logger == nil {
		logger = log.NewNopLogger()
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

	metricSamples, minValidTimestamp, maxValidTimestamp, err := parseMetrics(bytes, contentType)
	if err != nil {
		return err
	}

	if !skipTimestampCheck {
		err = verifyIntegration(dbPath, minValidTimestamp, maxValidTimestamp)
		if err != nil {
			return err
		}
	}

	tmpDbDir, err := ioutil.TempDir("", "importer")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDbDir)

	//snapshotPath, err := pushToDisk(metricSamples, tmpDbDir, minValidTimestamp, logger)
	snapshotPaths, err := pushToDisk(metricSamples, tmpDbDir, logger)
	if err != nil {
		return err
	}
	level.Info(logger).Log("msg", "blocks created", "blockPaths", snapshotPaths)

	err = copyToDatabase(tmpDbDir, dbPath)
	if err != nil {
		return err
	}

	return nil
}

// parseMetrics parses metrics formatted in the Prometheus exposition format.
// Returns the metric samples, min timestamp, max timestamp, and error.
func parseMetrics(b []byte, contentType string) ([][]*metricSample, timestamp, timestamp, error) {
	var minValidTimestamp timestamp
	minValidTimestamp = math.MaxInt64
	var maxValidTimestamp timestamp
	maxValidTimestamp = math.MinInt64

	var buckets [][]*metricSample
	var currentBucket []*metricSample
	var startTime time.Time
	var err error
	parser := textparse.New(b, contentType)
	for {
		var ent textparse.Entry
		if ent, err = parser.Next(); err != nil {
			// Error strings are just differently enough across packages,
			// hence this catch-all that just looks for "EOF" in the error
			// string, and if it finds one, it means that the parsing is complete.
			if strings.Contains(strings.ToLower(err.Error()), "eof") {
				err = nil
				break
			}
			// In case the error that we see is not related to the EOF.
			return nil, minValidTimestamp, maxValidTimestamp, err
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
		_, currentTimestampNs, val := parser.Series()

		// Parses converts all timestamps to nanoseconds.
		// TSDB looks for timestamps in milliseconds.
		var currentTimestampMs timestamp
		if currentTimestampNs == nil {
			currentTimestampMs = 0
		} else {
			currentTimestampMs = *currentTimestampNs / 1000
		}

		minValidTimestamp = minInt(minValidTimestamp, currentTimestampMs)
		maxValidTimestamp = maxInt(maxValidTimestamp, currentTimestampMs)

		var lset prom_labels.Labels
		_ = parser.Metric(&lset)

		tsdbLabels := tsdb_labels.FromMap(lset.Map())

		currentTime := time.Unix(currentTimestampMs/1000, 0)
		currentSample := &metricSample{TimestampMs: currentTimestampMs, Value: val, Labels: tsdbLabels}

		if startTime.IsZero() {
			startTime = currentTime
			currentBucket = append(currentBucket, currentSample)
			continue
		}

		timeDelta := currentTime.Sub(startTime)
		if timeDelta.Seconds()*1000 >= BlockDuration {
			startTime = currentTime
			buckets = append(buckets, currentBucket)
			currentBucket = []*metricSample{currentSample}
		} else {
			currentBucket = append(currentBucket, currentSample)
		}
	}
	// Last bucket to be added
	buckets = append(buckets, currentBucket)

	return buckets, minValidTimestamp, maxValidTimestamp, nil
}

// pushToDisk uses the 2h blocks partitioned for us by the parser, and creates
// new blocks.
func pushToDisk(samples [][]*metricSample, dbDir string, logger log.Logger) ([]string, error) {
	paths := make([]string, 0, len(samples))
	for _, bucket := range samples {
		blockPath, err := createBlock(bucket, dbDir, logger)
		if err != nil {
			return nil, err
		}
		paths = append(paths, blockPath)
	}
	return paths, nil
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

// createBlock creates a block from the samples passed to it, and writes it to disk.
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
func verifyIntegration(dbPath string, mint, maxt timestamp) error {
	// If we try to open a regular RW handle on an active TSDB instance,
	// it will fail. Hence, we open a RO handle.
	db, err := tsdb.OpenDBReadOnly(dbPath, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	blocks, err := db.Blocks()
	if err != nil {
		if err.Error() != "no blocks found" {
			return err
		}
	}
	for _, block := range blocks {
		bmint, bmaxt := block.Meta().MinTime, block.Meta().MaxTime
		if maxt > bmint && maxt < bmaxt {
			return OverlappingBlocksError
		}
		if mint > bmint && mint < bmaxt {
			return OverlappingBlocksError
		}
	}
	return nil
}
