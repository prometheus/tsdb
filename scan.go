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
	"fmt"
	"sort"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// Scanner provides an interface for scanning different building components of a block.
type Scanner interface {
	// Tombstones returns a list of block paths with invalid Tombstones grouped by the error.
	Tombstones() (map[string][]string, error)
	// Meta returns a list of block paths with invalid Meta file grouped by the error.
	Meta() (map[string][]string, error)
	// Index returns a list of block paths with invalid Index grouped by the error.
	Index() (map[string][]string, error)
	// Overlapping returns a list of blocks with overlapping time ranges.
	Overlapping() (Overlaps, error)
	// Dir returns the scanned directory.
	Dir() string
}

// DBScanner is the main struct for the scanner.
type DBScanner struct {
	db     *DB
	logger log.Logger
}

// NewDBScanner initializes a new database scanner.
func NewDBScanner(dir string, l log.Logger) (*DBScanner, error) {
	blocks, err := blockDirs(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "listing blocks")
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("directory doesn't include any blocks:%v", dir)
	}
	dbScanner := &DBScanner{
		db: &DB{
			dir:    dir,
			logger: l,
			opts:   &Options{},
		},
		logger: l,
	}
	return dbScanner, nil
}

func (s *DBScanner) Dir() string {
	return s.db.Dir()
}

func (s *DBScanner) Tombstones() (map[string][]string, error) {
	dirs, err := blockDirs(s.db.dir)
	if err != nil {
		return nil, err
	}

	inv := make(map[string][]string)
	for _, dir := range dirs {
		if _, err = readTombstones(dir); err != nil {
			inv[err.Error()] = append(inv[err.Error()], dir)
		}
	}
	return inv, nil
}

func (s *DBScanner) Meta() (map[string][]string, error) {
	dirs, err := blockDirs(s.db.dir)
	if err != nil {
		return nil, err
	}

	inv := make(map[string][]string)
	for _, dir := range dirs {
		if _, err = readMetaFile(dir); err != nil {
			inv[err.Error()] = append(inv[err.Error()], dir)
		}
	}
	return inv, nil
}

func (s *DBScanner) Overlapping() (Overlaps, error) {
	dirs, err := blockDirs(s.db.dir)
	if err != nil {
		return nil, err
	}

	var blocks []*Block
	for _, dir := range dirs {
		if meta, err := readMetaFile(dir); err == nil {
			blocks = append(blocks, &Block{
				meta: *meta,
				dir:  dir,
			})
		}
	}
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Meta().MinTime < blocks[j].Meta().MinTime
	})
	return OverlappingBlocks(blocks), nil
}

func (s *DBScanner) Index() (map[string][]string, error) {
	inv := make(map[string][]string)
	dirs, err := blockDirs(s.db.dir)
	if err != nil {
		return nil, err
	}

	for _, dir := range dirs {
		stats, err := indexStats(dir)
		if err != nil {
			inv[err.Error()] = append(inv[err.Error()], dir)
			continue
		}

		if stats.ErrSummary() != nil {
			inv[err.Error()] = append(inv[err.Error()], dir)
			continue
		}
	}
	return inv, nil
}

func indexStats(bdir string) (IndexStats, error) {
	block, err := OpenBlock(bdir, nil)
	if err != nil {
		return IndexStats{}, errors.Wrap(err, "open block")
	}
	defer block.Close()

	indexr, err := block.Index()
	if err != nil {
		return IndexStats{}, errors.Wrap(err, "open block index")
	}
	defer indexr.Close()

	stats := IndexStats{
		MinTime:  block.Meta().MinTime,
		MaxTime:  block.Meta().MaxTime,
		BlockDir: bdir,
	}

	p, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		return IndexStats{}, errors.Wrap(err, "get all postings")
	}
	var (
		lastLset labels.Labels
		lset     labels.Labels
		chks     []chunks.Meta
	)

	for p.Next() {
		lastLset = append(lastLset[:0], lset...)

		id := p.At()

		if err = indexr.Series(id, &lset, &chks); err != nil {
			return IndexStats{}, errors.Wrap(err, "read series")

		}

		if len(lset) == 0 {
			return IndexStats{}, errors.Errorf("empty label set detected for series %d", id)
		}
		if lastLset != nil && labels.Compare(lset, lastLset) <= 0 {
			return IndexStats{}, errors.Errorf("series are the same or out of order - current:%v  previous:%v", lset, lastLset)
		}
		l0 := lset[0]
		for _, l := range lset[1:] {
			if l.Name <= l0.Name {
				return IndexStats{}, errors.Errorf("out-of-order label set %s for series %d", lset, id)
			}
			l0 = l
		}
		if len(chks) == 0 {
			return IndexStats{}, errors.Errorf("empty chunks for series %d", id)
		}

		ooo := 0

		stats.ChunksTotal += len(chks)

		for i, c := range chks {
			if i > 0 {
				// Overlapping chunks.
				if c.OverlapsClosedInterval(chks[i-1].MinTime, chks[i-1].MaxTime) {
					ooo++
				}
			}

			if !c.OverlapsClosedInterval(block.Meta().MinTime, block.Meta().MaxTime) {
				stats.ChunksEntireOutsiders++
			} else if c.MinTime < block.Meta().MinTime || c.MaxTime > block.Meta().MaxTime {
				stats.ChunksPartialOutsiders++

			}

		}
		if ooo > 0 {
			stats.ChunksOverlapingTotal += ooo
		}
	}
	if p.Err() != nil {
		return IndexStats{}, errors.Wrap(err, "walk postings")
	}

	return stats, nil
}

// IndexStats holds useful index counters to asses the index health.
type IndexStats struct {
	BlockDir               string
	ChunksTotal            int
	ChunksOverlapingTotal  int
	ChunksPartialOutsiders int
	ChunksEntireOutsiders  int
	MinTime, MaxTime       int64
}

// ErrSummary implements the error interface and returns a formated index stats.
func (i IndexStats) ErrSummary() error {
	if i.ChunksOverlapingTotal > 0 ||
		i.ChunksPartialOutsiders > 0 ||
		i.ChunksEntireOutsiders > 0 {
		return errors.Errorf(`Time Range: %v - %v, Total Chunks:%v, Total Overlaping Chunks:%v Chunks partially outside:%v, Chunks completely outside:%v`,
			i.MinTime, i.MaxTime,
			i.ChunksTotal,
			i.ChunksOverlapingTotal,
			i.ChunksPartialOutsiders,
			i.ChunksEntireOutsiders,
		)
	}
	return nil
}
