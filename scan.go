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
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// Scanner provides an interface for scanning different building components of a block.
type Scanner interface {
	// Tombstones returns all invalid Tombstones grouped by the returned error while opening it.
	Tombstones() (map[string][]string, error)
	// Meta returns all block paths with invalid meta file grouped by the returned error while opening.
	Meta() (map[string][]string, error)
	// Indexes attempts to repair all blocks with invalid indexes.
	// Returns a map of unrepairable and repaired indexes.
	// The list of unrepairable indexes is grouped by the returned error while opening the index.
	// During the rewrite it drops all chunks that are completely outside the block range,
	// drops all but one overlapping chunks,
	// chunks with times partially outside the block range are rewritten by removing the outsider samples.
	Indexes() (map[string][]string, []IndexStats, error)
	// Overlapping returns all blocks with overlapping time ranges.
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
			inv[err.Error()] = append(inv[err.Error()], filepath.Join(dir, tombstoneFilename))
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

func (s *DBScanner) Indexes() (map[string][]string, []IndexStats, error) {
	unrepairable := make(map[string][]string)
	var repaired []IndexStats
	dirs, err := blockDirs(s.db.dir)
	if err != nil {
		return nil, nil, err
	}

	for _, bdir := range dirs {
		stats, err := indexStats(bdir)
		if err != nil {
			unrepairable[err.Error()] = append(unrepairable[err.Error()], bdir)
			continue
		}

		if stats.ErrSummary() != nil {
			level.Debug(s.logger).Log("msg", "repairing block with index corruption", "block", bdir, "stats", stats.ErrSummary())
			if err := repairBlock(s.db.Dir(), bdir); err != nil {
				unrepairable[err.Error()] = append(unrepairable[err.Error()], bdir)
				continue
			}
			repaired = append(repaired, stats)

			if err := os.RemoveAll(bdir); err != nil {
				level.Error(s.logger).Log("msg", "delete old block", "block", bdir, "err", err)
			}
		}
	}
	return unrepairable, repaired, nil
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
	return OverlappingBlocks(blocks), nil
}

// repairBlock attempts to repair a block.
// It deletes the original block when the repair was successful or returns an error when it failed to repair the block.
func repairBlock(dbDir string, bdir string) (err error) {
	block, err := OpenBlock(bdir, nil)
	if err != nil {
		return errors.Wrap(err, "open block")
	}
	defer block.Close()

	indexr, err := block.Index()
	if err != nil {
		return
	}
	defer indexr.Close()
	chunkr, err := block.Chunks()
	if err != nil {
		return
	}
	defer chunkr.Close()

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	newBlockID := ulid.MustNew(ulid.Now(), entropy)
	newBlockDir := filepath.Join(dbDir, newBlockID.String())
	newBlockMeta := block.Meta()
	newBlockMeta.ULID = newBlockID
	newBlockMeta.Stats = BlockStats{} // Reset stats.

	// Delete the new block dir when the repair failed somewhere.
	defer func() {
		if err != nil {
			if errD := os.RemoveAll(newBlockDir); errD != nil {
				errors.Wrap(err, errors.Wrap(errD, "deleting new block after an error").Error())
			}
		}
	}()

	chunkw, err := chunks.NewWriter(chunkDir(newBlockDir))
	if err != nil {
		err = errors.Wrap(err, "opening new chunk writer")
		return
	}

	newIndexPath := filepath.Join(newBlockDir, indexFilename)
	indexw, err := index.NewWriter(newIndexPath)
	if err != nil {
		err = errors.Wrap(err, "opening new index writer")
		return
	}

	symbols, err := indexr.Symbols()
	if err != nil {
		err = errors.Wrap(err, "writing new chunks")
		return
	}
	if err = indexw.AddSymbols(symbols); err != nil {
		err = errors.Wrap(err, "add symbols to new index")
		return
	}

	all, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		err = errors.Wrap(err, "getting all postings from the corrupted index")
		return
	}
	all = indexr.SortedPostings(all)

	// We fully rebuild the postings list index from merged series.
	var (
		postings = index.NewMemPostings()
		values   = map[string]stringset{}
		i        = uint64(0)
	)

	var lset labels.Labels
	var chks []chunks.Meta

	for all.Next() {
		id := all.At()

		if err = indexr.Series(id, &lset, &chks); err != nil {
			err = errors.Wrap(err, "reading index series from the corrupted index")
			return
		}
		for i, c := range chks {
			chks[i].Chunk, err = chunkr.Chunk(c.Ref)
			if err != nil {
				err = errors.Wrap(err, "reading chunk from the source block")
				return
			}
		}
		chks, err = removeCompleteOutsiders(chks, newBlockMeta.MinTime, newBlockMeta.MaxTime)
		if err != nil {
			return
		}
		chks, err = repairPartialOutsiders(chks, newBlockMeta.MinTime, newBlockMeta.MaxTime)
		if err != nil {
			return
		}

		if len(chks) == 0 {
			err = fmt.Errorf("new block includes no chunks")
			return
		}

		if err = chunkw.WriteChunks(chks...); err != nil {
			err = errors.Wrap(err, "write chunks for the new block")
			return
		}
		if err = indexw.AddSeries(i, lset, chks...); err != nil {
			err = errors.Wrap(err, "add series to the new index")
			return
		}

		newBlockMeta.Stats.NumChunks += uint64(len(chks))
		newBlockMeta.Stats.NumSeries++

		for _, chk := range chks {
			newBlockMeta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
		}

		for _, l := range lset {
			valset, ok := values[l.Name]
			if !ok {
				valset = stringset{}
				values[l.Name] = valset
			}
			valset.set(l.Value)
		}
		postings.Add(i, lset)
		i++
	}
	if err = all.Err(); err != nil {
		err = errors.Wrap(err, "iterate series from the corrupted index")
		return
	}

	s := make([]string, 0, 256)
	for n, v := range values {
		s = s[:0]

		for x := range v {
			s = append(s, x)
		}
		if err = indexw.WriteLabelIndex([]string{n}, s); err != nil {
			err = errors.Wrap(err, "write label to the new index")
			return
		}
	}

	for _, l := range postings.SortedKeys() {
		if err = indexw.WritePostings(l.Name, l.Value, postings.Get(l.Name, l.Value)); err != nil {
			err = errors.Wrap(err, "write postings to the index")
			return
		}
	}

	// Flush files to disk.
	indexw.Close()
	chunkw.Close()
	if err = writeMetaFile(newBlockDir, &newBlockMeta); err != nil {
		err = errors.Wrap(err, "write the new block meta file")
		return
	}

	// The repair is successful only when the new block is clean.
	stats, err := indexStats(newBlockDir)
	if err != nil {
		err = errors.Wrap(err, "getting  new index stats")
		return
	}
	if stats.ErrSummary() != nil {
		err = fmt.Errorf("new block index still includes invalid chunks: %+v", stats.ErrSummary())
		return
	}

	return
}

func removeCompleteOutsiders(chks []chunks.Meta, mint int64, maxt int64) ([]chunks.Meta, error) {
	if len(chks) == 0 {
		return nil, fmt.Errorf("can't work with 0 chunks")
	}
	// First, ensure that chunks are ordered by their start time.
	sort.Slice(chks, func(i, j int) bool {
		return chks[i].MinTime < chks[j].MinTime
	})

	goodChunks := make([]chunks.Meta, 0, len(chks))
	for _, c := range chks {
		// min or max chunk time is within the time range. We don't remove partial outsiders.
		if c.OverlapsClosedInterval(mint, maxt) {
			// Skip if the chunk is overlapping with the previous one.
			if len(goodChunks) > 0 {
				last := goodChunks[len(goodChunks)-1]
				if c.OverlapsClosedInterval(last.MinTime, last.MaxTime) {
					continue
				}
			}
			goodChunks = append(goodChunks, c)
		}
	}
	return goodChunks, nil
}

func repairPartialOutsiders(chks []chunks.Meta, mint int64, maxt int64) ([]chunks.Meta, error) {
	if len(chks) == 0 {
		return nil, fmt.Errorf("can't work with 0 chunks")
	}
	// First, ensure that chunks are ordered by their start time.
	sort.Slice(chks, func(i, j int) bool {
		return chks[i].MinTime < chks[j].MinTime
	})

	for i, c := range chks {
		repairedChunk := chunks.Meta{
			Chunk:   chunkenc.NewXORChunk(),
			MinTime: 0,
			Ref:     c.Ref,
		}
		if c.OverlapsClosedInterval(mint, maxt) && (c.MinTime < mint || c.MaxTime > maxt) {
			repairedChunk.Ref = c.Ref
			app, err := repairedChunk.Chunk.Appender()
			if err != nil {
				return nil, err
			}

			it := c.Chunk.Iterator()
			for it.Next() {
				if t, v := it.At(); t >= mint && t <= maxt {
					app.Append(t, v)
					if t > repairedChunk.MinTime {
						repairedChunk.MinTime = t
					}
					repairedChunk.MaxTime = t
				}
			}
			if it.Err() != nil {
				return nil, it.Err()
			}
			chks[i] = repairedChunk
		}
	}

	return chks, nil
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
