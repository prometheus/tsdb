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
package record

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"
)

var (
	// ErrOutOfOrderSample is returned if an appended sample has a
	// timestamp smaller than the most recent sample.
	ErrOutOfOrderSample = errors.New("out of order sample")

	// ErrNotFound is returned if a looked up resource was not found.
	ErrNotFound = errors.New("not found")

	// ErrAmendSample is returned if an appended sample has the same timestamp
	// as the most recent sample but a different value.
	ErrAmendSample = errors.New("amending sample")
)

type sample struct {
	t int64
	v float64
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) V() float64 {
	return s.v
}

// RefSeries is the series labels with the series ID.
type RefSeries struct {
	Ref    uint64
	Labels labels.Labels
}

// RefSample is a timestamp/value pair associated with a reference to a series.
type RefSample struct {
	Ref    uint64
	T      int64
	V      float64
	Series *MemSeries
}

// MemSeries is the in-memory representation of a series. None of its methods
// are goroutine safe and it is the caller's responsibility to lock it.
type MemSeries struct {
	sync.Mutex

	Ref           uint64
	PendingCommit bool // Whether there are samples waiting to be committed to this series.
	Chunks        []*MemChunk
	Lset          labels.Labels
	HeadChunk     *MemChunk

	chunkRange   int64
	firstChunkID int

	nextAt    int64 // Timestamp at which to cut the next chunk.
	sampleBuf [4]sample

	app chunkenc.Appender // Current appender for the chunk.
}

func NewMemSeries(lset labels.Labels, id uint64, chunkRange int64) *MemSeries {
	s := &MemSeries{
		Lset:       lset,
		Ref:        id,
		chunkRange: chunkRange,
		nextAt:     math.MinInt64,
	}
	return s
}

func (s *MemSeries) MinTime() int64 {
	if len(s.Chunks) == 0 {
		return math.MinInt64
	}
	return s.Chunks[0].MinTime
}

func (s *MemSeries) MaxTime() int64 {
	c := s.head()
	if c == nil {
		return math.MinInt64
	}
	return c.MaxTime
}

func (s *MemSeries) cut(mint int64) *MemChunk {
	c := &MemChunk{
		Chunk:   chunkenc.NewXORChunk(),
		MinTime: mint,
		MaxTime: math.MinInt64,
	}
	s.Chunks = append(s.Chunks, c)
	s.HeadChunk = c

	// Set upper bound on when the next chunk must be started. An earlier timestamp
	// may be chosen dynamically at a later point.
	s.nextAt = rangeForTimestamp(mint, s.chunkRange)

	app, err := c.Chunk.Appender()
	if err != nil {
		panic(err)
	}
	s.app = app
	return c
}

func (s *MemSeries) ChunksMetas() []chunks.Meta {
	metas := make([]chunks.Meta, 0, len(s.Chunks))
	for _, chk := range s.Chunks {
		metas = append(metas, chunks.Meta{Chunk: chk.Chunk, MinTime: chk.MinTime, MaxTime: chk.MaxTime})
	}
	return metas
}

// reset re-initialises all the variable in the MemSeries except 'lset', 'ref',
// and 'chunkRange', like how it would appear after 'newMemSeries(...)'.
func (s *MemSeries) Reset() {
	s.Chunks = nil
	s.HeadChunk = nil
	s.firstChunkID = 0
	s.nextAt = math.MinInt64
	s.sampleBuf = [4]sample{}
	s.PendingCommit = false
	s.app = nil
}

// Appendable checks whether the given sample is valid for appending to the series.
func (s *MemSeries) Appendable(t int64, v float64) error {
	c := s.head()
	if c == nil {
		return nil
	}

	if t > c.MaxTime {
		return nil
	}
	if t < c.MaxTime {
		return ErrOutOfOrderSample
	}
	// We are allowing exact duplicates as we can encounter them in valid cases
	// like federation and erroring out at that time would be extremely noisy.
	if math.Float64bits(s.sampleBuf[3].v) != math.Float64bits(v) {
		return ErrAmendSample
	}
	return nil
}

func (s *MemSeries) Chunk(id int) *MemChunk {
	ix := id - s.firstChunkID
	if ix < 0 || ix >= len(s.Chunks) {
		return nil
	}
	return s.Chunks[ix]
}

func (s *MemSeries) ChunkID(pos int) int {
	return pos + s.firstChunkID
}

// TruncateChunksBefore removes all chunks from the series that have not timestamp
// at or after mint. Chunk IDs remain unchanged.
func (s *MemSeries) TruncateChunksBefore(mint int64) (removed int) {
	var k int
	for i, c := range s.Chunks {
		if c.MaxTime >= mint {
			break
		}
		k = i + 1
	}
	s.Chunks = append(s.Chunks[:0], s.Chunks[k:]...)
	s.firstChunkID += k
	if len(s.Chunks) == 0 {
		s.HeadChunk = nil
	} else {
		s.HeadChunk = s.Chunks[len(s.Chunks)-1]
	}

	return k
}

// Append adds the sample (t, v) to the series.
func (s *MemSeries) Append(t int64, v float64) (success, chunkCreated bool) {
	// Based on Gorilla white papers this offers near-optimal compression ratio
	// so anything bigger that this has diminishing returns and increases
	// the time range within which we have to decompress all samples.
	const samplesPerChunk = 120

	c := s.head()

	if c == nil {
		c = s.cut(t)
		chunkCreated = true
	}
	numSamples := c.Chunk.NumSamples()

	// Out of order sample.
	if c.MaxTime >= t {
		return false, chunkCreated
	}
	// If we reach 25% of a chunk's desired sample count, set a definitive time
	// at which to start the next chunk.
	// At latest it must happen at the timestamp set when the chunk was cut.
	if numSamples == samplesPerChunk/4 {
		s.nextAt = computeChunkEndTime(c.MinTime, c.MaxTime, s.nextAt)
	}
	if t >= s.nextAt {
		c = s.cut(t)
		chunkCreated = true
	}
	s.app.Append(t, v)

	c.MaxTime = t

	s.sampleBuf[0] = s.sampleBuf[1]
	s.sampleBuf[1] = s.sampleBuf[2]
	s.sampleBuf[2] = s.sampleBuf[3]
	s.sampleBuf[3] = sample{t: t, v: v}

	return true, chunkCreated
}

func (s *MemSeries) Iterator(id int) chunkenc.Iterator {
	c := s.Chunk(id)
	// TODO(fabxc): Work around! A querier may have retrieved a pointer to a series' chunk,
	// which got then garbage collected before it got accessed.
	// We must ensure to not garbage collect as long as any readers still hold a reference.
	if c == nil {
		return chunkenc.NewNopIterator()
	}

	if id-s.firstChunkID < len(s.Chunks)-1 {
		return c.Chunk.Iterator()
	}
	// Serve the last 4 samples for the last chunk from the sample buffer
	// as their compressed bytes may be mutated by added samples.
	it := &MemSafeIterator{
		Iterator: c.Chunk.Iterator(),
		i:        -1,
		total:    c.Chunk.NumSamples(),
		buf:      s.sampleBuf,
	}
	return it
}

func (s *MemSeries) head() *MemChunk {
	return s.HeadChunk
}

type MemChunk struct {
	Chunk            chunkenc.Chunk
	MinTime, MaxTime int64
}

// Returns true if the chunk overlaps [mint, maxt].
func (mc *MemChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	return mc.MinTime <= maxt && mint <= mc.MaxTime
}

type MemSafeIterator struct {
	chunkenc.Iterator

	i     int
	total int
	buf   [4]sample
}

func (it *MemSafeIterator) Next() bool {
	if it.i+1 >= it.total {
		return false
	}
	it.i++
	if it.total-it.i > 4 {
		return it.Iterator.Next()
	}
	return true
}

func (it *MemSafeIterator) At() (int64, float64) {
	if it.total-it.i > 4 {
		return it.Iterator.At()
	}
	s := it.buf[4-(it.total-it.i)]
	return s.t, s.v
}

func rangeForTimestamp(t int64, width int64) (maxt int64) {
	return (t/width)*width + width
}

// computeChunkEndTime estimates the end timestamp based the beginning of a chunk,
// its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
func computeChunkEndTime(start, cur, max int64) int64 {
	a := (max - start) / ((cur - start + 1) * 4)
	if a == 0 {
		return max
	}
	return start + (max-start)/a
}

// RenameFile renames the file from, removing to if it already exists before doing the rename.
func RenameFile(from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = pdir.Sync(); err != nil {
		pdir.Close()
		return err
	}
	return pdir.Close()
}
