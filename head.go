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
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"encoding/binary"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

var (
	// ErrNotFound is returned if a looked up resource was not found.
	ErrNotFound = errors.Errorf("not found")

	// ErrOutOfOrderSample is returned if an appended sample has a
	// timestamp larger than the most recent sample.
	ErrOutOfOrderSample = errors.New("out of order sample")

	// ErrAmendSample is returned if an appended sample has the same timestamp
	// as the most recent sample but a different value.
	ErrAmendSample = errors.New("amending sample")

	// ErrOutOfBounds is returned if an appended sample is out of the
	// writable time range.
	ErrOutOfBounds = errors.New("out of bounds")
)

// HeadBlock handles reads and writes of time series data within a time window.
type HeadBlock struct {
	mtx sync.RWMutex
	dir string
	wal WAL

	activeWriters uint64
	highTimestamp int64
	closed        bool

	// descs holds all chunk descs for the head block. Each chunk implicitly
	// is assigned the index as its ID.
	series []*memSeries
	// hashes contains a collision map of label set hashes of chunks
	// to their chunk descs.
	hashes map[uint64][]*memSeries

	values   map[string]stringset // label names to possible values
	postings *memPostings         // postings lists for terms

	tombstones tombstoneReader

	meta BlockMeta
}

// TouchHeadBlock atomically touches a new head block in dir for
// samples in the range [mint,maxt).
func TouchHeadBlock(dir string, mint, maxt int64) (string, error) {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))

	ulid, err := ulid.New(ulid.Now(), entropy)
	if err != nil {
		return "", err
	}

	// Make head block creation appear atomic.
	dir = filepath.Join(dir, ulid.String())
	tmp := dir + ".tmp"

	if err := os.MkdirAll(tmp, 0777); err != nil {
		return "", err
	}

	if err := writeMetaFile(tmp, &BlockMeta{
		ULID:    ulid,
		MinTime: mint,
		MaxTime: maxt,
	}); err != nil {
		return "", err
	}

	return dir, renameFile(tmp, dir)
}

// OpenHeadBlock opens the head block in dir.
func OpenHeadBlock(dir string, l log.Logger, wal WAL) (*HeadBlock, error) {
	meta, err := readMetaFile(dir)
	if err != nil {
		return nil, err
	}

	h := &HeadBlock{
		dir:        dir,
		wal:        wal,
		series:     []*memSeries{nil}, // 0 is not a valid posting, filled with nil.
		hashes:     map[uint64][]*memSeries{},
		values:     map[string]stringset{},
		postings:   &memPostings{m: make(map[term][]uint32)},
		meta:       *meta,
		tombstones: newEmptyTombstoneReader(),
	}
	return h, h.init()
}

func (h *HeadBlock) init() error {
	r := h.wal.Reader()

	seriesFunc := func(series []labels.Labels) error {
		for _, lset := range series {
			h.create(lset.Hash(), lset)
			h.meta.Stats.NumSeries++
		}

		return nil
	}
	samplesFunc := func(samples []RefSample) error {
		for _, s := range samples {
			if int(s.Ref) >= len(h.series) {
				return errors.Errorf("unknown series reference %d (max %d); abort WAL restore",
					s.Ref, len(h.series))
			}
			h.series[s.Ref].append(s.T, s.V, 0)

			if !h.inBounds(s.T) {
				return errors.Wrap(ErrOutOfBounds, "consume WAL")
			}
			h.meta.Stats.NumSamples++
		}

		return nil
	}
	deletesFunc := func(stones []Stone) error {
		for _, s := range stones {
			for _, itv := range s.intervals {
				h.tombstones.add(s.ref, itv)
			}
		}

		return nil
	}

	if err := r.Read(seriesFunc, samplesFunc, deletesFunc); err != nil {
		return errors.Wrap(err, "consume WAL")
	}

	return nil
}

// inBounds returns true if the given timestamp is within the valid
// time bounds of the block.
func (h *HeadBlock) inBounds(t int64) bool {
	return t >= h.meta.MinTime && t <= h.meta.MaxTime
}

func (h *HeadBlock) String() string {
	return h.meta.ULID.String()
}

// Close syncs all data and closes underlying resources of the head block.
func (h *HeadBlock) Close() error {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	if err := h.wal.Close(); err != nil {
		return errors.Wrapf(err, "close WAL for head %s", h.dir)
	}
	// Check whether the head block still exists in the underlying dir
	// or has already been replaced with a compacted version or removed.
	meta, err := readMetaFile(h.dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if meta.ULID == h.meta.ULID {
		return writeMetaFile(h.dir, &h.meta)
	}

	h.closed = true
	return nil
}

// Meta returns a BlockMeta for the head block.
func (h *HeadBlock) Meta() BlockMeta {
	m := BlockMeta{
		ULID:       h.meta.ULID,
		MinTime:    h.meta.MinTime,
		MaxTime:    h.meta.MaxTime,
		Compaction: h.meta.Compaction,
	}

	m.Stats.NumChunks = atomic.LoadUint64(&h.meta.Stats.NumChunks)
	m.Stats.NumSeries = atomic.LoadUint64(&h.meta.Stats.NumSeries)
	m.Stats.NumSamples = atomic.LoadUint64(&h.meta.Stats.NumSamples)

	return m
}

// Tombstones returns the TombstoneReader against the block.
func (h *HeadBlock) Tombstones() TombstoneReader {
	return h.tombstones
}

// Delete implements headBlock.
func (h *HeadBlock) Delete(mint int64, maxt int64, ms ...labels.Matcher) error {
	ir := h.Index()

	pr := newPostingsReader(ir)
	p, absent := pr.Select(ms...)

	var stones []Stone

Outer:
	for p.Next() {
		ref := p.At()
		lset := h.series[ref].lset
		for _, abs := range absent {
			if lset.Get(abs) != "" {
				continue Outer
			}
		}

		// Delete only until the current values and not beyond.
		tmin, tmax := clampInterval(mint, maxt, h.series[ref].chunks[0].minTime, h.series[ref].head().maxTime)
		stones = append(stones, Stone{ref, intervals{{tmin, tmax}}})
	}

	if p.Err() != nil {
		return p.Err()
	}
	if err := h.wal.LogDeletes(stones); err != nil {
		return err
	}

	for _, s := range stones {
		h.tombstones.add(s.ref, s.intervals[0])
	}

	h.meta.Stats.NumTombstones = uint64(len(h.tombstones))
	return nil
}

// Snapshot persists the current state of the headblock to the given directory.
// TODO(gouthamve): Snapshot must be called when there are no active appenders.
// This has been ensured by acquiring a Lock on DB.mtx, but this limitation should
// be removed in the future.
func (h *HeadBlock) Snapshot(snapshotDir string) error {
	if h.meta.Stats.NumSeries == 0 {
		return nil
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	dir := filepath.Join(snapshotDir, uid.String())
	tmp := dir + ".tmp"

	if err := os.RemoveAll(tmp); err != nil {
		return err
	}

	if err := os.MkdirAll(tmp, 0777); err != nil {
		return err
	}

	// Populate chunk and index files into temporary directory with
	// data of all blocks.
	chunkw, err := newChunkWriter(chunkDir(tmp))
	if err != nil {
		return errors.Wrap(err, "open chunk writer")
	}
	indexw, err := newIndexWriter(tmp)
	if err != nil {
		return errors.Wrap(err, "open index writer")
	}

	meta, err := populateBlock([]Block{h}, indexw, chunkw)
	if err != nil {
		return errors.Wrap(err, "write snapshot")
	}
	meta.ULID = uid
	meta.MaxTime = h.highTimestamp

	if err = writeMetaFile(tmp, meta); err != nil {
		return errors.Wrap(err, "write merged meta")
	}

	if err = chunkw.Close(); err != nil {
		return errors.Wrap(err, "close chunk writer")
	}
	if err = indexw.Close(); err != nil {
		return errors.Wrap(err, "close index writer")
	}

	// Create an empty tombstones file.
	if err := writeTombstoneFile(tmp, newEmptyTombstoneReader()); err != nil {
		return errors.Wrap(err, "write new tombstones file")
	}

	// Block successfully written, make visible
	if err := renameFile(tmp, dir); err != nil {
		return errors.Wrap(err, "rename block dir")
	}

	return nil
}

// Dir returns the directory of the block.
func (h *HeadBlock) Dir() string { return h.dir }

// Index returns an IndexReader against the block.
func (h *HeadBlock) Index() IndexReader { return &headIndexReader{h} }

// Chunks returns a ChunkReader against the block.
func (h *HeadBlock) Chunks(isolation *IsolationState) ChunkReader {
	return &headChunkReader{HeadBlock: h, isolation: isolation}
}

// Querier returns a new Querier against the block for the range [mint, maxt].
func (h *HeadBlock) Querier(mint, maxt int64, isolation *IsolationState) Querier {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	if h.closed {
		panic(fmt.Sprintf("block %s already closed", h.dir))
	}

	// Reference on the original slice to use for postings mapping.
	series := h.series[:]

	return &blockQuerier{
		mint:       mint,
		maxt:       maxt,
		index:      h.Index(),
		chunks:     h.Chunks(isolation),
		tombstones: h.Tombstones(),

		postingsMapper: func(p Postings) Postings {
			ep := make([]uint32, 0, 64)

			for p.Next() {
				// Skip posting entries that include series added after we
				// instantiated the querier.
				if int(p.At()) >= len(series) {
					break
				}
				ep = append(ep, p.At())
			}
			if err := p.Err(); err != nil {
				return errPostings{err: errors.Wrap(err, "expand postings")}
			}

			sort.Slice(ep, func(i, j int) bool {
				return labels.Compare(series[ep[i]].lset, series[ep[j]].lset) < 0
			})
			return newListPostings(ep)
		},
	}
}

// Appender returns a new Appender against the head block.
func (h *HeadBlock) Appender(writeId, cleanupWriteIdsBelow uint64) Appender {
	atomic.AddUint64(&h.activeWriters, 1)

	h.mtx.RLock()

	if h.closed {
		panic(fmt.Sprintf("block %s already closed", h.dir))
	}
	return &headAppender{HeadBlock: h, samples: getHeadAppendBuffer(), writeId: writeId, cleanupWriteIdsBelow: cleanupWriteIdsBelow}
}

// ActiveWriters returns true if the block has open write transactions.
func (h *HeadBlock) ActiveWriters() int {
	return int(atomic.LoadUint64(&h.activeWriters))
}

// HighTimestamp returns the highest inserted sample timestamp.
func (h *HeadBlock) HighTimestamp() int64 {
	return atomic.LoadInt64(&h.highTimestamp)
}

var headPool = sync.Pool{}

func getHeadAppendBuffer() []RefSample {
	b := headPool.Get()
	if b == nil {
		return make([]RefSample, 0, 512)
	}
	return b.([]RefSample)
}

func putHeadAppendBuffer(b []RefSample) {
	headPool.Put(b[:0])
}

type headAppender struct {
	*HeadBlock

	newSeries            []*hashedLabels
	newLabels            []labels.Labels
	newHashes            map[uint64]uint64
	writeId              uint64
	cleanupWriteIdsBelow uint64

	samples       []RefSample
	highTimestamp int64
}

type hashedLabels struct {
	ref    uint64
	hash   uint64
	labels labels.Labels
}

func (a *headAppender) Add(lset labels.Labels, t int64, v float64) (string, error) {
	if !a.inBounds(t) {
		return "", ErrOutOfBounds
	}

	hash := lset.Hash()
	refb := make([]byte, 8)

	// Series exists already in the block.
	if ms := a.get(hash, lset); ms != nil {
		binary.BigEndian.PutUint64(refb, uint64(ms.ref))
		return string(refb), a.AddFast(string(refb), t, v)
	}
	// Series was added in this transaction previously.
	if ref, ok := a.newHashes[hash]; ok {
		binary.BigEndian.PutUint64(refb, ref)
		// XXX(fabxc): there's no fast path for multiple samples for the same new series
		// in the same transaction. We always return the invalid empty ref. It's has not
		// been a relevant use case so far and is not worth the trouble.
		return "", a.AddFast(string(refb), t, v)
	}

	// The series is completely new.
	if a.newSeries == nil {
		a.newHashes = map[uint64]uint64{}
	}
	// First sample for new series.
	ref := uint64(len(a.newSeries))

	a.newSeries = append(a.newSeries, &hashedLabels{
		ref:    ref,
		hash:   hash,
		labels: lset,
	})
	// First bit indicates its a series created in this transaction.
	ref |= (1 << 63)

	a.newHashes[hash] = ref
	binary.BigEndian.PutUint64(refb, ref)

	return "", a.AddFast(string(refb), t, v)
}

func (a *headAppender) AddFast(ref string, t int64, v float64) error {
	if len(ref) != 8 {
		return errors.Wrap(ErrNotFound, "invalid ref length")
	}
	var (
		refn = binary.BigEndian.Uint64(yoloBytes(ref))
		id   = (refn << 1) >> 1
		inTx = refn&(1<<63) != 0
	)
	// Distinguish between existing series and series created in
	// this transaction.
	if inTx {
		if id > uint64(len(a.newSeries)-1) {
			return errors.Wrap(ErrNotFound, "transaction series ID too high")
		}
		// TODO(fabxc): we also have to validate here that the
		// sample sequence is valid.
		// We also have to revalidate it as we switch locks and create
		// the new series.
	} else if id > uint64(len(a.series)) {
		return errors.Wrap(ErrNotFound, "transaction series ID too high")
	} else {
		ms := a.series[id]
		if ms == nil {
			return errors.Wrap(ErrNotFound, "nil series")
		}
		// TODO(fabxc): memory series should be locked here already.
		// Only problem is release of locks in case of a rollback.
		c := ms.head()

		if !a.inBounds(t) {
			return ErrOutOfBounds
		}
		if t < c.maxTime {
			return ErrOutOfOrderSample
		}

		// We are allowing exact duplicates as we can encounter them in valid cases
		// like federation and erroring out at that time would be extremely noisy.
		if c.maxTime == t && math.Float64bits(ms.lastValue) != math.Float64bits(v) {
			return ErrAmendSample
		}
	}

	if t > a.highTimestamp {
		a.highTimestamp = t
	}

	a.samples = append(a.samples, RefSample{
		Ref: refn,
		T:   t,
		V:   v,
	})
	return nil
}

func (a *headAppender) createSeries() error {
	if len(a.newSeries) == 0 {
		return nil
	}
	a.newLabels = make([]labels.Labels, 0, len(a.newSeries))
	base0 := len(a.series)

	a.mtx.RUnlock()
	defer a.mtx.RLock()
	a.mtx.Lock()
	defer a.mtx.Unlock()

	base1 := len(a.series)

	for _, l := range a.newSeries {
		// We switched locks and have to re-validate that the series were not
		// created by another goroutine in the meantime.
		if base1 > base0 {
			if ms := a.get(l.hash, l.labels); ms != nil {
				l.ref = uint64(ms.ref)
				continue
			}
		}
		// Series is still new.
		a.newLabels = append(a.newLabels, l.labels)
		l.ref = uint64(len(a.series))

		a.create(l.hash, l.labels)
	}

	// Write all new series to the WAL.
	if err := a.wal.LogSeries(a.newLabels); err != nil {
		return errors.Wrap(err, "WAL log series")
	}

	return nil
}

func (a *headAppender) Commit() error {
	defer atomic.AddUint64(&a.activeWriters, ^uint64(0))
	defer putHeadAppendBuffer(a.samples)
	defer a.mtx.RUnlock()

	if err := a.createSeries(); err != nil {
		return err
	}

	// We have to update the refs of samples for series we just created.
	for i := range a.samples {
		s := &a.samples[i]
		if s.Ref&(1<<63) != 0 {
			s.Ref = a.newSeries[(s.Ref<<1)>>1].ref
		}
	}

	// Write all new samples to the WAL and add them to the
	// in-mem database on success.
	if err := a.wal.LogSamples(a.samples); err != nil {
		return errors.Wrap(err, "WAL log samples")
	}

	total := uint64(len(a.samples))

	for _, s := range a.samples {
		if !a.series[s.Ref].append(s.T, s.V, a.writeId) {
			total--
		}
		// Also take the opportuinty to GC writeIds in the memSeries
		// that are no longer required.
		a.series[s.Ref].cleanupWriteIdsBelow(a.cleanupWriteIdsBelow)
	}

	atomic.AddUint64(&a.meta.Stats.NumSamples, total)
	atomic.AddUint64(&a.meta.Stats.NumSeries, uint64(len(a.newSeries)))

	for {
		ht := a.HeadBlock.HighTimestamp()
		if a.highTimestamp <= ht {
			break
		}
		if atomic.CompareAndSwapInt64(&a.HeadBlock.highTimestamp, ht, a.highTimestamp) {
			break
		}
	}

	return nil
}

func (a *headAppender) Rollback() error {
	a.mtx.RUnlock()
	atomic.AddUint64(&a.activeWriters, ^uint64(0))
	putHeadAppendBuffer(a.samples)
	return nil
}

type headChunkReader struct {
	*HeadBlock
	isolation *IsolationState
}

// Chunk returns the chunk for the reference number.
func (h *headChunkReader) Chunk(ref uint64) (chunks.Chunk, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	si := ref >> 32
	ci := (ref << 32) >> 32

	c := &safeChunk{
		Chunk:     h.series[si].chunks[ci].chunk,
		s:         h.series[si],
		i:         int(ci),
		isolation: h.isolation,
	}
	return c, nil
}

type safeChunk struct {
	chunks.Chunk
	s         *memSeries
	i         int
	isolation *IsolationState
}

func (c *safeChunk) Iterator() chunks.Iterator {
	c.s.mtx.RLock()
	defer c.s.mtx.RUnlock()
	return c.s.iterator(c.i, c.isolation)
}

// func (c *safeChunk) Appender() (chunks.Appender, error) { panic("illegal") }
// func (c *safeChunk) Bytes() []byte                      { panic("illegal") }
// func (c *safeChunk) Encoding() chunks.Encoding          { panic("illegal") }

type headIndexReader struct {
	*HeadBlock
}

// LabelValues returns the possible label values
func (h *headIndexReader) LabelValues(names ...string) (StringTuples, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	if len(names) != 1 {
		return nil, errInvalidSize
	}
	var sl []string

	for s := range h.values[names[0]] {
		sl = append(sl, s)
	}
	sort.Strings(sl)

	return &stringTuples{l: len(names), s: sl}, nil
}

// Postings returns the postings list iterator for the label pair.
func (h *headIndexReader) Postings(name, value string) (Postings, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	return h.postings.get(term{name: name, value: value}), nil
}

// Series returns the series for the given reference.
func (h *headIndexReader) Series(ref uint32) (labels.Labels, []*ChunkMeta, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	if int(ref) >= len(h.series) {
		return nil, nil, ErrNotFound
	}

	s := h.series[ref]
	if s == nil {
		return nil, nil, ErrNotFound
	}
	metas := make([]*ChunkMeta, 0, len(s.chunks))

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	for i, c := range s.chunks {
		metas = append(metas, &ChunkMeta{
			MinTime: c.minTime,
			MaxTime: c.maxTime,
			Ref:     (uint64(ref) << 32) | uint64(i),
		})
	}

	return s.lset, metas, nil
}

func (h *headIndexReader) LabelIndices() ([][]string, error) {
	h.mtx.RLock()
	defer h.mtx.RUnlock()

	res := [][]string{}

	for s := range h.values {
		res = append(res, []string{s})
	}
	return res, nil
}

// get retrieves the chunk with the hash and label set and creates
// a new one if it doesn't exist yet.
func (h *HeadBlock) get(hash uint64, lset labels.Labels) *memSeries {
	series := h.hashes[hash]

	for _, s := range series {
		if s.lset.Equals(lset) {
			return s
		}
	}
	return nil
}

func (h *HeadBlock) create(hash uint64, lset labels.Labels) *memSeries {
	s := newMemSeries(lset, uint32(len(h.series)), h.meta.MaxTime)

	// Allocate empty space until we can insert at the given index.
	h.series = append(h.series, s)

	h.hashes[hash] = append(h.hashes[hash], s)

	for _, l := range lset {
		valset, ok := h.values[l.Name]
		if !ok {
			valset = stringset{}
			h.values[l.Name] = valset
		}
		valset.set(l.Value)

		h.postings.add(s.ref, term{name: l.Name, value: l.Value})
	}

	h.postings.add(s.ref, term{})

	return s
}

type sample struct {
	t int64
	v float64
}

type memSeries struct {
	mtx sync.RWMutex

	ref    uint32
	lset   labels.Labels
	chunks []*memChunk

	nextAt    int64 // timestamp at which to cut the next chunk.
	maxt      int64 // maximum timestamp for the series.
	lastValue float64
	sampleBuf [4]sample

	// Write ids of most recent samples. This is a ring buffer.
	writeIds     []uint64
	writeIdFirst int // Position of first id in the ring.
	writeIdCount int // How many ids in the ring.

	app chunks.Appender // Current appender for the chunk.
}

func (s *memSeries) cut(mint int64) *memChunk {
	c := &memChunk{
		chunk:   chunks.NewXORChunk(),
		minTime: mint,
		maxTime: math.MinInt64,
	}
	s.chunks = append(s.chunks, c)

	app, err := c.chunk.Appender()
	if err != nil {
		panic(err)
	}
	s.app = app
	return c
}

func newMemSeries(lset labels.Labels, id uint32, maxt int64) *memSeries {
	s := &memSeries{
		lset:     lset,
		ref:      id,
		maxt:     maxt,
		nextAt:   math.MinInt64,
		writeIds: make([]uint64, 4),
	}
	return s
}

func (s *memSeries) append(t int64, v float64, writeId uint64) bool {
	const samplesPerChunk = 120

	s.mtx.Lock()
	defer s.mtx.Unlock()

	var c *memChunk

	if len(s.chunks) == 0 {
		c = s.cut(t)
	}
	c = s.head()
	if c.maxTime >= t {
		return false
	}
	if c.samples > samplesPerChunk/4 && t >= s.nextAt {
		c = s.cut(t)
	}
	s.app.Append(t, v)

	c.maxTime = t
	c.samples++

	if c.samples == samplesPerChunk/4 {
		s.nextAt = computeChunkEndTime(c.minTime, c.maxTime, s.maxt)
	}

	s.lastValue = v

	s.sampleBuf[0] = s.sampleBuf[1]
	s.sampleBuf[1] = s.sampleBuf[2]
	s.sampleBuf[2] = s.sampleBuf[3]
	s.sampleBuf[3] = sample{t: t, v: v}

	if s.writeIdCount == len(s.writeIds) {
		// Ring buffer is full, expand by doubling.
		newRing := make([]uint64, s.writeIdCount*2)
		copy(newRing[s.writeIdCount+s.writeIdFirst:], s.writeIds[s.writeIdFirst:])
		copy(newRing, s.writeIds[:s.writeIdFirst])
		s.writeIds = newRing
		s.writeIdFirst += s.writeIdCount
	}
	s.writeIds[(s.writeIdFirst+s.writeIdCount)%len(s.writeIds)] = writeId
	s.writeIdCount++

	return true
}

func (s *memSeries) cleanupWriteIdsBelow(bound uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	pos := s.writeIdFirst

	for s.writeIdCount > 0 {
		if s.writeIds[pos] < bound {
			s.writeIdFirst++
			s.writeIdCount--
		} else {
			break
		}
		pos++
		if pos == len(s.writeIds) {
			pos = 0
		}
	}
	if s.writeIdFirst >= len(s.writeIds) {
		s.writeIdFirst -= len(s.writeIds)
	}
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

func (s *memSeries) iterator(i int, isolation *IsolationState) chunks.Iterator {
	c := s.chunks[i]

	stopAfter := c.samples
	if isolation != nil {
		totalSamples := 0
		previousSamples := 0 // Samples before this chunk.
		for j, d := range s.chunks {
			totalSamples += d.samples
			if j < i {
				previousSamples += d.samples
			}
		}
		writeIdsToConsider := (previousSamples + c.samples) - (totalSamples - s.writeIdCount)
		// Iterate over the ring, find the first one that the isolation state says not
		// to return.
		pos := s.writeIdFirst
		for index := 0; index < writeIdsToConsider; index++ {
			writeId := s.writeIds[pos]
			if _, ok := isolation.incompleteWrites[writeId]; ok || writeId > isolation.maxWriteId {
				stopAfter = index - (writeIdsToConsider - c.samples)
				if stopAfter < 0 {
					stopAfter = 0 // Stopped in a previous chunk.
				}
				break
			}
			pos++
			if pos == len(s.writeIds) {
				pos = 0
			}
		}
	}

	if i < len(s.chunks)-1 {
		it := &memSafeIterator{
			Iterator:  c.chunk.Iterator(),
			i:         -1,
			total:     c.samples,
			stopAfter: stopAfter,
		}
		return it
	}

	it := &memSafeIterator{
		Iterator:        c.chunk.Iterator(),
		i:               -1,
		total:           c.samples,
		stopAfter:       stopAfter,
		bufferedSamples: 4,
		buf:             s.sampleBuf,
	}
	return it
}

func (s *memSeries) head() *memChunk {
	return s.chunks[len(s.chunks)-1]
}

type memChunk struct {
	chunk            chunks.Chunk
	minTime, maxTime int64
	samples          int
}

type memSafeIterator struct {
	chunks.Iterator

	i               int
	total           int
	stopAfter       int
	bufferedSamples int
	buf             [4]sample
}

func (it *memSafeIterator) Next() bool {
	if it.i+1 >= it.stopAfter {
		return false
	}
	it.i++
	if it.total-it.i > it.bufferedSamples {
		return it.Iterator.Next()
	}
	return true
}

func (it *memSafeIterator) At() (int64, float64) {
	if it.total-it.i > it.bufferedSamples {
		return it.Iterator.At()
	}
	s := it.buf[it.bufferedSamples-(it.total-it.i)]
	return s.t, s.v
}
