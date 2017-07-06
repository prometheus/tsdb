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

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/labels"
)

// Compactor provides compaction against an underlying storage
// of time series data.
type Compactor interface {
	// Plan returns a set of non-overlapping directories that can
	// be compacted concurrently.
	// Results returned when compactions are in progress are undefined.
	Plan() ([][]string, error)

	// Write persists a Block into a directory.
	Write(b Block) error

	// Compact runs compaction against the provided directories. Must
	// only be called concurrently with results of Plan().
	Compact(dirs ...string) error
}

// compactor implements the Compactor interface.
type compactor struct {
	dir     string
	metrics *compactorMetrics
	logger  log.Logger
	opts    *compactorOptions
}

type compactorMetrics struct {
	ran      prometheus.Counter
	failed   prometheus.Counter
	duration prometheus.Histogram
}

func newCompactorMetrics(r prometheus.Registerer) *compactorMetrics {
	m := &compactorMetrics{}

	m.ran = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	m.failed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "tsdb_compactions_failed_total",
		Help: "Total number of compactions that failed for the partition.",
	})
	m.duration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name: "tsdb_compaction_duration",
		Help: "Duration of compaction runs.",
	})

	if r != nil {
		r.MustRegister(
			m.ran,
			m.failed,
			m.duration,
		)
	}
	return m
}

type compactorOptions struct {
	blockRanges []int64
}

func newCompactor(dir string, r prometheus.Registerer, l log.Logger, opts *compactorOptions) *compactor {
	if opts.blockRanges == nil {
		opts.blockRanges = []int64{
			int64(2 * time.Hour),
			int64(6 * time.Hour),
			int64(24 * time.Hour),
			int64(72 * time.Hour),  // 3d
			int64(216 * time.Hour), // 9d
		}
	}
	return &compactor{
		dir:     dir,
		opts:    opts,
		logger:  l,
		metrics: newCompactorMetrics(r),
	}
}

type compactionInfo struct {
	seq        int
	generation int
	mint, maxt int64
}

const compactionBlocksLen = 3

type dirMeta struct {
	dir  string
	meta *BlockMeta
}

func (c *compactor) Plan() ([][]string, error) {
	dirs, err := blockDirs(c.dir)
	if err != nil {
		return nil, err
	}

	var dms []dirMeta

	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		if err != nil {
			return nil, err
		}
		if meta.Compaction.Generation > 0 {
			dms = append(dms, dirMeta{dir, meta})
		}
	}
	sort.Slice(dms, func(i, j int) bool {
		return dms[i].meta.MinTime < dms[j].meta.MinTime
	})

	if len(dms) <= 1 {
		return nil, nil
	}

	sliceDirs := func(dms []dirMeta) [][]string {
		if len(dms) == 0 {
			return nil
		}
		var res []string
		for _, dm := range dms {
			res = append(res, dm.dir)
		}
		return [][]string{res}
	}

	return sliceDirs(c.selectDirs(dms)), nil
}

func (c *compactor) selectDirs(ds []dirMeta) []dirMeta {
	return selectRecurse(ds, c.opts.blockRanges)
}

func selectRecurse(dms []dirMeta, intervals []int64) []dirMeta {
	if len(intervals) == 0 {
		return dms
	}

	// Get the blocks by the max interval
	blocks := splitByRange(dms, intervals[len(intervals)-1])
	dirs := []dirMeta{}
	for i := len(blocks) - 1; i >= 0; i-- {
		// Choose the blocks to compact, largest (at the end) first.
		// And if there is more than 1 block, compact it.
		if len(blocks[i]) > 1 {
			dirs = blocks[i]
			break
		}
	}

	// If there are too many blocks, see if a smaller interval will catch them.
	if len(dirs) > 2 {
		smallerDirs := selectRecurse(dirs, intervals[:len(intervals)-1])
		if len(smallerDirs) > 1 {
			return smallerDirs
		}
	}

	return dirs
}

func splitByRange(ds []dirMeta, tr int64) [][]dirMeta {
	// TODO(gouthamve): Optimise
	rMap := make(map[int64][]dirMeta)
	for _, dir := range ds {
		t0 := dir.meta.MinTime - dir.meta.MinTime%tr
		if intervalContains(t0, t0+tr, dir.meta.MinTime) && intervalContains(t0, t0+tr, dir.meta.MaxTime) {
			rMap[t0] = append(rMap[t0], dir)
		}
	}
	res := make([][]dirMeta, 0, len(rMap))
	for _, v := range rMap {
		res = append(res, v)
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i][0].meta.MinTime < res[j][0].meta.MinTime
	})

	return res
}

func compactBlockMetas(blocks ...BlockMeta) (res BlockMeta) {
	res.MinTime = blocks[0].MinTime
	res.MaxTime = blocks[len(blocks)-1].MaxTime

	sources := map[ulid.ULID]struct{}{}

	for _, b := range blocks {
		res.Stats.NumSamples += b.Stats.NumSamples

		if b.Compaction.Generation > res.Compaction.Generation {
			res.Compaction.Generation = b.Compaction.Generation
		}
		for _, s := range b.Compaction.Sources {
			sources[s] = struct{}{}
		}
		// If it's an in memory block, its ULID goes into the sources.
		if b.Compaction.Generation == 0 {
			sources[b.ULID] = struct{}{}
		}
	}
	res.Compaction.Generation++

	for s := range sources {
		res.Compaction.Sources = append(res.Compaction.Sources, s)
	}
	sort.Slice(res.Compaction.Sources, func(i, j int) bool {
		return res.Compaction.Sources[i].Compare(res.Compaction.Sources[j]) < 0
	})

	return res
}

func (c *compactor) Compact(dirs ...string) (err error) {
	var blocks []Block

	for _, d := range dirs {
		b, err := newPersistedBlock(d)
		if err != nil {
			return err
		}
		defer b.Close()

		blocks = append(blocks, b)
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	return c.write(uid, blocks...)
}

func (c *compactor) Write(b Block) error {
	// Buffering blocks might have been created that often have no data.
	if b.Meta().Stats.NumSeries == 0 {
		return errors.Wrap(os.RemoveAll(b.Dir()), "remove empty block")
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid := ulid.MustNew(ulid.Now(), entropy)

	return c.write(uid, b)
}

// write creates a new block that is the union of the provided blocks into dir.
// It cleans up all files of the old blocks after completing successfully.
func (c *compactor) write(uid ulid.ULID, blocks ...Block) (err error) {
	c.logger.Log("msg", "compact blocks", "blocks", fmt.Sprintf("%v", blocks))

	defer func(t time.Time) {
		if err != nil {
			c.metrics.failed.Inc()
		}
		c.metrics.ran.Inc()
		c.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	dir := filepath.Join(c.dir, uid.String())
	tmp := dir + ".tmp"

	if err = os.RemoveAll(tmp); err != nil {
		return err
	}

	if err = os.MkdirAll(tmp, 0777); err != nil {
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

	meta, err := populateBlock(blocks, indexw, chunkw)
	if err != nil {
		return errors.Wrap(err, "write compaction")
	}
	meta.ULID = uid

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

	// Block successfully written, make visible and remove old ones.
	if err := renameFile(tmp, dir); err != nil {
		return errors.Wrap(err, "rename block dir")
	}
	for _, b := range blocks {
		if err := os.RemoveAll(b.Dir()); err != nil {
			return err
		}
	}
	// Properly sync parent dir to ensure changes are visible.
	df, err := fileutil.OpenDir(dir)
	if err != nil {
		return errors.Wrap(err, "sync block dir")
	}
	defer df.Close()

	if err := fileutil.Fsync(df); err != nil {
		return errors.Wrap(err, "sync block dir")
	}

	return nil
}

// populateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
func populateBlock(blocks []Block, indexw IndexWriter, chunkw ChunkWriter) (*BlockMeta, error) {
	var set compactionSet
	var metas []BlockMeta

	for i, b := range blocks {
		metas = append(metas, b.Meta())

		all, err := b.Index().Postings("", "")
		if err != nil {
			return nil, err
		}
		s := newCompactionSeriesSet(b.Index(), b.Chunks(), b.Tombstones(), all)

		if i == 0 {
			set = s
			continue
		}
		set, err = newCompactionMerger(set, s)
		if err != nil {
			return nil, err
		}
	}

	// We fully rebuild the postings list index from merged series.
	var (
		postings = &memPostings{m: make(map[term][]uint32, 512)}
		values   = map[string]stringset{}
		i        = uint32(0)
		meta     = compactBlockMetas(metas...)
	)

	for set.Next() {
		lset, chks, dranges := set.At() // The chunks here are not fully deleted.

		if len(dranges) > 0 {
			// Re-encode the chunk to not have deleted values.
			for _, chk := range chks {
				if intervalOverlap(dranges[0].mint, dranges[len(dranges)-1].maxt, chk.MinTime, chk.MaxTime) {
					newChunk := chunks.NewXORChunk()
					app, err := newChunk.Appender()
					if err != nil {
						return nil, err
					}

					it := &deletedIterator{it: chk.Chunk.Iterator(), intervals: dranges}
					for it.Next() {
						ts, v := it.At()
						app.Append(ts, v)
					}

					chk.Chunk = newChunk
				}
			}
		}

		if err := chunkw.WriteChunks(chks...); err != nil {
			return nil, err
		}

		indexw.AddSeries(i, lset, chks...)

		meta.Stats.NumChunks += uint64(len(chks))
		meta.Stats.NumSeries++

		for _, l := range lset {
			valset, ok := values[l.Name]
			if !ok {
				valset = stringset{}
				values[l.Name] = valset
			}
			valset.set(l.Value)

			postings.add(i, term{name: l.Name, value: l.Value})
		}
		i++
	}
	if set.Err() != nil {
		return nil, set.Err()
	}

	s := make([]string, 0, 256)
	for n, v := range values {
		s = s[:0]

		for x := range v {
			s = append(s, x)
		}
		if err := indexw.WriteLabelIndex([]string{n}, s); err != nil {
			return nil, err
		}
	}

	for t := range postings.m {
		if err := indexw.WritePostings(t.name, t.value, postings.get(t)); err != nil {
			return nil, err
		}
	}
	// Write a postings list containing all series.
	all := make([]uint32, i)
	for i := range all {
		all[i] = uint32(i)
	}
	if err := indexw.WritePostings("", "", newListPostings(all)); err != nil {
		return nil, err
	}

	return &meta, nil
}

type compactionSet interface {
	Next() bool
	At() (labels.Labels, []*ChunkMeta, intervals)
	Err() error
}

type compactionSeriesSet struct {
	p          Postings
	index      IndexReader
	chunks     ChunkReader
	tombstones TombstoneReader

	l         labels.Labels
	c         []*ChunkMeta
	intervals intervals
	err       error
}

func newCompactionSeriesSet(i IndexReader, c ChunkReader, t TombstoneReader, p Postings) *compactionSeriesSet {
	return &compactionSeriesSet{
		index:      i,
		chunks:     c,
		tombstones: t,
		p:          p,
	}
}

func (c *compactionSeriesSet) Next() bool {
	if !c.p.Next() {
		return false
	}

	c.intervals = c.tombstones.Get(c.p.At())

	c.l, c.c, c.err = c.index.Series(c.p.At())
	if c.err != nil {
		return false
	}

	// Remove completely deleted chunks.
	if len(c.intervals) > 0 {
		chks := make([]*ChunkMeta, 0, len(c.c))
		for _, chk := range c.c {
			if !(interval{chk.MinTime, chk.MaxTime}.isSubrange(c.intervals)) {
				chks = append(chks, chk)
			}
		}

		c.c = chks
	}

	for _, chk := range c.c {
		chk.Chunk, c.err = c.chunks.Chunk(chk.Ref)
		if c.err != nil {
			return false
		}
	}

	return true
}

func (c *compactionSeriesSet) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.p.Err()
}

func (c *compactionSeriesSet) At() (labels.Labels, []*ChunkMeta, intervals) {
	return c.l, c.c, c.intervals
}

type compactionMerger struct {
	a, b compactionSet

	aok, bok  bool
	l         labels.Labels
	c         []*ChunkMeta
	intervals intervals
}

type compactionSeries struct {
	labels labels.Labels
	chunks []*ChunkMeta
}

func newCompactionMerger(a, b compactionSet) (*compactionMerger, error) {
	c := &compactionMerger{
		a: a,
		b: b,
	}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	c.aok = c.a.Next()
	c.bok = c.b.Next()

	return c, c.Err()
}

func (c *compactionMerger) compare() int {
	if !c.aok {
		return 1
	}
	if !c.bok {
		return -1
	}
	a, _, _ := c.a.At()
	b, _, _ := c.b.At()
	return labels.Compare(a, b)
}

func (c *compactionMerger) Next() bool {
	if !c.aok && !c.bok || c.Err() != nil {
		return false
	}

	d := c.compare()
	// Both sets contain the current series. Chain them into a single one.
	if d > 0 {
		c.l, c.c, c.intervals = c.b.At()
		c.bok = c.b.Next()
	} else if d < 0 {
		c.l, c.c, c.intervals = c.a.At()
		c.aok = c.a.Next()
	} else {
		l, ca, ra := c.a.At()
		_, cb, rb := c.b.At()
		for _, r := range rb {
			ra = ra.add(r)
		}

		c.l = l
		c.c = append(ca, cb...)
		c.intervals = ra

		c.aok = c.a.Next()
		c.bok = c.b.Next()
	}
	return true
}

func (c *compactionMerger) Err() error {
	if c.a.Err() != nil {
		return c.a.Err()
	}
	return c.b.Err()
}

func (c *compactionMerger) At() (labels.Labels, []*ChunkMeta, intervals) {
	return c.l, c.c, c.intervals
}

func renameFile(from, to string) error {
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
	defer pdir.Close()

	if err = fileutil.Fsync(pdir); err != nil {
		return err
	}
	if err = pdir.Close(); err != nil {
		return err
	}
	return nil
}
