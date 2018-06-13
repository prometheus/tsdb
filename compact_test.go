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
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
)

func TestSplitByRange(t *testing.T) {
	cases := []struct {
		trange int64
		ranges [][2]int64
		output [][][2]int64
	}{
		{
			trange: 60,
			ranges: [][2]int64{{0, 10}},
			output: [][][2]int64{
				{{0, 10}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{0, 60}},
			output: [][][2]int64{
				{{0, 60}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{0, 10}, {9, 15}, {30, 60}},
			output: [][][2]int64{
				{{0, 10}, {9, 15}, {30, 60}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{70, 90}, {125, 130}, {130, 180}, {1000, 1001}},
			output: [][][2]int64{
				{{70, 90}},
				{{125, 130}, {130, 180}},
				{{1000, 1001}},
			},
		},
		// Mis-aligned or too-large blocks are ignored.
		{
			trange: 60,
			ranges: [][2]int64{{50, 70}, {70, 80}},
			output: [][][2]int64{
				{{70, 80}},
			},
		},
		{
			trange: 72,
			ranges: [][2]int64{{0, 144}, {144, 216}, {216, 288}},
			output: [][][2]int64{
				{{144, 216}},
				{{216, 288}},
			},
		},
		// Various awkward edge cases easy to hit with negative numbers.
		{
			trange: 60,
			ranges: [][2]int64{{-10, -5}},
			output: [][][2]int64{
				{{-10, -5}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{-60, -50}, {-10, -5}},
			output: [][][2]int64{
				{{-60, -50}, {-10, -5}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{-60, -50}, {-10, -5}, {0, 15}},
			output: [][][2]int64{
				{{-60, -50}, {-10, -5}},
				{{0, 15}},
			},
		},
	}

	for _, c := range cases {
		// Transform input range tuples into dirMetas.
		blocks := make([]dirMeta, 0, len(c.ranges))
		for _, r := range c.ranges {
			blocks = append(blocks, dirMeta{
				meta: &BlockMeta{
					MinTime: r[0],
					MaxTime: r[1],
				},
			})
		}

		// Transform output range tuples into dirMetas.
		exp := make([][]dirMeta, len(c.output))
		for i, group := range c.output {
			for _, r := range group {
				exp[i] = append(exp[i], dirMeta{
					meta: &BlockMeta{MinTime: r[0], MaxTime: r[1]},
				})
			}
		}

		testutil.Equals(t, exp, splitByRange(blocks, c.trange))
	}
}

// See https://github.com/prometheus/prometheus/issues/3064
func TestNoPanicFor0Tombstones(t *testing.T) {
	metas := []dirMeta{
		{
			dir: "1",
			meta: &BlockMeta{
				MinTime: 0,
				MaxTime: 100,
			},
		},
		{
			dir: "2",
			meta: &BlockMeta{
				MinTime: 101,
				MaxTime: 200,
			},
		},
	}

	c, err := NewLeveledCompactor(nil, nil, []int64{50}, nil)
	testutil.Ok(t, err)

	c.plan(metas)
}

func TestLeveledCompactor_plan(t *testing.T) {
	// This mimicks our default ExponentialBlockRanges with min block size equals to 20.
	compactor, err := NewLeveledCompactor(nil, nil, []int64{
		20,
		60,
		180,
		540,
		1620,
	}, nil)
	testutil.Ok(t, err)

	cases := []struct {
		metas    []dirMeta
		expected []string
	}{
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
			},
			expected: nil,
		},
		// We should wait for four blocks of size 20 to appear before compacting.
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
			},
			expected: nil,
		},
		// We should wait for a next block of size 20 to appear before compacting
		// the existing ones. We have three, but we ignore the fresh one from WAl.
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 40, 60, nil),
			},
			expected: nil,
		},
		// Block to fill the entire parent range appeared – should be compacted.
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 40, 60, nil),
				metaRange("4", 60, 80, nil),
			},
			expected: []string{"1", "2", "3"},
		},
		// Block for the next parent range appeared with gap with size 20. Nothing will happen in the first one
		// anymore but we ignore fresh one still, so no compaction.
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 60, 80, nil),
			},
			expected: nil,
		},
		// Block for the next parent range appeared, and we have a gap with size 20 between second and third block.
		// We will not get this missed gap anymore and we should compact just these two.
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 60, 80, nil),
				metaRange("4", 80, 100, nil),
			},
			expected: []string{"1", "2"},
		},
		{
			// We have 20, 20, 20, 60, 60 range blocks. "5" is marked as fresh one.
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 40, 60, nil),
				metaRange("4", 60, 120, nil),
				metaRange("5", 120, 180, nil),
			},
			expected: []string{"1", "2", "3"},
		},
		{
			// We have 20, 60, 20, 60, 240 range blocks. We can compact 20 + 60 + 60.
			metas: []dirMeta{
				metaRange("2", 20, 40, nil),
				metaRange("4", 60, 120, nil),
				metaRange("5", 960, 980, nil), // Fresh one.
				metaRange("6", 120, 180, nil),
				metaRange("7", 720, 960, nil),
			},
			expected: []string{"2", "4", "6"},
		},
		// Do not select large blocks that have many tombstones when there is no fresh block.
		{
			metas: []dirMeta{
				metaRange("1", 0, 540, &BlockStats{
					NumSeries:     10,
					NumTombstones: 3,
				}),
			},
			expected: nil,
		},
		// Select large blocks that have many tombstones when fresh appears.
		{
			metas: []dirMeta{
				metaRange("1", 0, 540, &BlockStats{
					NumSeries:     10,
					NumTombstones: 3,
				}),
				metaRange("2", 540, 560, nil),
			},
			expected: []string{"1"},
		},
		// For small blocks, do not compact tombstones, even when fresh appears.
		{
			metas: []dirMeta{
				metaRange("1", 0, 60, &BlockStats{
					NumSeries:     10,
					NumTombstones: 3,
				}),
				metaRange("2", 60, 80, nil),
			},
			expected: nil,
		},
		// Regression test: we were stuck in a compact loop where we always recompacted
		// the same block when tombstones and series counts were zero.
		{
			metas: []dirMeta{
				metaRange("1", 0, 540, &BlockStats{
					NumSeries:     0,
					NumTombstones: 0,
				}),
				metaRange("2", 540, 560, nil),
			},
			expected: nil,
		},
		// Regression test: we were wrongly assuming that new block is fresh from WAL when its ULID is newest.
		// We need to actually look on max time instead.
		//
		// With previous, wrong approach "8" block was ignored, so we were wrongly compacting 5 and 7 and introducing
		// block overlaps.
		{
			metas: []dirMeta{
				metaRange("5", 0, 360, nil),
				metaRange("6", 540, 560, nil), // Fresh one.
				metaRange("7", 360, 420, nil),
				metaRange("8", 420, 540, nil),
			},
			expected: []string{"7", "8"},
		},
	}

	for _, c := range cases {
		if !t.Run("", func(t *testing.T) {
			res, err := compactor.plan(c.metas)
			testutil.Ok(t, err)

			testutil.Equals(t, c.expected, res)
		}) {
			return
		}
	}
}

func TestRangeWithFailedCompactionWontGetSelected(t *testing.T) {
	compactor, err := NewLeveledCompactor(nil, nil, []int64{
		20,
		60,
		240,
		720,
		2160,
	}, nil)
	testutil.Ok(t, err)

	cases := []struct {
		metas []dirMeta
	}{
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 40, 60, nil),
				metaRange("4", 60, 80, nil),
			},
		},
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 60, 80, nil),
				metaRange("4", 80, 100, nil),
			},
		},
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 40, 60, nil),
				metaRange("4", 60, 120, nil),
				metaRange("5", 120, 180, nil),
				metaRange("6", 180, 200, nil),
			},
		},
	}

	for _, c := range cases {
		c.metas[1].meta.Compaction.Failed = true
		res, err := compactor.plan(c.metas)
		testutil.Ok(t, err)

		testutil.Equals(t, []string(nil), res)
	}
}

func TestCompactionFailWillCleanUpTempDir(t *testing.T) {
	compactor, err := NewLeveledCompactor(nil, log.NewNopLogger(), []int64{
		20,
		60,
		240,
		720,
		2160,
	}, nil)
	testutil.Ok(t, err)

	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpdir)

	testutil.NotOk(t, compactor.write(tmpdir, &BlockMeta{}, erringBReader{}))
	_, err = os.Stat(filepath.Join(tmpdir, BlockMeta{}.ULID.String()) + ".tmp")
	testutil.Assert(t, os.IsNotExist(err), "directory is not cleaned up")
}

func metaRange(name string, mint, maxt int64, stats *BlockStats) dirMeta {
	meta := &BlockMeta{MinTime: mint, MaxTime: maxt}
	if stats != nil {
		meta.Stats = *stats
	}
	return dirMeta{
		dir:  name,
		meta: meta,
	}
}

type erringBReader struct{}

func (erringBReader) Index() (IndexReader, error)          { return nil, errors.New("index") }
func (erringBReader) Chunks() (ChunkReader, error)         { return nil, errors.New("chunks") }
func (erringBReader) Tombstones() (TombstoneReader, error) { return nil, errors.New("tombstones") }

type mockedBReader struct {
	ir IndexReader
	cr ChunkReader
}

func (r *mockedBReader) Index() (IndexReader, error)          { return r.ir, nil }
func (r *mockedBReader) Chunks() (ChunkReader, error)         { return r.cr, nil }
func (r *mockedBReader) Tombstones() (TombstoneReader, error) { return NewMemTombstones(), nil }

type mockedIndexWriter struct {
	series []seriesSamples
}

func (mockedIndexWriter) AddSymbols(sym map[string]struct{}) error { return nil }
func (m *mockedIndexWriter) AddSeries(ref uint64, l labels.Labels, chunks ...chunks.Meta) error {
	i := -1
	for j, s := range m.series {
		if !labels.FromMap(s.lset).Equals(l) {
			continue
		}
		i = j
		break
	}
	if i == -1 {
		m.series = append(m.series, seriesSamples{
			lset: l.Map(),
		})
		i = len(m.series) - 1
	}

	for _, chk := range chunks {
		samples := make([]sample, 0, chk.Chunk.NumSamples())

		iter := chk.Chunk.Iterator()
		for iter.Next() {
			s := sample{}
			s.t, s.v = iter.At()

			samples = append(samples, s)
		}
		if err := iter.Err(); err != nil {
			return err
		}

		m.series[i].chunks = append(m.series[i].chunks, samples)
	}
	return nil
}

func (mockedIndexWriter) WriteLabelIndex(names []string, values []string) error     { return nil }
func (mockedIndexWriter) WritePostings(name, value string, it index.Postings) error { return nil }
func (mockedIndexWriter) Close() error                                              { return nil }

type nopChunkWriter struct{}

func (nopChunkWriter) WriteChunks(chunks ...chunks.Meta) error { return nil }
func (nopChunkWriter) Close() error                            { return nil }

var populateBlocksCases = []struct {
	inputBlocks    [][]seriesSamples
	compactMinTime int64
	compactMaxTime int64 // by default it is math.MaxInt64

	expectedBlock []seriesSamples
	expectedErr   error
}{
	{
		// Populate block from empty input should return error.
		inputBlocks: [][]seriesSamples{},
		expectedErr: errors.New("cannot populate block from no readers"),
	},
	{
		// Populate from single block without chunks. We expect these kind of series being ignored.
		inputBlocks: [][]seriesSamples{
			{
				{
					lset: map[string]string{"a": "b"},
				},
			},
		},
	},
	{
		// Populate from single block. We expect the same samples at the output.
		inputBlocks: [][]seriesSamples{
			{
				{
					lset:   map[string]string{"a": "b"},
					chunks: [][]sample{{{t: 0}, {t: 10}}, {{t: 11}, {t: 20}}},
				},
			},
		},
		expectedBlock: []seriesSamples{
			{
				lset:   map[string]string{"a": "b"},
				chunks: [][]sample{{{t: 0}, {t: 10}}, {{t: 11}, {t: 20}}},
			},
		},
	},
	{
		// Populate from two blocks.
		inputBlocks: [][]seriesSamples{
			{
				{
					lset:   map[string]string{"a": "b"},
					chunks: [][]sample{{{t: 0}, {t: 10}}, {{t: 11}, {t: 20}}},
				},
				{
					lset:   map[string]string{"a": "c"},
					chunks: [][]sample{{{t: 1}, {t: 9}}, {{t: 10}, {t: 19}}},
				},
				{
					// no-chunk series should be dropped.
					lset: map[string]string{"a": "empty"},
				},
			},
			{
				{
					lset:   map[string]string{"a": "b"},
					chunks: [][]sample{{{t: 21}, {t: 30}}},
				},
				{
					lset:   map[string]string{"a": "c"},
					chunks: [][]sample{{{t: 40}, {t: 45}}},
				},
			},
		},
		expectedBlock: []seriesSamples{
			{
				lset:   map[string]string{"a": "b"},
				chunks: [][]sample{{{t: 0}, {t: 10}}, {{t: 11}, {t: 20}}, {{t: 21}, {t: 30}}},
			},
			{
				lset:   map[string]string{"a": "c"},
				chunks: [][]sample{{{t: 1}, {t: 9}}, {{t: 10}, {t: 19}}, {{t: 40}, {t: 45}}},
			},
		},
	},
	{
		// Populate from two blocks showing that order is maintained.
		inputBlocks: [][]seriesSamples{
			{
				{
					lset:   map[string]string{"a": "b"},
					chunks: [][]sample{{{t: 21}, {t: 30}}},
				},
				{
					lset:   map[string]string{"a": "c"},
					chunks: [][]sample{{{t: 40}, {t: 45}}},
				},
			},
			{
				{
					lset:   map[string]string{"a": "b"},
					chunks: [][]sample{{{t: 0}, {t: 10}}, {{t: 11}, {t: 20}}},
				},
				{
					lset:   map[string]string{"a": "c"},
					chunks: [][]sample{{{t: 1}, {t: 9}}, {{t: 10}, {t: 19}}},
				},
			},
		},
		expectedBlock: []seriesSamples{
			{
				lset:   map[string]string{"a": "b"},
				chunks: [][]sample{{{t: 21}, {t: 30}}, {{t: 0}, {t: 10}}, {{t: 11}, {t: 20}}},
			},
			{
				lset:   map[string]string{"a": "c"},
				chunks: [][]sample{{{t: 40}, {t: 45}}, {{t: 1}, {t: 9}}, {{t: 10}, {t: 19}}},
			},
		},
	},
	{
		// Populate from two blocks showing that order or series is sorted.
		inputBlocks: [][]seriesSamples{
			{
				{
					lset:   map[string]string{"a": "4"},
					chunks: [][]sample{{{t: 5}, {t: 7}}},
				},
				{
					lset:   map[string]string{"a": "3"},
					chunks: [][]sample{{{t: 5}, {t: 6}}},
				},
				{
					lset:   map[string]string{"a": "same"},
					chunks: [][]sample{{{t: 1}, {t: 4}}},
				},
			},
			{
				{
					lset:   map[string]string{"a": "2"},
					chunks: [][]sample{{{t: 1}, {t: 3}}},
				},
				{
					lset:   map[string]string{"a": "1"},
					chunks: [][]sample{{{t: 1}, {t: 2}}},
				},
				{
					lset:   map[string]string{"a": "same"},
					chunks: [][]sample{{{t: 5}, {t: 8}}},
				},
			},
		},
		expectedBlock: []seriesSamples{
			{
				lset:   map[string]string{"a": "1"},
				chunks: [][]sample{{{t: 1}, {t: 2}}},
			},
			{
				lset:   map[string]string{"a": "2"},
				chunks: [][]sample{{{t: 1}, {t: 3}}},
			},
			{
				lset:   map[string]string{"a": "3"},
				chunks: [][]sample{{{t: 5}, {t: 6}}},
			},
			{
				lset:   map[string]string{"a": "4"},
				chunks: [][]sample{{{t: 5}, {t: 7}}},
			},
			{
				lset:   map[string]string{"a": "same"},
				chunks: [][]sample{{{t: 1}, {t: 4}}, {{t: 5}, {t: 8}}},
			},
		},
	},
	{
		// Populate from single block containing chunk outside of compact meta time range.
		// This should not happened because head block is making sure the chunks are not crossing block boundaries.
		inputBlocks: [][]seriesSamples{
			{
				{
					lset:   map[string]string{"a": "b"},
					chunks: [][]sample{{{t: 1}, {t: 2}}, {{t: 10}, {t: 30}}},
				},
			},
		},
		compactMinTime: 0,
		compactMaxTime: 20,
		expectedErr:    errors.New("found chunk with minTime: 10 maxTime: 30 outside of compacted minTime: 0 maxTime: 20"),
	},
	{
		// Populate from single block containing extra chunk introduced by https://github.com/prometheus/tsdb/issues/347.
		inputBlocks: [][]seriesSamples{
			{
				{
					lset:   map[string]string{"a": "issue347"},
					chunks: [][]sample{{{t: 1}, {t: 2}}, {{t: 10}, {t: 20}}},
				},
			},
		},
		compactMinTime: 0,
		compactMaxTime: 10,
		expectedErr:    errors.New("found chunk with minTime: 10 maxTime: 20 outside of compacted minTime: 0 maxTime: 10"),
	},
	{
		// Populate from two blocks containing duplicated chunk.
		// No special deduplication expected.
		inputBlocks: [][]seriesSamples{
			{
				{
					lset:   map[string]string{"a": "b"},
					chunks: [][]sample{{{t: 1}, {t: 2}}, {{t: 10}, {t: 20}}},
				},
			},
			{
				{
					lset:   map[string]string{"a": "b"},
					chunks: [][]sample{{{t: 10}, {t: 20}}},
				},
			},
		},
		expectedBlock: []seriesSamples{
			{
				lset:   map[string]string{"a": "b"},
				chunks: [][]sample{{{t: 1}, {t: 2}}, {{t: 10}, {t: 20}}, {{t: 10}, {t: 20}}},
			},
		},
	},
}

func TestCompaction_populateBlock(t *testing.T) {
	for _, tc := range populateBlocksCases {
		if ok := t.Run("", func(t *testing.T) {
			blocks := make([]BlockReader, 0, len(tc.inputBlocks))
			for _, b := range tc.inputBlocks {
				ir, cr := createIdxChkReaders(b)
				blocks = append(blocks, &mockedBReader{ir: ir, cr: cr})
			}

			c, err := NewLeveledCompactor(nil, nil, []int64{0}, nil)
			testutil.Ok(t, err)

			meta := &BlockMeta{
				MinTime: tc.compactMinTime,
				MaxTime: tc.compactMaxTime,
			}
			if meta.MaxTime == 0 {
				meta.MaxTime = math.MaxInt64
			}

			iw := &mockedIndexWriter{}
			err = c.populateBlock(blocks, meta, iw, nopChunkWriter{})
			if tc.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)

			testutil.Equals(t, tc.expectedBlock, iw.series)

			// Check if stats are calculated properly.
			s := BlockStats{
				NumSeries: uint64(len(tc.expectedBlock)),
			}
			for _, series := range tc.expectedBlock {
				s.NumChunks += uint64(len(series.chunks))
				for _, chk := range series.chunks {
					s.NumSamples += uint64(len(chk))
				}
			}
			testutil.Equals(t, s, meta.Stats)
		}); !ok {
			return
		}
	}
}
