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
	"sort"
	"testing"

	"github.com/prometheus/tsdb/labels"
	"github.com/stretchr/testify/require"
)

type mockSeriesIterator struct {
	seek func(int64) bool
	at   func() (int64, float64)
	next func() bool
	err  func() error
}

func (m *mockSeriesIterator) Seek(t int64) bool    { return m.seek(t) }
func (m *mockSeriesIterator) At() (int64, float64) { return m.at() }
func (m *mockSeriesIterator) Next() bool           { return m.next() }
func (m *mockSeriesIterator) Err() error           { return m.err() }

type mockSeries struct {
	labels   func() labels.Labels
	iterator func() SeriesIterator
}

func (m *mockSeries) Labels() labels.Labels    { return m.labels() }
func (m *mockSeries) Iterator() SeriesIterator { return m.iterator() }

type listSeriesIterator struct {
	list []sample
	idx  int
}

func newListSeriesIterator(list []sample) *listSeriesIterator {
	return &listSeriesIterator{list: list, idx: -1}
}

func (it *listSeriesIterator) At() (int64, float64) {
	s := it.list[it.idx]
	return s.t, s.v
}

func (it *listSeriesIterator) Next() bool {
	it.idx++
	return it.idx < len(it.list)
}

func (it *listSeriesIterator) Seek(t int64) bool {
	if it.idx == -1 {
		it.idx = 0
	}
	// Do binary search between current position and end.
	it.idx = sort.Search(len(it.list)-it.idx, func(i int) bool {
		s := it.list[i+it.idx]
		return s.t >= t
	})

	return it.idx < len(it.list)
}

func (it *listSeriesIterator) Err() error {
	return nil
}

func TestMergedSeriesSet(t *testing.T) {
	newSeries := func(l map[string]string, s []sample) Series {
		return &mockSeries{
			labels:   func() labels.Labels { return labels.FromMap(l) },
			iterator: func() SeriesIterator { return newListSeriesIterator(s) },
		}
	}

	cases := []struct {
		// The input sets in order (samples in series in b are strictly
		// after those in a).
		a, b SeriesSet
		// The composition of a and b in the partition series set must yield
		// results equivalent to the result series set.
		exp SeriesSet
	}{
		{
			a: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"a": "a",
				}, []sample{
					{t: 1, v: 1},
				}),
			}),
			b: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"a": "a",
				}, []sample{
					{t: 2, v: 2},
				}),
				newSeries(map[string]string{
					"b": "b",
				}, []sample{
					{t: 1, v: 1},
				}),
			}),
			exp: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"a": "a",
				}, []sample{
					{t: 1, v: 1},
					{t: 2, v: 2},
				}),
				newSeries(map[string]string{
					"b": "b",
				}, []sample{
					{t: 1, v: 1},
				}),
			}),
		},
		{
			a: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 1, v: 1},
				}),
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "localhost:9090",
				}, []sample{
					{t: 1, v: 2},
				}),
			}),
			b: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 2, v: 1},
				}),
				newSeries(map[string]string{
					"handler":  "query",
					"instance": "localhost:9090",
				}, []sample{
					{t: 2, v: 2},
				}),
			}),
			exp: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 1, v: 1},
					{t: 2, v: 1},
				}),
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "localhost:9090",
				}, []sample{
					{t: 1, v: 2},
				}),
				newSeries(map[string]string{
					"handler":  "query",
					"instance": "localhost:9090",
				}, []sample{
					{t: 2, v: 2},
				}),
			}),
		},
	}

Outer:
	for _, c := range cases {
		res := newMergedSeriesSet(c.a, c.b)

		for {
			eok, rok := c.exp.Next(), res.Next()
			require.Equal(t, eok, rok, "next")

			if !eok {
				continue Outer
			}
			sexp := c.exp.At()
			sres := res.At()

			require.Equal(t, sexp.Labels(), sres.Labels(), "labels")

			smplExp, errExp := expandSeriesIterator(sexp.Iterator())
			smplRes, errRes := expandSeriesIterator(sres.Iterator())

			require.Equal(t, errExp, errRes, "samples error")
			require.Equal(t, smplExp, smplRes, "samples")
		}
	}
}

func expandSeriesIterator(it SeriesIterator) (r []sample, err error) {
	for it.Next() {
		t, v := it.At()
		r = append(r, sample{t: t, v: v})
	}

	return r, it.Err()
}

func TestMergedDupSeriesIterator(t *testing.T) {
	cases := []struct {
		a, b SeriesIterator
		exp  SeriesIterator
	}{
		{
			a: newListSeriesIterator([]sample{}),
			b: newListSeriesIterator([]sample{}),

			exp: newListSeriesIterator([]sample{}),
		},
		{
			a: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{6, 1},
			}),
			b: newListSeriesIterator([]sample{}),

			exp: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{6, 1},
			}),
		},
		{
			a: newListSeriesIterator([]sample{}),
			b: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{6, 1},
			}),

			exp: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{6, 1},
			}),
		},
		{
			a: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{6, 1},
			}),
			b: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{6, 1},
			}),

			exp: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{6, 1},
			}),
		},
		{
			a: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{7, 1},
			}),
			b: newListSeriesIterator([]sample{
				{3, 5},
				{4, 3},
				{5, 5},
				{6, 9},
				{9, 10},
			}),

			exp: newListSeriesIterator([]sample{
				{1, 2},
				{2, 3},
				{3, 5},
				{4, 3},
				{5, 5},
				{6, 9},
				{7, 1},
				{9, 10},
			}),
		},
	}

	for _, tc := range cases {
		res := newMergedDupSeriesIterator(tc.a, tc.b)

		smplExp, errExp := expandSeriesIterator(tc.exp)
		smplRes, errRes := expandSeriesIterator(res)

		require.Equal(t, errExp, errRes, "samples error")
		require.Equal(t, smplExp, smplRes, "samples")
	}

	return
}

func TestMergedDupSeriesSet(t *testing.T) {
	newSeries := func(l map[string]string, s []sample) Series {
		return &mockSeries{
			labels:   func() labels.Labels { return labels.FromMap(l) },
			iterator: func() SeriesIterator { return newListSeriesIterator(s) },
		}
	}

	cases := []struct {
		a, b SeriesSet
		// The composition of a and b in the partition series set must yield
		// results equivalent to the result series set.
		exp SeriesSet
	}{
		{
			a: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"a": "a",
				}, []sample{
					{t: 1, v: 1},
				}),
			}),
			b: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"a": "a",
				}, []sample{
					{t: 2, v: 2},
				}),
				newSeries(map[string]string{
					"b": "b",
				}, []sample{
					{t: 1, v: 1},
				}),
			}),
			exp: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"a": "a",
				}, []sample{
					{t: 1, v: 1},
					{t: 2, v: 2},
				}),
				newSeries(map[string]string{
					"b": "b",
				}, []sample{
					{t: 1, v: 1},
				}),
			}),
		},
		{
			a: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 1, v: 1},
				}),
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "localhost:9090",
				}, []sample{
					{t: 1, v: 2},
				}),
			}),
			b: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 2, v: 1},
				}),
				newSeries(map[string]string{
					"handler":  "query",
					"instance": "localhost:9090",
				}, []sample{
					{t: 2, v: 2},
				}),
			}),
			exp: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 1, v: 1},
					{t: 2, v: 1},
				}),
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "localhost:9090",
				}, []sample{
					{t: 1, v: 2},
				}),
				newSeries(map[string]string{
					"handler":  "query",
					"instance": "localhost:9090",
				}, []sample{
					{t: 2, v: 2},
				}),
			}),
		},
		{
			a: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 1, v: 1},
					{t: 2, v: 2},
				}),
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "localhost:9090",
				}, []sample{
					{t: 1, v: 2},
					{t: 3, v: 3},
					{t: 4, v: 4},
				}),
			}),
			b: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 2, v: 2},
					{t: 3, v: 4},
				}),
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "localhost:9090",
				}, []sample{
					{t: 2, v: 6},
					{t: 4, v: 4},
					{t: 5, v: 7},
					{t: 6, v: 4},
				}),
				newSeries(map[string]string{
					"handler":  "query",
					"instance": "localhost:9090",
				}, []sample{
					{t: 2, v: 2},
				}),
			}),
			exp: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 1, v: 1},
					{t: 2, v: 2},
					{t: 3, v: 4},
				}),
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "localhost:9090",
				}, []sample{
					{t: 1, v: 2},
					{t: 2, v: 6},
					{t: 3, v: 3},
					{t: 4, v: 4},
					{t: 5, v: 7},
					{t: 6, v: 4},
				}),
				newSeries(map[string]string{
					"handler":  "query",
					"instance": "localhost:9090",
				}, []sample{
					{t: 2, v: 2},
				}),
			}),
		},
	}

Outer:
	for _, c := range cases {
		res := newMergedDupSeriesSet(c.a, c.b)

		for {
			eok, rok := c.exp.Next(), res.Next()
			require.Equal(t, eok, rok, "next")

			if !eok {
				continue Outer
			}
			sexp := c.exp.At()
			sres := res.At()

			require.Equal(t, sexp.Labels(), sres.Labels(), "labels")

			smplExp, errExp := expandSeriesIterator(sexp.Iterator())
			smplRes, errRes := expandSeriesIterator(sres.Iterator())

			require.Equal(t, errExp, errRes, "samples error")
			require.Equal(t, smplExp, smplRes, "samples")
		}
	}
}

func TestMergeSeriesSet(t *testing.T) {
	newSeries := func(l map[string]string, s []sample) Series {
		return &mockSeries{
			labels:   func() labels.Labels { return labels.FromMap(l) },
			iterator: func() SeriesIterator { return newListSeriesIterator(s) },
		}
	}

	cases := []struct {
		s []SeriesSet
		// The composition of a and b in the partition series set must yield
		// results equivalent to the result series set.
		exp SeriesSet
	}{
		{
			s: []SeriesSet{
				newListSeriesSet([]Series{
					newSeries(map[string]string{
						"handler":  "prometheus",
						"instance": "127.0.0.1:9090",
					}, []sample{
						{t: 1, v: 1},
						{t: 2, v: 2},
					}),
					newSeries(map[string]string{
						"handler":  "prometheus",
						"instance": "localhost:9090",
					}, []sample{
						{t: 1, v: 2},
						{t: 3, v: 3},
						{t: 4, v: 4},
					}),
				}),
				newListSeriesSet([]Series{
					newSeries(map[string]string{
						"handler":  "prometheus",
						"instance": "127.0.0.1:9090",
					}, []sample{
						{t: 2, v: 2},
						{t: 3, v: 4},
					}),
					newSeries(map[string]string{
						"handler":  "prometheus",
						"instance": "localhost:9090",
					}, []sample{
						{t: 2, v: 6},
						{t: 4, v: 4},
						{t: 5, v: 7},
						{t: 6, v: 4},
					}),
				}),
				newListSeriesSet([]Series{
					newSeries(map[string]string{
						"handler":  "prometheus",
						"instance": "127.0.0.1:9090",
					}, []sample{
						{t: 0, v: 1},
					}),
					newSeries(map[string]string{
						"handler":  "query",
						"instance": "localhost:9090",
					}, []sample{
						{t: 2, v: 2},
					}),
				}),
			},
			exp: newListSeriesSet([]Series{
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "127.0.0.1:9090",
				}, []sample{
					{t: 0, v: 1},
					{t: 1, v: 1},
					{t: 2, v: 2},
					{t: 3, v: 4},
				}),
				newSeries(map[string]string{
					"handler":  "prometheus",
					"instance": "localhost:9090",
				}, []sample{
					{t: 1, v: 2},
					{t: 2, v: 6},
					{t: 3, v: 3},
					{t: 4, v: 4},
					{t: 5, v: 7},
					{t: 6, v: 4},
				}),
				newSeries(map[string]string{
					"handler":  "query",
					"instance": "localhost:9090",
				}, []sample{
					{t: 2, v: 2},
				}),
			}),
		},
	}

Outer:
	for _, c := range cases {
		res := MergeSeriesSet(c.s...)

		for {
			eok, rok := c.exp.Next(), res.Next()
			require.Equal(t, eok, rok, "next")

			if !eok {
				continue Outer
			}
			sexp := c.exp.At()
			sres := res.At()

			require.Equal(t, sexp.Labels(), sres.Labels(), "labels")

			smplExp, errExp := expandSeriesIterator(sexp.Iterator())
			smplRes, errRes := expandSeriesIterator(sres.Iterator())

			require.Equal(t, errExp, errRes, "samples error")
			require.Equal(t, smplExp, smplRes, "samples")
		}
	}
}
