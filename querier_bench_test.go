package tsdb

import (
	"strconv"
	"testing"

	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
)

func BenchmarkBlockQuerier(b *testing.B) {
	for _, count := range []int{10, 100, 1000, 10000, 100000, 1000000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			benchmarkBlockQuerier(b, count)
		})
	}
}

// benchmarkBlockQuerier simply creates chunkreaders for the number of series and
// then creates a querier that will match all of those same series
func benchmarkBlockQuerier(b *testing.B, numSeries int) {
	type query struct {
		mint, maxt int64
		ms         []labels.Matcher
		exp        SeriesSet
	}

	data := make([]seriesSamples, numSeries)

	// Generate the series
	for i := 0; i < numSeries; i++ {
		data[i] = seriesSamples{
			lset: map[string]string{
				"a": strconv.Itoa(i),
				"b": "b",
			},
			chunks: [][]sample{
				{
					{1, 2}, {2, 3}, {3, 4},
				},
			},
		}
	}

	all, _ := labels.NewRegexpMatcher("a", ".*")
	some, _ := labels.NewRegexpMatcher("a", "1.*")
	queries := map[string]query{
		"regex_all": query{
			mint: 0,
			maxt: 100,
			ms:   []labels.Matcher{all},
		},
		"equal_all": {
			mint: 0,
			maxt: 100,
			ms:   []labels.Matcher{labels.NewEqualMatcher("b", "b")},
		},
		"first": {
			mint: 0,
			maxt: 100,
			ms:   []labels.Matcher{labels.NewEqualMatcher("a", "1")},
		},
		"last": {
			mint: 0,
			maxt: 100,
			ms:   []labels.Matcher{labels.NewEqualMatcher("a", strconv.Itoa(numSeries-1))},
		},
		"regex_subset": query{
			mint: 0,
			maxt: 100,
			ms:   []labels.Matcher{some},
		},
	}

	ir, cr := createIdxChkReaders(data)

	for name, q := range queries {
		b.Run(name, func(b *testing.B) {
			querier := &blockQuerier{
				index:      ir,
				chunks:     cr,
				tombstones: NewMemTombstones(),

				mint: q.mint,
				maxt: q.maxt,
			}
			for i := 0; i < b.N; i++ {
				_, err := querier.Select(q.ms...)
				testutil.Ok(b, err)
			}
		})
	}
}
