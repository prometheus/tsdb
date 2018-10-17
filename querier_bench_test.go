package tsdb

import (
	"context"
	"flag"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
)

var skipTimeout bool

func init() {
	flag.BoolVar(&skipTimeout, "querier-bench.skiptimeout", false, "Skip timeout enforcement")
}

func BenchmarkBlockQuerier(b *testing.B) {
	//counts := []int{10, 100, 1000, 10000, 100000, 1000000}
	counts := []int{1000000}
	timeouts := []time.Duration{
		time.Millisecond * 100,
		//time.Second,
		//time.Minute,
	}
	for _, timeout := range timeouts {
		for _, count := range counts {
			b.Run(strconv.Itoa(count)+"/"+timeout.String(), func(b *testing.B) {
				benchmarkBlockQuerier(b, count, timeout)
			})
		}
	}
}

// benchmarkBlockQuerier simply creates chunkreaders for the number of series and
// then creates a querier that will match all of those same series
func benchmarkBlockQuerier(b *testing.B, numSeries int, timeout time.Duration) {
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

	b.Run("head", func(b *testing.B) {
		baseCtx := context.Background()

		hb, err := NewHead(nil, nil, nil, 1000000)
		testutil.Ok(b, err)
		app := hb.Appender()
		for _, seriesSample := range data {
			for _, chunk := range seriesSample.chunks {
				for _, sample := range chunk {
					app.Add(labels.FromMap(seriesSample.lset), sample.t, sample.v)
				}
			}
		}
		testutil.Ok(b, app.Commit())

		for name, q := range queries {
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					ctx := baseCtx
					if !skipTimeout {
						ctx, _ = context.WithTimeout(baseCtx, timeout)
					}

					querier, err := NewBlockQuerier(ctx, hb, q.mint, q.maxt)
					testutil.Ok(b, err)
					start := time.Now()
					_, err = querier.Select(q.ms...)
					if err != nil && err != ctx.Err() {
						b.Fatalf("Unexpected Error: %v", err)
					}
					if !skipTimeout {
						took := time.Now().Sub(start)
						// if it took >1m over the timeout, then it didn't properly timeout
						if took > (timeout + time.Millisecond) {
							b.Fatalf("didn't timeout")
						}
					}
				}
			})
		}
	})

	b.Run("block", func(b *testing.B) {
		baseCtx := context.Background()

		hb, err := NewHead(nil, nil, nil, 1000000)
		testutil.Ok(b, err)
		app := hb.Appender()
		for _, seriesSample := range data {
			for _, chunk := range seriesSample.chunks {
				for _, sample := range chunk {
					app.Add(labels.FromMap(seriesSample.lset), sample.t, sample.v)
				}
			}
		}
		testutil.Ok(b, app.Commit())

		// Create block
		tmpDir, err := ioutil.TempDir("", "blockbench")
		defer os.RemoveAll(tmpDir)
		testutil.Ok(b, err)
		db, close := openTestDB(b, nil)
		defer close()
		defer db.Close()
		ulid, err := db.compactor.Write(tmpDir, hb, 0, 5, nil)
		testutil.Ok(b, err)
		block, err := OpenBlock(path.Join(tmpDir, ulid.String()), nil)
		testutil.Ok(b, err)

		for name, q := range queries {
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					ctx := baseCtx
					if !skipTimeout {
						ctx, _ = context.WithTimeout(baseCtx, timeout)
					}
					querier, err := NewBlockQuerier(ctx, block, q.mint, q.maxt)
					testutil.Ok(b, err)
					start := time.Now()
					_, err = querier.Select(q.ms...)
					if err != nil && err != ctx.Err() {
						b.Fatalf("Unexpected Error: %v", err)
					}
					if !skipTimeout {
						took := time.Now().Sub(start)
						// if it took >1m over the timeout, then it didn't properly timeout
						if took > (timeout + time.Millisecond) {
							b.Fatalf("didn't timeout")
						}
					}
				}
			})
		}
	})

}
