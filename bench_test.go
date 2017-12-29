package tsdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
)

func benchInMemQuery(b *testing.B, series []labels.Labels, selector labels.Selector, expand bool) {
	hb, err := createHB(series)
	testutil.Ok(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := NewBlockQuerier(hb, 0, 10)
		testutil.Ok(b, err)

		ss, err := q.Select(selector...)
		testutil.Ok(b, err)
		if expand {
			for ss.Next() {
				s := ss.At()
				s.Labels()
				s.Iterator()
			}
			testutil.Ok(b, ss.Err())
		}

		testutil.Ok(b, q.Close())
	}
}

func benchPersistedQuery(b *testing.B, series []labels.Labels, selector labels.Selector, expand bool) {
	hb, err := createHB(series)
	testutil.Ok(b, err)

	compactor, err := NewLeveledCompactor(nil, nil, []int64{1000000}, nil)
	testutil.Ok(b, err)

	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(b, err)
	defer os.RemoveAll(tmpdir)

	ulid, err := compactor.Write(tmpdir, hb, hb.MinTime(), hb.MaxTime())
	testutil.Ok(b, err)

	block, err := OpenBlock(filepath.Join(tmpdir, ulid.String()), nil)
	testutil.Ok(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		q, err := NewBlockQuerier(block, 0, 10)
		testutil.Ok(b, err)

		ss, err := q.Select(selector...)
		testutil.Ok(b, err)
		if expand {
			for ss.Next() {
				s := ss.At()
				s.Labels()
				s.Iterator()
			}
			testutil.Ok(b, ss.Err())
		}

		testutil.Ok(b, q.Close())
	}
}

func createHB(
	series []labels.Labels,
) (*Head, error) {
	hb, err := NewHead(nil, nil, NopWAL(), 10*60*60*1000)
	if err != nil {
		return nil, err
	}

	app := hb.Appender()
	for _, l := range series {
		if _, err := app.Add(l, 1, 0); err != nil {
			return nil, err
		}
	}

	if err := app.Commit(); err != nil {
		return nil, err
	}

	return hb, nil
}

func BenchmarkInMemQueries_Series_1M_EQSelector_1_NoExpansion(b *testing.B) {
	series := genSeries(6, 10)

	benchInMemQuery(b, series, labels.Selector{labels.NewEqualMatcher("label-1", "value-5")}, false)
}

func BenchmarkInMemQueries_Series_1M_EQSelector_1_Expansion(b *testing.B) {
	series := genSeries(6, 10)

	benchInMemQuery(b, series, labels.Selector{labels.NewEqualMatcher("label-1", "value-5")}, true)
}

func BenchmarkInMemQueries_Series_1M_EQSelector_2_NoExpansion(b *testing.B) {
	series := genSeries(6, 10)

	benchInMemQuery(b, series, labels.Selector{
		labels.NewEqualMatcher("label-1", "value-5"),
		labels.NewEqualMatcher("label-2", "value-4"),
	}, false)
}

func BenchmarkInMemQueries_Series_1M_EQSelector_2_Expansion(b *testing.B) {
	series := genSeries(6, 10)

	benchInMemQuery(b, series, labels.Selector{
		labels.NewEqualMatcher("label-1", "value-5"),
		labels.NewEqualMatcher("label-2", "value-4"),
	}, true)
}

func BenchmarkInMemQueries_Series_1M_EQSelector_3_NoExpansion(b *testing.B) {
	series := genSeries(6, 10)

	benchInMemQuery(b, series, labels.Selector{
		labels.NewEqualMatcher("label-1", "value-5"),
		labels.NewEqualMatcher("label-2", "value-4"),
		labels.NewEqualMatcher("label-3", "value-4"),
	}, false)
}

func BenchmarkInMemQueries_Series_1M_EQSelector_3_Expansion(b *testing.B) {
	series := genSeries(6, 10)

	benchInMemQuery(b, series, labels.Selector{
		labels.NewEqualMatcher("label-1", "value-5"),
		labels.NewEqualMatcher("label-2", "value-4"),
		labels.NewEqualMatcher("label-3", "value-4"),
	}, true)
}

func BenchmarkPersistedQueries_Series_1M_EQSelector_1_NoExpansion(b *testing.B) {
	series := genSeries(6, 10)

	benchPersistedQuery(b, series, labels.Selector{labels.NewEqualMatcher("label-1", "value-5")}, false)
}

func BenchmarkPersistedQueries_Series_1M_EQSelector_1_Expansion(b *testing.B) {
	series := genSeries(6, 10)

	benchPersistedQuery(b, series, labels.Selector{labels.NewEqualMatcher("label-1", "value-5")}, true)
}

func BenchmarkPersistedQueries_Series_1M_EQSelector_2_NoExpansion(b *testing.B) {
	series := genSeries(6, 10)

	benchPersistedQuery(b, series, labels.Selector{
		labels.NewEqualMatcher("label-1", "value-5"),
		labels.NewEqualMatcher("label-2", "value-4"),
	}, false)
}

func BenchmarkPersistedQueries_Series_1M_EQSelector_2_Expansion(b *testing.B) {
	series := genSeries(6, 10)

	benchPersistedQuery(b, series, labels.Selector{
		labels.NewEqualMatcher("label-1", "value-5"),
		labels.NewEqualMatcher("label-2", "value-4"),
	}, true)
}

func BenchmarkPersistedQueries_Series_1M_EQSelector_3_NoExpansion(b *testing.B) {
	series := genSeries(6, 10)

	benchPersistedQuery(b, series, labels.Selector{
		labels.NewEqualMatcher("label-1", "value-5"),
		labels.NewEqualMatcher("label-2", "value-4"),
		labels.NewEqualMatcher("label-3", "value-4"),
	}, false)
}

func BenchmarkPersistedQueries_Series_1M_EQSelector_3_Expansion(b *testing.B) {
	series := genSeries(6, 10)

	benchPersistedQuery(b, series, labels.Selector{
		labels.NewEqualMatcher("label-1", "value-5"),
		labels.NewEqualMatcher("label-2", "value-4"),
		labels.NewEqualMatcher("label-3", "value-4"),
	}, true)
}

// Meta helpers.
func genSeries(numLabels, numVals int) []labels.Labels {
	labelPrefix := "label-"
	valuePrefix := "value-"

	vals := make([]int, numLabels)
	permuts := &([][]int{})

	genSeriesRec(0, numLabels, numVals, vals, permuts)

	series := make([]labels.Labels, 0)

	for _, vals := range *permuts {
		l := labels.Labels{}
		for i, v := range vals {
			l = append(l, labels.Label{
				Name:  fmt.Sprintf("%s%d", labelPrefix, i),
				Value: fmt.Sprintf("%s%d", valuePrefix, v),
			})
		}

		series = append(series, l)
	}

	return series
}

func genSeriesRec(idx, numLabels, numVals int, vals []int, series *[][]int) {
	if idx == numLabels {
		vals2 := make([]int, len(vals))
		copy(vals2, vals)
		*series = append(*series, vals2)
		return
	}

	for i := 0; i < numVals; i++ {
		vals[idx] = i
		genSeriesRec(idx+1, numLabels, numVals, vals, series)
	}
}
