package tsdb

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/labels"

	promlabels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/stretchr/testify/require"
)

func BenchmarkCreateSeries(b *testing.B) {
	lbls, err := readPrometheusLabels("cmd/tsdb/testdata.1m", 1e6)
	require.NoError(b, err)

	b.Run("", func(b *testing.B) {
		dir, err := ioutil.TempDir("", "create_series_bench")
		require.NoError(b, err)
		defer os.RemoveAll(dir)

		h, err := createHeadBlock(dir, 0, nil, 0, 1)
		require.NoError(b, err)

		b.ReportAllocs()
		b.ResetTimer()

		for _, l := range lbls[:b.N] {
			h.create(l.Hash(), l)
		}
	})
}

func readPrometheusLabels(fn string, n int) ([]labels.Labels, error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	p := textparse.New(b)
	i := 0
	var mets []labels.Labels
	hashes := map[uint64]struct{}{}

	for p.Next() && i < n {
		m := make(labels.Labels, 0, 10)
		p.Metric((*promlabels.Labels)(unsafe.Pointer(&m)))

		h := m.Hash()
		if _, ok := hashes[h]; ok {
			continue
		}
		mets = append(mets, m)
		hashes[h] = struct{}{}
		i++
	}
	if err := p.Err(); err != nil {
		return nil, err
	}
	if i != n {
		return mets, errors.Errorf("requested %d metrics but found %d", n, i)
	}
	return mets, nil
}

func TestLatestValRead(t *testing.T) {
	dir, err := ioutil.TempDir("", "testlatestvalread")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := Open(dir, nil, nil, &Options{
		WALFlushInterval:  10 * time.Second,
		MinBlockDuration:  uint64(2 * time.Hour.Seconds() * 1000),
		MaxBlockDuration:  uint64(36 * time.Hour.Seconds() * 1000),
		AppendableBlocks:  2,
		RetentionDuration: uint64(15 * 24 * time.Hour.Seconds() * 1000),
	})

	t.Run("simple", func(t *testing.T) {
		// TODO: Rather simplified, there is only one label.
		table := []struct {
			l      []labels.Labels
			s      []sample
			cutoff int64
		}{
			{
				l: []labels.Labels{
					{{Name: "num", Value: "1"}},
					{{Name: "num", Value: "2"}},
					{{Name: "num", Value: "3"}},
					{{Name: "num", Value: "4"}},
					{{Name: "num", Value: "5"}},
					{{Name: "num", Value: "6"}},
				},
				s: []sample{
					{t: 1, v: 2},
					{t: 1, v: 2},
					{t: 1, v: 2},
					{t: 1, v: 2},
					{t: 1, v: 2},
					{t: 1, v: 2},
				},
				cutoff: 1,
			},
			{
				l: []labels.Labels{
					{{Name: "num", Value: "1"}},
					{{Name: "num", Value: "2"}},
					{{Name: "num", Value: "3"}},
					{{Name: "num", Value: "4"}},
					{{Name: "num", Value: "5"}},
					{{Name: "num", Value: "6"}},
				},
				s: []sample{
					{t: 2, v: 3},
					{t: 2, v: 3},
					{t: 2, v: 3},
					{t: 2, v: 3},
					{t: 2, v: 3},
					{t: 2, v: 3},
				},
				cutoff: 1,
			},
			{
				l: []labels.Labels{
					{{Name: "num", Value: "2"}},
					{{Name: "num", Value: "3"}},
					{{Name: "num", Value: "4"}},
					{{Name: "num", Value: "5"}},
					{{Name: "num", Value: "6"}},
				},
				s: []sample{
					{t: 3, v: 4},
					{t: 3, v: 4},
					{t: 3, v: 4},
					{t: 3, v: 4},
					{t: 3, v: 4},
				},
				cutoff: 1,
			},
		}

		m := make(map[string]sample)
		for _, tc := range table {
			app := db.Appender()
			for i, l := range tc.l {
				s := tc.s[i]
				_, err := app.Add(l, s.t, s.v)
				require.NoError(t, err)

				m[l[0].Value] = s
			}
			require.NoError(t, app.Commit())

			matcher := labels.NewEqualMatcher("num", "4")
			matcher = labels.Not(matcher)

			ss := db.QueryLatest(tc.cutoff, matcher)
			for ss.Next() {
				s := ss.At()
				it := s.Iterator()
				l := s.Labels()

				n := 0
				for it.Next() {
					n++
					time, v := it.At()
					require.True(t, time >= tc.cutoff)

					require.Equal(t, m[l[0].Value].t, time)
					require.Equal(t, m[l[0].Value].v, v)
				}

				require.NoError(t, it.Err())
				require.True(t, n <= 1)
			}
			require.NoError(t, ss.Err())

			db.cut(tc.cutoff)
		}

		return
	})

	return
}
