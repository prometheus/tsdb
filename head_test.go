package tsdb

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/go-kit/kit/log"
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

func TestDeadlock(t *testing.T) {

	dir, err := ioutil.TempDir("", "test_persistence_e2e")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	lbls, err := readPrometheusLabels("testdata/20k.series", 1)
	require.NoError(t, err)

	l := log.NewLogfmtLogger(os.Stdout)
	ts := time.Now().UnixNano() / 1000000

	hb, err := createHeadBlock(dir, 0, l, ts, ts+50*1000)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		ts := time.Now().UnixNano() / 1000000
		for j := 0; j < 10; j++ {
			app := hb.Appender()
			go ingestScrapes(t, lbls, strconv.Itoa(j), ts, app)
		}
		time.Sleep(1 * time.Second)
	}

	fmt.Println(hb.Meta().Stats, hb.Busy())
}

func ingestScrapes(
	t *testing.T,
	l []labels.Labels,
	instance string,
	ts int64,
	app Appender,
) {
	for _, lset := range l {
		lset = append(lset, labels.Label{Name: "instance", Value: instance})
		_, err := app.Add(lset, ts, rand.Float64())
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	return
}
