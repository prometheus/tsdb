package tsdb

import (
	"io/ioutil"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/fabxc/tsdb/labels"
	"github.com/pkg/errors"

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

type mockSeries struct {
	lset    labels.Labels
	samples []sample
}

func (m mockSeries) Labels() labels.Labels { return m.lset }

func (m mockSeries) Iterator() SeriesIterator {
	return newListSeriesIterator(m.samples)
}

type mockHead struct {
	dir  string
	meta BlockMeta

	postings *memPostings
	values   map[string]stringset

	// The series for this head. The refs are stored in index.
	series []mockSeries
	lref   lmap

	activeWriters uint64

	sync.RWMutex
}

func (m mockHead) Dir() string {
	return m.dir
}

func (m mockHead) Meta() BlockMeta {
	return m.meta
}

func (m mockHead) IndexReader() IndexReader {
	m.RLock()
	defer m.RUnlock()

	index := newMockIndex()
	for _, v := range m.lref.lref {
		for _, rl := range v {
			samples := m.series[rl.ref].samples

			cm := &ChunkMeta{
				Ref:     uint64(rl.ref),
				MaxTime: samples[len(samples)-1].t,
				MinTime: samples[0].t,
			}

			index.AddSeries(uint32(rl.ref), rl.lset, cm)
		}
	}

	for k, v := range m.values {
		index.WriteLabelIndex([]string{k}, v.slice())
	}

	for k, v := range m.postings.m {
		index.WritePostings(k.name, k.value, newListPostings(v))
	}

	return index
}

// TODO?: Seems to be used only while persisting.
func (m mockHead) Chunks() ChunkReader {
	return nil
}

func (m mockHead) Close() error {
	return nil
}

func (m mockHead) Querier(mint, maxt int64) Querier {
	m.RLock()
	return mockHeadQuerier{
		h:    m,
		i:    m.IndexReader(),
		mint: mint,
		maxt: maxt,
	}
}

type mockHeadQuerier struct {
	h    mockHead
	i    IndexReader
	mint int64
	maxt int64
}

func (q mockHeadQuerier) Select(ms ...labels.Matcher) SeriesSet {

	var (
		its    []Postings
		absent []string
	)
	for _, m := range ms {
		// If the matcher checks absence of a label, don't select them
		// but propagate the check into the series set.
		if _, ok := m.(*labels.EqualMatcher); ok && m.Matches("") {
			absent = append(absent, m.Name())
			continue
		}
		its = append(its, q.selectSingle(m))
	}

	p := Intersect(its...)

	slist := make([]Series, 0, 512)

	for p.Next() {
		slist = append(slist, q.h.series[p.At()])
	}

	return newListSeriesSet(slist)
}
func (q mockHeadQuerier) selectSingle(m labels.Matcher) Postings {
	tpls, err := q.i.LabelValues(m.Name())
	if err != nil {
		return errPostings{err: err}
	}
	// TODO(fabxc): use interface upgrading to provide fast solution
	// for equality and prefix matches. Tuples are lexicographically sorted.
	var res []string

	for i := 0; i < tpls.Len(); i++ {
		vals, err := tpls.At(i)
		if err != nil {
			return errPostings{err: err}
		}
		if m.Matches(vals[0]) {
			res = append(res, vals[0])
		}
	}

	if len(res) == 0 {
		return emptyPostings
	}

	var rit []Postings

	for _, v := range res {
		it, err := q.i.Postings(m.Name(), v)
		if err != nil {
			return errPostings{err: err}
		}
		rit = append(rit, it)
	}

	return Merge(rit...)
}

func (q mockHeadQuerier) LabelValues(name string) ([]string, error) {
	ns, ok := q.h.values[name]
	if !ok {
		return []string{}, nil
	}
	return ns.slice(), nil
}

func (q mockHeadQuerier) LabelValuesFor(string, labels.Label) ([]string, error) {
	return nil, nil
}

func (q mockHeadQuerier) Close() error {
	q.h.RUnlock()
	return nil
}

func (m mockHead) Appender() Appender {
	atomic.AddUint64(&m.activeWriters, 1)
	return mockHeadAppender{
		h:      m,
		series: make([][]sample, 0, 512),
		lref:   newLmap(),
	}
}

func (m mockHead) Busy() bool {
	return atomic.LoadUint64(&m.activeWriters) == 0
}

type refdLset struct {
	lset labels.Labels
	ref  int
}

type lmap struct {
	lref map[uint64][]refdLset
}

func newLmap() lmap {
	return lmap{
		lref: make(map[uint64][]refdLset),
	}
}

func (l lmap) add(lset labels.Labels, ref int) {
	h := lset.Hash()
	s, ok := l.lref[h]
	if ok {
		_, ok := l.get(lset)

		if !ok {
			l.lref[h] = append(l.lref[h], refdLset{lset, ref})
			return
		}

		for k, v := range s {
			if v.lset.Equals(lset) {
				s[k].ref = ref
				return
			}
		}
	}

	l.lref[h] = []refdLset{{lset, ref}}
	return
}

func (l lmap) get(lset labels.Labels) (int, bool) {
	h := lset.Hash()
	s, ok := l.lref[h]
	if !ok {
		return 0, ok
	}

	for _, v := range s {
		if lset.Equals(v.lset) {
			return v.ref, true
		}
	}

	return 0, false
}

type mockHeadAppender struct {
	h mockHead

	// The index here is the ref.
	series [][]sample
	lref   lmap
}

func (m mockHeadAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	ref, ok := m.lref.get(l)
	if !ok {
		m.series = append(m.series, make([]sample, 0, 512))
		m.lref.add(l, len(m.series)-1)
		ref = len(m.series) - 1
	}

	return uint64(ref), m.AddFast(uint64(ref), t, v)
}

func (m mockHeadAppender) AddFast(ref uint64, t int64, v float64) error {
	if len(m.series) <= int(ref) {
		return ErrNotFound
	}

	s := m.series[ref]
	l := len(s)
	if l == 0 {
		// TODO: Can we just use s?
		m.series[ref] = append(m.series[ref], sample{t, v})
		return nil
	}

	if t < s[l-1].t {
		return ErrOutOfOrderSample
	}

	if t == s[l-1].t && s[l-1].v != v {
		return ErrAmendSample
	}

	m.series[ref] = append(m.series[ref], sample{t, v})
	return nil
}

func (m mockHeadAppender) Commit() error {
	defer atomic.AddUint64(&m.h.activeWriters, ^uint64(0))

	m.h.Lock()
	defer m.h.Unlock()
	mint := int64(math.MaxInt64)
	maxt := int64(math.MinInt64)

	for _, v := range m.lref.lref {
		for _, rlset := range v {
			// If series exists then append, else add series and append.
			ref, ok := m.h.lref.get(rlset.lset)
			if !ok {
				ref = len(m.h.series)

				m.h.lref.add(rlset.lset, ref)
				m.h.series = append(m.h.series, mockSeries{
					lset:    rlset.lset,
					samples: make([]sample, 0, 512),
				})
				m.addIndex(rlset.lset, ref)

				m.h.meta.Stats.NumSeries++
			}

			for _, v := range m.series[rlset.ref] {
				samples := m.h.series[ref].samples
				if v.t < samples[len(samples)-1].t {
					m.h.series[ref].samples = append(m.h.series[ref].samples, v)

					if mint > v.t {
						mint = v.t
					}
					if maxt < v.t {
						maxt = v.t
					}

					m.h.meta.Stats.NumSamples++
				}
			}

		}
	}

	if m.h.meta.MaxTime < maxt {
		m.h.meta.MaxTime = maxt
	}

	if m.h.meta.MinTime > mint {
		m.h.meta.MinTime = mint
	}

	return nil
}

func (m mockHeadAppender) addIndex(lset labels.Labels, ref int) {
	for _, l := range lset {
		valset, ok := m.h.values[l.Name]
		if !ok {
			valset = stringset{}
			m.h.values[l.Name] = valset
		}
		valset.set(l.Value)

		m.h.postings.add(uint32(ref), term{name: l.Name, value: l.Value})
	}

	m.h.postings.add(uint32(ref), term{})
}

func (m mockHeadAppender) Rollback() error {
	atomic.AddUint64(&m.h.activeWriters, ^uint64(0))
	return nil
}
