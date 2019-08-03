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

package index

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/encoding"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
)

type series struct {
	l      labels.Labels
	chunks []chunks.Meta
}

type mockIndex struct {
	series     map[uint64]series
	labelIndex map[string][]string
	postings   map[labels.Label][]uint64
	symbols    map[string]struct{}
}

func newMockIndex() mockIndex {
	ix := mockIndex{
		series:     make(map[uint64]series),
		labelIndex: make(map[string][]string),
		postings:   make(map[labels.Label][]uint64),
		symbols:    make(map[string]struct{}),
	}
	return ix
}

func (m mockIndex) Symbols() (map[string]struct{}, error) {
	return m.symbols, nil
}

func (m mockIndex) AddSeries(ref uint64, l labels.Labels, chunks ...chunks.Meta) error {
	if _, ok := m.series[ref]; ok {
		return errors.Errorf("series with reference %d already added", ref)
	}
	for _, lbl := range l {
		m.symbols[lbl.Name] = struct{}{}
		m.symbols[lbl.Value] = struct{}{}
	}

	s := series{l: l}
	// Actual chunk data is not stored in the index.
	for _, c := range chunks {
		c.Chunk = nil
		s.chunks = append(s.chunks, c)
	}
	m.series[ref] = s

	return nil
}

func (m mockIndex) WriteLabelIndex(names []string, values []string) error {
	// TODO support composite indexes
	if len(names) != 1 {
		return errors.New("composite indexes not supported yet")
	}
	sort.Strings(values)
	m.labelIndex[names[0]] = values
	return nil
}

func (m mockIndex) WritePostings(name, value string, it Postings) error {
	l := labels.Label{Name: name, Value: value}
	if _, ok := m.postings[l]; ok {
		return errors.Errorf("postings for %s already added", l)
	}
	ep, err := ExpandPostings(it)
	if err != nil {
		return err
	}
	m.postings[l] = ep
	return nil
}

func (m mockIndex) Close() error {
	return nil
}

func (m mockIndex) LabelValues(names ...string) (StringTuples, error) {
	// TODO support composite indexes
	if len(names) != 1 {
		return nil, errors.New("composite indexes not supported yet")
	}

	return NewStringTuples(m.labelIndex[names[0]], 1)
}

func (m mockIndex) Postings(name, value string) (Postings, error) {
	l := labels.Label{Name: name, Value: value}
	return NewListPostings(m.postings[l]), nil
}

func (m mockIndex) SortedPostings(p Postings) Postings {
	ep, err := ExpandPostings(p)
	if err != nil {
		return ErrPostings(errors.Wrap(err, "expand postings"))
	}

	sort.Slice(ep, func(i, j int) bool {
		return labels.Compare(m.series[ep[i]].l, m.series[ep[j]].l) < 0
	})
	return NewListPostings(ep)
}

func (m mockIndex) Series(ref uint64, lset *labels.Labels, chks *[]chunks.Meta) error {
	s, ok := m.series[ref]
	if !ok {
		return errors.New("not found")
	}
	*lset = append((*lset)[:0], s.l...)
	*chks = append((*chks)[:0], s.chunks...)

	return nil
}

func (m mockIndex) LabelIndices() ([][]string, error) {
	res := make([][]string, 0, len(m.labelIndex))
	for k := range m.labelIndex {
		res = append(res, []string{k})
	}
	return res, nil
}

func TestIndexRW_Create_Open(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_index_create")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	fn := filepath.Join(dir, indexFilename)

	// An empty index must still result in a readable file.
	iw, err := NewWriter(fn)
	testutil.Ok(t, err)
	testutil.Ok(t, iw.Close())

	ir, err := NewFileReader(fn)
	testutil.Ok(t, err)
	testutil.Ok(t, ir.Close())

	// Modify magic header must cause open to fail.
	f, err := os.OpenFile(fn, os.O_WRONLY, 0666)
	testutil.Ok(t, err)
	_, err = f.WriteAt([]byte{0, 0}, 0)
	testutil.Ok(t, err)
	f.Close()

	_, err = NewFileReader(dir)
	testutil.NotOk(t, err)
}

func TestIndexRW_Postings(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_index_postings")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	fn := filepath.Join(dir, indexFilename)

	iw, err := NewWriter(fn)
	testutil.Ok(t, err)

	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "1", "b", "3"),
		labels.FromStrings("a", "1", "b", "4"),
	}

	err = iw.AddSymbols(map[string]struct{}{
		"a": {},
		"b": {},
		"1": {},
		"2": {},
		"3": {},
		"4": {},
	})
	testutil.Ok(t, err)

	// Postings lists are only written if a series with the respective
	// reference was added before.
	testutil.Ok(t, iw.AddSeries(1, series[0]))
	testutil.Ok(t, iw.AddSeries(2, series[1]))
	testutil.Ok(t, iw.AddSeries(3, series[2]))
	testutil.Ok(t, iw.AddSeries(4, series[3]))

	err = iw.WritePostings("a", "1", newListPostings(1, 2, 3, 4))
	testutil.Ok(t, err)

	testutil.Ok(t, iw.Close())

	ir, err := NewFileReader(fn)
	testutil.Ok(t, err)

	p, err := ir.Postings("a", "1")
	testutil.Ok(t, err)

	var l labels.Labels
	var c []chunks.Meta

	for i := 0; p.Next(); i++ {
		err := ir.Series(p.At(), &l, &c)

		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(c))
		testutil.Equals(t, series[i], l)
	}
	testutil.Ok(t, p.Err())

	testutil.Ok(t, ir.Close())
}

func rewriteIndex(inputFilePath, outputFilePath string) int {
	var (
		labelsBuf         labels.Labels
		chunksBuf         []chunks.Meta
		err               error
		apkName, apkValue = AllPostingsKey()
		values            = map[string]map[string]struct{}{}
		lenCount          = map[int]int{}
		larger1           = map[int]map[int]int{}
		// larger2           = map[int]map[int]int{}
		// count             = uint64(0)
	)

	indexr, err := NewFileReader(inputFilePath)
	if err != nil {
		fmt.Println("cannot create index reader")
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer indexr.Close()

	// Rename the symbols.
	originalSymbols, err := indexr.Symbols()
	if err != nil {
		fmt.Println("index reader symbols")
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	indexw, err := NewWriter(outputFilePath)
	if err != nil {
		fmt.Println("index writer")
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer indexw.Close()

	// Write symbols.
	if err := indexw.AddSymbols(originalSymbols); err != nil {
		fmt.Println("index writer symbols")
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// Write Series.
	posts, err := indexr.Postings1(apkName, apkValue)
	if err != nil {
		fmt.Println("index reader postings")
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	for posts.Next() {
		p := posts.At()
		labelsBuf = labelsBuf[:0]
		chunksBuf = chunksBuf[:0]

		if err := indexr.Series(p, &labelsBuf, &chunksBuf); err != nil {
			fmt.Println("index reader series")
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		// Recording the original labels values which is needed
		// to fetch and write the postings.
		for _, l := range labelsBuf {
			valset, ok := values[l.Name]
			if !ok {
				valset = map[string]struct{}{}
				values[l.Name] = valset
			}
			valset[l.Value] = struct{}{}
		}

		if err := indexw.AddSeries(p, labelsBuf, chunksBuf...); err != nil {
			fmt.Println("index writer series")
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}

	names := []string{}
	labelValuesBuf := []string{}
	for n, v := range values {
		labelValuesBuf = labelValuesBuf[:0]
		names = append(names, n)

		for val := range v {
			labelValuesBuf = append(labelValuesBuf, val)
		}
		if err := indexw.WriteLabelIndex([]string{n}, labelValuesBuf); err != nil {
			return 1
		}
	}
	names = append(names, apkName)
	values[apkName] = map[string]struct{}{apkValue: struct{}{}}
	sort.Strings(names)

	for _, n := range names {
		labelValuesBuf = labelValuesBuf[:0]
		for v := range values[n] {
			labelValuesBuf = append(labelValuesBuf, v)
		}
		sort.Strings(labelValuesBuf)

		for _, v := range labelValuesBuf {
			posts, err := indexr.Postings1(n, v)
			if err != nil {
				return 1
			}
			arr, _ := ExpandPostings(posts)
			if _, ok := lenCount[len(arr)]; ok {
				lenCount[len(arr)] += 1
			} else {
				lenCount[len(arr)] = 1
			}
			posts, _ = indexr.Postings1(n, v)

			// if len(arr) < 512 {
				l, err := indexw.WritePostings3(n, v, posts)
				if err != nil {
					return 1
				}
				if l > uint64(len(arr) * 4 + 12) {
					if _, ok := larger1[len(arr)]; !ok {
						larger1[len(arr)] = map[int]int{}
					}
					if _, ok := larger1[len(arr)][int(l) - (len(arr) * 4 + 12)]; ok {
						larger1[len(arr)][int(l) - (len(arr) * 4 + 12)] += 1
					} else {
						larger1[len(arr)][int(l) - (len(arr) * 4 + 12)] = 1
					}
				}
			// } else {
			// 	if _, _, err := indexw.WritePostings2(n, v, posts); err != nil {
			// 		return 1
			// 	}
			// }
			// l, n, _ := indexw.WritePostings2(n, v, posts)
			// if len(arr) > 11 {
				// if l > uint64(len(arr) * 4 + 12) {
				// 	if _, ok := larger1[len(arr)]; !ok {
				// 		larger1[len(arr)] = map[int]int{}
				// 	}
				// 	if _, ok := larger1[len(arr)][int(l) - (len(arr) * 4 + 12)]; ok {
				// 		larger1[len(arr)][int(l) - (len(arr) * 4 + 12)] += 1
				// 	} else {
				// 		larger1[len(arr)][int(l) - (len(arr) * 4 + 12)] = 1
				// 	}
				// 	if _, ok := larger2[len(arr)]; !ok {
				// 		larger2[len(arr)] = map[int]int{}
				// 	}
				// 	if _, ok := larger2[len(arr)][n]; ok {
				// 		larger2[len(arr)][n] += 1
				// 	} else {
				// 		larger2[len(arr)][n] = 1
				// 	}
				// }
			// }
			// if len(arr) == 300 {
			// 	fmt.Println(n)
			// 	for _, i := range arr {
			// 		fmt.Printf("%d,", i)
			// 	}
			// 	fmt.Println()
			// }
		}
	}
	// fmt.Println(lenCount)
	// fmt.Println(count)
	fmt.Println(larger1)
	// fmt.Println()
	// fmt.Println(larger2)

	return 0
}

func TestIndexSizeComparison(t *testing.T) {
	f, err := fileutil.OpenMmapFile("../../remappedindex_corrected")
	testutil.Ok(t, err)
	toc, err := NewTOCFromByteSlice(realByteSlice(f.Bytes()))
	testutil.Ok(t, err)
	t.Log("size of postings =", toc.LabelIndicesTable-toc.Postings)
	t.Log(toc)
	f.Close()


	// ir, err := NewFileReader("../../remappedindex")
	// testutil.Ok(t, err)
	// labelNames, _ := ir.LabelNames()
	// // labelValues := make(map[string][]string)
	// // for _, name := range labelNames {
	// // 	vals, _ := ir.LabelValues(name)
	// // 	arr := make([]string, vals.Len())
	// // 	for i := 0; i < vals.Len(); i++ {
	// // 		arr[i], _ = vals.At(i)
	// // 	}
	// // 	labelValues[name] = arr
	// // }
	// // iw, err := NewWriter("../../remappedindex_r16")
	// // testutil.Ok(t, err)
	// all := []uint64{}
	// lenCount := map[int]int{}
	// t.Log("labelNames size =", len(labelNames))
	// for _, name := range labelNames {
	// 	t.Log(name)
	// 	vals, _ := ir.LabelValues(name)
	// 	for i := 0; i < vals.Len(); i++ {
	// 		v, _ := vals.At(i)
	// 		p, err := ir.Postings(name, v[0])
	// 		testutil.Ok(t, err)
	// 		count := 0
	// 		for p.Next() {
	// 			all = append(all, p.At())
	// 			count += 1
	// 		}
	// 		if _, ok := lenCount[count]; ok {
	// 			lenCount[count] += 1
	// 		} else {
	// 			lenCount[count] = 1
	// 		}
	// 		p, err = ir.Postings(name, v[0])
	// 		testutil.Ok(t, err)
	// 		// err = iw.WritePostings2(name, v[0], p)
	// 		// testutil.Ok(t, err)
	// 	}
	// }
	// sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	// t.Log(lenCount)
	// t.Log("AllPostings len =", len(all))
	// // err = iw.WritePostings2("", "", newListPostings(all...))
	// // testutil.Ok(t, err)
	// ir.Close()
	// // iw.Close()

	// f, err = fileutil.OpenMmapFile("../../remappedindex_r16")
	// testutil.Ok(t, err)
	// toc, err = NewTOCFromByteSlice(realByteSlice(f.Bytes()))
	// testutil.Ok(t, err)
	// t.Log("size of postings (r16) =", toc.LabelIndicesTable-toc.Postings)
	// f.Close()
	rewriteIndex("../../remappedindex_corrected", "../../remappedindex_corrected_1")
	f, err = fileutil.OpenMmapFile("../../remappedindex_corrected_1")
	testutil.Ok(t, err)
	toc, err = NewTOCFromByteSlice(realByteSlice(f.Bytes()))
	testutil.Ok(t, err)
	t.Log("size of postings =", toc.LabelIndicesTable-toc.Postings)
	t.Log(toc)
	f.Close()
}

func TestPersistence_index_e2e(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_persistence_e2e")
	testutil.Ok(t, err)
	defer func() {
		testutil.Ok(t, os.RemoveAll(dir))
	}()

	lbls, err := labels.ReadLabels(filepath.Join("..", "testdata", "20kseries.json"), 20000)
	testutil.Ok(t, err)

	// Sort labels as the index writer expects series in sorted order.
	sort.Sort(labels.Slice(lbls))

	symbols := map[string]struct{}{}
	for _, lset := range lbls {
		for _, l := range lset {
			symbols[l.Name] = struct{}{}
			symbols[l.Value] = struct{}{}
		}
	}

	var input indexWriterSeriesSlice

	// Generate ChunkMetas for every label set.
	for i, lset := range lbls {
		var metas []chunks.Meta

		for j := 0; j <= (i % 20); j++ {
			metas = append(metas, chunks.Meta{
				MinTime: int64(j * 10000),
				MaxTime: int64((j + 1) * 10000),
				Ref:     rand.Uint64(),
				Chunk:   chunkenc.NewXORChunk(),
			})
		}
		input = append(input, &indexWriterSeries{
			labels: lset,
			chunks: metas,
		})
	}

	iw, err := NewWriter(filepath.Join(dir, indexFilename))
	testutil.Ok(t, err)

	testutil.Ok(t, iw.AddSymbols(symbols))

	// Population procedure as done by compaction.
	var (
		postings = NewMemPostings()
		values   = map[string]map[string]struct{}{}
	)

	mi := newMockIndex()

	for i, s := range input {
		err = iw.AddSeries(uint64(i), s.labels, s.chunks...)
		testutil.Ok(t, err)
		testutil.Ok(t, mi.AddSeries(uint64(i), s.labels, s.chunks...))

		for _, l := range s.labels {
			valset, ok := values[l.Name]
			if !ok {
				valset = map[string]struct{}{}
				values[l.Name] = valset
			}
			valset[l.Value] = struct{}{}
		}
		postings.Add(uint64(i), s.labels)
	}

	for k, v := range values {
		var vals []string
		for e := range v {
			vals = append(vals, e)
		}
		sort.Strings(vals)

		testutil.Ok(t, iw.WriteLabelIndex([]string{k}, vals))
		testutil.Ok(t, mi.WriteLabelIndex([]string{k}, vals))
	}

	all := make([]uint64, len(lbls))
	for i := range all {
		all[i] = uint64(i)
	}
	err = iw.WritePostings("", "", newListPostings(all...))
	testutil.Ok(t, err)
	testutil.Ok(t, mi.WritePostings("", "", newListPostings(all...)))

	for n, e := range postings.m {
		for v := range e {
			err = iw.WritePostings(n, v, postings.Get(n, v))
			testutil.Ok(t, err)
			mi.WritePostings(n, v, postings.Get(n, v))
		}
	}

	err = iw.Close()
	testutil.Ok(t, err)

	f, err := fileutil.OpenMmapFile(filepath.Join(dir, indexFilename))
	testutil.Ok(t, err)
	toc, err := NewTOCFromByteSlice(realByteSlice(f.Bytes()))
	testutil.Ok(t, err)
	t.Log("size of postings =", toc.LabelIndicesTable-toc.Postings)

	ir, err := NewFileReader(filepath.Join(dir, indexFilename))
	testutil.Ok(t, err)

	for p := range mi.postings {
		gotp, err := ir.Postings(p.Name, p.Value)
		testutil.Ok(t, err)

		expp, err := mi.Postings(p.Name, p.Value)
		testutil.Ok(t, err)

		var lset, explset labels.Labels
		var chks, expchks []chunks.Meta

		for gotp.Next() {
			testutil.Assert(t, expp.Next() == true, "")

			ref := gotp.At()

			err := ir.Series(ref, &lset, &chks)
			testutil.Ok(t, err)

			err = mi.Series(expp.At(), &explset, &expchks)
			testutil.Ok(t, err)
			testutil.Equals(t, explset, lset)
			testutil.Equals(t, expchks, chks)
		}
		testutil.Assert(t, expp.Next() == false, "")
		testutil.Ok(t, gotp.Err())
	}

	for k, v := range mi.labelIndex {
		tplsExp, err := NewStringTuples(v, 1)
		testutil.Ok(t, err)

		tplsRes, err := ir.LabelValues(k)
		testutil.Ok(t, err)

		testutil.Equals(t, tplsExp.Len(), tplsRes.Len())
		for i := 0; i < tplsExp.Len(); i++ {
			strsExp, err := tplsExp.At(i)
			testutil.Ok(t, err)

			strsRes, err := tplsRes.At(i)
			testutil.Ok(t, err)

			testutil.Equals(t, strsExp, strsRes)
		}
	}

	gotSymbols, err := ir.Symbols()
	testutil.Ok(t, err)

	testutil.Equals(t, len(mi.symbols), len(gotSymbols))
	for s := range mi.symbols {
		_, ok := gotSymbols[s]
		testutil.Assert(t, ok, "")
	}

	testutil.Ok(t, ir.Close())
}

func TestDecbufUvariantWithInvalidBuffer(t *testing.T) {
	b := realByteSlice([]byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81})

	db := encoding.NewDecbufUvarintAt(b, 0, castagnoliTable)
	testutil.NotOk(t, db.Err())
}

func TestReaderWithInvalidBuffer(t *testing.T) {
	b := realByteSlice([]byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81})

	_, err := NewReader(b)
	testutil.NotOk(t, err)
}

// TestNewFileReaderErrorNoOpenFiles ensures that in case of an error no file remains open.
func TestNewFileReaderErrorNoOpenFiles(t *testing.T) {
	dir := testutil.NewTemporaryDirectory("block", t)

	idxName := filepath.Join(dir.Path(), "index")
	err := ioutil.WriteFile(idxName, []byte("corrupted contents"), 0644)
	testutil.Ok(t, err)

	_, err = NewFileReader(idxName)
	testutil.NotOk(t, err)

	// dir.Close will fail on Win if idxName fd is not closed on error path.
	dir.Close()
}
