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
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand"
	"sort"
	"testing"

	"github.com/prometheus/tsdb/encoding"
	"github.com/prometheus/tsdb/testutil"
)

func TestMemPostings_addFor(t *testing.T) {
	p := NewMemPostings()
	p.m[allPostingsKey.Name] = map[string][]uint64{}
	p.m[allPostingsKey.Name][allPostingsKey.Value] = []uint64{1, 2, 3, 4, 6, 7, 8}

	p.addFor(5, allPostingsKey)

	testutil.Equals(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8}, p.m[allPostingsKey.Name][allPostingsKey.Value])
}

func TestMemPostings_ensureOrder(t *testing.T) {
	p := NewUnorderedMemPostings()
	p.m["a"] = map[string][]uint64{}

	for i := 0; i < 100; i++ {
		l := make([]uint64, 100)
		for j := range l {
			l[j] = rand.Uint64()
		}
		v := fmt.Sprintf("%d", i)

		p.m["a"][v] = l
	}

	p.EnsureOrder()

	for _, e := range p.m {
		for _, l := range e {
			ok := sort.SliceIsSorted(l, func(i, j int) bool {
				return l[i] < l[j]
			})
			if !ok {
				t.Fatalf("postings list %v is not sorted", l)
			}
		}
	}
}

func TestIntersect(t *testing.T) {
	a := newListPostings(1, 2, 3)
	b := newListPostings(2, 3, 4)

	var cases = []struct {
		in []Postings

		res Postings
	}{
		{
			in:  []Postings{},
			res: EmptyPostings(),
		},
		{
			in:  []Postings{a, b, EmptyPostings()},
			res: EmptyPostings(),
		},
		{
			in:  []Postings{b, a, EmptyPostings()},
			res: EmptyPostings(),
		},
		{
			in:  []Postings{EmptyPostings(), b, a},
			res: EmptyPostings(),
		},
		{
			in:  []Postings{EmptyPostings(), a, b},
			res: EmptyPostings(),
		},
		{
			in:  []Postings{a, EmptyPostings(), b},
			res: EmptyPostings(),
		},
		{
			in:  []Postings{b, EmptyPostings(), a},
			res: EmptyPostings(),
		},
		{
			in:  []Postings{b, EmptyPostings(), a, a, b, a, a, a},
			res: EmptyPostings(),
		},
		{
			in: []Postings{
				newListPostings(1, 2, 3, 4, 5),
				newListPostings(6, 7, 8, 9, 10),
			},
			res: newListPostings(),
		},
		{
			in: []Postings{
				newListPostings(1, 2, 3, 4, 5),
				newListPostings(4, 5, 6, 7, 8),
			},
			res: newListPostings(4, 5),
		},
		{
			in: []Postings{
				newListPostings(1, 2, 3, 4, 9, 10),
				newListPostings(1, 4, 5, 6, 7, 8, 10, 11),
			},
			res: newListPostings(1, 4, 10),
		},
		{
			in: []Postings{
				newListPostings(1),
				newListPostings(0, 1),
			},
			res: newListPostings(1),
		},
		{
			in: []Postings{
				newListPostings(1),
			},
			res: newListPostings(1),
		},
		{
			in: []Postings{
				newListPostings(1),
				newListPostings(),
			},
			res: newListPostings(),
		},
		{
			in: []Postings{
				newListPostings(),
				newListPostings(),
			},
			res: newListPostings(),
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			if c.res == nil {
				t.Fatal("intersect result expectancy cannot be nil")
			}

			expected, err := ExpandPostings(c.res)
			testutil.Ok(t, err)

			i := Intersect(c.in...)

			if c.res == EmptyPostings() {
				testutil.Equals(t, EmptyPostings(), i)
				return
			}

			if i == EmptyPostings() {
				t.Fatal("intersect unexpected result: EmptyPostings sentinel")
			}

			res, err := ExpandPostings(i)
			testutil.Ok(t, err)
			testutil.Equals(t, expected, res)
		})
	}
}

func TestMultiIntersect(t *testing.T) {
	var cases = []struct {
		p   [][]uint64
		res []uint64
	}{
		{
			p: [][]uint64{
				{1, 2, 3, 4, 5, 6, 1000, 1001},
				{2, 4, 5, 6, 7, 8, 999, 1001},
				{1, 2, 5, 6, 7, 8, 1001, 1200},
			},
			res: []uint64{2, 5, 6, 1001},
		},
		// One of the reproduceable cases for:
		// https://github.com/prometheus/prometheus/issues/2616
		// The initialisation of intersectPostings was moving the iterator forward
		// prematurely making us miss some postings.
		{
			p: [][]uint64{
				{1, 2},
				{1, 2},
				{1, 2},
				{2},
			},
			res: []uint64{2},
		},
	}

	for _, c := range cases {
		ps := make([]Postings, 0, len(c.p))
		for _, postings := range c.p {
			ps = append(ps, newListPostings(postings...))
		}

		res, err := ExpandPostings(Intersect(ps...))

		testutil.Ok(t, err)
		testutil.Equals(t, c.res, res)
	}
}

func BenchmarkIntersect(t *testing.B) {
	t.Run("LongPostings1", func(bench *testing.B) {
		var a, b, c, d []uint64

		for i := 0; i < 10000000; i += 2 {
			a = append(a, uint64(i))
		}
		for i := 5000000; i < 5000100; i += 4 {
			b = append(b, uint64(i))
		}
		for i := 5090000; i < 5090600; i += 4 {
			b = append(b, uint64(i))
		}
		for i := 4990000; i < 5100000; i++ {
			c = append(c, uint64(i))
		}
		for i := 4000000; i < 6000000; i++ {
			d = append(d, uint64(i))
		}

		i1 := newListPostings(a...)
		i2 := newListPostings(b...)
		i3 := newListPostings(c...)
		i4 := newListPostings(d...)

		bench.ResetTimer()
		bench.ReportAllocs()
		for i := 0; i < bench.N; i++ {
			if _, err := ExpandPostings(Intersect(i1, i2, i3, i4)); err != nil {
				bench.Fatal(err)
			}
		}
	})

	t.Run("LongPostings2", func(bench *testing.B) {
		var a, b, c, d []uint64

		for i := 0; i < 12500000; i++ {
			a = append(a, uint64(i))
		}
		for i := 7500000; i < 12500000; i++ {
			b = append(b, uint64(i))
		}
		for i := 9000000; i < 20000000; i++ {
			c = append(c, uint64(i))
		}
		for i := 10000000; i < 12000000; i++ {
			d = append(d, uint64(i))
		}

		i1 := newListPostings(a...)
		i2 := newListPostings(b...)
		i3 := newListPostings(c...)
		i4 := newListPostings(d...)

		bench.ResetTimer()
		bench.ReportAllocs()
		for i := 0; i < bench.N; i++ {
			if _, err := ExpandPostings(Intersect(i1, i2, i3, i4)); err != nil {
				bench.Fatal(err)
			}
		}
	})

	// Many matchers(k >> n).
	t.Run("ManyPostings", func(bench *testing.B) {
		var its []Postings

		// 100000 matchers(k=100000).
		for i := 0; i < 100000; i++ {
			var temp []uint64
			for j := 1; j < 100; j++ {
				temp = append(temp, uint64(j))
			}
			its = append(its, newListPostings(temp...))
		}

		bench.ResetTimer()
		bench.ReportAllocs()
		for i := 0; i < bench.N; i++ {
			if _, err := ExpandPostings(Intersect(its...)); err != nil {
				bench.Fatal(err)
			}
		}
	})
}

func TestMultiMerge(t *testing.T) {
	i1 := newListPostings(1, 2, 3, 4, 5, 6, 1000, 1001)
	i2 := newListPostings(2, 4, 5, 6, 7, 8, 999, 1001)
	i3 := newListPostings(1, 2, 5, 6, 7, 8, 1001, 1200)

	res, err := ExpandPostings(Merge(i1, i2, i3))
	testutil.Ok(t, err)
	testutil.Equals(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 999, 1000, 1001, 1200}, res)
}

func TestMergedPostings(t *testing.T) {
	var cases = []struct {
		in []Postings

		res Postings
	}{
		{
			in:  []Postings{},
			res: EmptyPostings(),
		},
		{
			in: []Postings{
				newListPostings(),
				newListPostings(),
			},
			res: EmptyPostings(),
		},
		{
			in: []Postings{
				newListPostings(),
			},
			res: newListPostings(),
		},
		{
			in: []Postings{
				EmptyPostings(),
				EmptyPostings(),
				EmptyPostings(),
				EmptyPostings(),
			},
			res: EmptyPostings(),
		},
		{
			in: []Postings{
				newListPostings(1, 2, 3, 4, 5),
				newListPostings(6, 7, 8, 9, 10),
			},
			res: newListPostings(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
		},
		{
			in: []Postings{
				newListPostings(1, 2, 3, 4, 5),
				newListPostings(4, 5, 6, 7, 8),
			},
			res: newListPostings(1, 2, 3, 4, 5, 6, 7, 8),
		},
		{
			in: []Postings{
				newListPostings(1, 2, 3, 4, 9, 10),
				newListPostings(1, 4, 5, 6, 7, 8, 10, 11),
			},
			res: newListPostings(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
		},
		{
			in: []Postings{
				newListPostings(1, 2, 3, 4, 9, 10),
				EmptyPostings(),
				newListPostings(1, 4, 5, 6, 7, 8, 10, 11),
			},
			res: newListPostings(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),
		},
		{
			in: []Postings{
				newListPostings(1, 2),
				newListPostings(),
			},
			res: newListPostings(1, 2),
		},
		{
			in: []Postings{
				newListPostings(1, 2),
				EmptyPostings(),
			},
			res: newListPostings(1, 2),
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			if c.res == nil {
				t.Fatal("merge result expectancy cannot be nil")
			}

			expected, err := ExpandPostings(c.res)
			testutil.Ok(t, err)

			m := Merge(c.in...)

			if c.res == EmptyPostings() {
				testutil.Equals(t, EmptyPostings(), m)
				return
			}

			if m == EmptyPostings() {
				t.Fatal("merge unexpected result: EmptyPostings sentinel")
			}

			res, err := ExpandPostings(m)
			testutil.Ok(t, err)
			testutil.Equals(t, expected, res)
		})
	}
}

func TestMergedPostingsSeek(t *testing.T) {
	var cases = []struct {
		a, b []uint64

		seek    uint64
		success bool
		res     []uint64
	}{
		{
			a: []uint64{2, 3, 4, 5},
			b: []uint64{6, 7, 8, 9, 10},

			seek:    1,
			success: true,
			res:     []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			a: []uint64{1, 2, 3, 4, 5},
			b: []uint64{6, 7, 8, 9, 10},

			seek:    2,
			success: true,
			res:     []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			a: []uint64{1, 2, 3, 4, 5},
			b: []uint64{4, 5, 6, 7, 8},

			seek:    9,
			success: false,
			res:     nil,
		},
		{
			a: []uint64{1, 2, 3, 4, 9, 10},
			b: []uint64{1, 4, 5, 6, 7, 8, 10, 11},

			seek:    10,
			success: true,
			res:     []uint64{10, 11},
		},
	}

	for _, c := range cases {
		a := newListPostings(c.a...)
		b := newListPostings(c.b...)

		p := Merge(a, b)

		testutil.Equals(t, c.success, p.Seek(c.seek))

		// After Seek(), At() should be called.
		if c.success {
			start := p.At()
			lst, err := ExpandPostings(p)
			testutil.Ok(t, err)

			lst = append([]uint64{start}, lst...)
			testutil.Equals(t, c.res, lst)
		}
	}
}

func TestRemovedPostings(t *testing.T) {
	var cases = []struct {
		a, b []uint64
		res  []uint64
	}{
		{
			a:   nil,
			b:   nil,
			res: []uint64(nil),
		},
		{
			a:   []uint64{1, 2, 3, 4},
			b:   nil,
			res: []uint64{1, 2, 3, 4},
		},
		{
			a:   nil,
			b:   []uint64{1, 2, 3, 4},
			res: []uint64(nil),
		},
		{
			a:   []uint64{1, 2, 3, 4, 5},
			b:   []uint64{6, 7, 8, 9, 10},
			res: []uint64{1, 2, 3, 4, 5},
		},
		{
			a:   []uint64{1, 2, 3, 4, 5},
			b:   []uint64{4, 5, 6, 7, 8},
			res: []uint64{1, 2, 3},
		},
		{
			a:   []uint64{1, 2, 3, 4, 9, 10},
			b:   []uint64{1, 4, 5, 6, 7, 8, 10, 11},
			res: []uint64{2, 3, 9},
		},
		{
			a:   []uint64{1, 2, 3, 4, 9, 10},
			b:   []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			res: []uint64(nil),
		},
	}

	for _, c := range cases {
		a := newListPostings(c.a...)
		b := newListPostings(c.b...)

		res, err := ExpandPostings(newRemovedPostings(a, b))
		testutil.Ok(t, err)
		testutil.Equals(t, c.res, res)
	}

}

func TestRemovedNextStackoverflow(t *testing.T) {
	var full []uint64
	var remove []uint64

	var i uint64
	for i = 0; i < 1e7; i++ {
		full = append(full, i)
		remove = append(remove, i)
	}

	flp := newListPostings(full...)
	rlp := newListPostings(remove...)
	rp := newRemovedPostings(flp, rlp)
	gotElem := false
	for rp.Next() {
		gotElem = true
	}

	testutil.Ok(t, rp.Err())
	testutil.Assert(t, !gotElem, "")
}

func TestRemovedPostingsSeek(t *testing.T) {
	var cases = []struct {
		a, b []uint64

		seek    uint64
		success bool
		res     []uint64
	}{
		{
			a: []uint64{2, 3, 4, 5},
			b: []uint64{6, 7, 8, 9, 10},

			seek:    1,
			success: true,
			res:     []uint64{2, 3, 4, 5},
		},
		{
			a: []uint64{1, 2, 3, 4, 5},
			b: []uint64{6, 7, 8, 9, 10},

			seek:    2,
			success: true,
			res:     []uint64{2, 3, 4, 5},
		},
		{
			a: []uint64{1, 2, 3, 4, 5},
			b: []uint64{4, 5, 6, 7, 8},

			seek:    9,
			success: false,
			res:     nil,
		},
		{
			a: []uint64{1, 2, 3, 4, 9, 10},
			b: []uint64{1, 4, 5, 6, 7, 8, 10, 11},

			seek:    10,
			success: false,
			res:     nil,
		},
		{
			a: []uint64{1, 2, 3, 4, 9, 10},
			b: []uint64{1, 4, 5, 6, 7, 8, 11},

			seek:    4,
			success: true,
			res:     []uint64{9, 10},
		},
		{
			a: []uint64{1, 2, 3, 4, 9, 10},
			b: []uint64{1, 4, 5, 6, 7, 8, 11},

			seek:    5,
			success: true,
			res:     []uint64{9, 10},
		},
		{
			a: []uint64{1, 2, 3, 4, 9, 10},
			b: []uint64{1, 4, 5, 6, 7, 8, 11},

			seek:    10,
			success: true,
			res:     []uint64{10},
		},
	}

	for _, c := range cases {
		a := newListPostings(c.a...)
		b := newListPostings(c.b...)

		p := newRemovedPostings(a, b)

		testutil.Equals(t, c.success, p.Seek(c.seek))

		// After Seek(), At() should be called.
		if c.success {
			start := p.At()
			lst, err := ExpandPostings(p)
			testutil.Ok(t, err)

			lst = append([]uint64{start}, lst...)
			testutil.Equals(t, c.res, lst)
		}
	}
}

func TestBigEndian(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(25)) + 2
	}

	beLst := make([]byte, num*4)
	for i := 0; i < num; i++ {
		b := beLst[i*4 : i*4+4]
		binary.BigEndian.PutUint32(b, ls[i])
	}

	t.Run("Iteration", func(t *testing.T) {
		bep := newBigEndianPostings(beLst)
		for i := 0; i < num; i++ {
			testutil.Assert(t, bep.Next() == true, "")
			testutil.Equals(t, uint64(ls[i]), bep.At())
		}

		testutil.Assert(t, bep.Next() == false, "")
		testutil.Assert(t, bep.Err() == nil, "")
	})

	t.Run("Seek", func(t *testing.T) {
		table := []struct {
			seek  uint32
			val   uint32
			found bool
		}{
			{
				ls[0] - 1, ls[0], true,
			},
			{
				ls[4], ls[4], true,
			},
			{
				ls[500] - 1, ls[500], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[0], ls[601], true,
			},
			{
				ls[600], ls[601], true,
			},
			{
				ls[999], ls[999], true,
			},
			{
				ls[999] + 10, ls[999], false,
			},
		}

		bep := newBigEndianPostings(beLst)

		for _, v := range table {
			testutil.Equals(t, v.found, bep.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), bep.At())
			testutil.Assert(t, bep.Err() == nil, "")
		}
	})
}

func TestBaseDeltaPostings(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(25)) + 2
	}

	width := bits.Len32(ls[len(ls)-1] - ls[0])
	buf := encoding.Encbuf{}
	for i := 0; i < num; i++ {
		buf.PutBits(uint64(ls[i]-ls[0]), width)
	}
	// t.Log("(baseDeltaPostings) len of 1000 number = ", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		bdp := newBaseDeltaPostings(buf.Get(), ls[0], width, len(ls))
		for i := 0; i < num; i++ {
			testutil.Assert(t, bdp.Next() == true, "")
			testutil.Equals(t, uint64(ls[i]), bdp.At())
		}

		testutil.Assert(t, bdp.Next() == false, "")
		testutil.Assert(t, bdp.Err() == nil, "")
	})

	t.Run("Seek", func(t *testing.T) {
		table := []struct {
			seek  uint32
			val   uint32
			found bool
		}{
			{
				ls[0] - 1, ls[0], true,
			},
			{
				ls[4], ls[4], true,
			},
			{
				ls[500] - 1, ls[500], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[0], ls[601], true,
			},
			{
				ls[600], ls[601], true,
			},
			{
				ls[999], ls[999], true,
			},
			{
				ls[999] + 10, ls[999], false,
			},
		}

		bdp := newBaseDeltaPostings(buf.Get(), ls[0], width, len(ls))

		for _, v := range table {
			testutil.Equals(t, v.found, bdp.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), bdp.At())
			testutil.Assert(t, bdp.Err() == nil, "")
		}
	})
}

func TestDeltaBlockPostings(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(25)) + 2
	}

	buf := encoding.Encbuf{}
	writeDeltaBlockPostings(&buf, ls)
	// t.Log("(deltaBlockPostings) len of 1000 number = ", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		dbp := newDeltaBlockPostings(buf.Get(), len(ls))
		for i := 0; i < num; i++ {
			testutil.Assert(t, dbp.Next() == true, "")
			if uint64(ls[i]) != dbp.At() {
				t.Log(i, dbp.GetOff(), "width=", dbp.GetWidth())
			}
			testutil.Equals(t, uint64(ls[i]), dbp.At())
		}

		testutil.Assert(t, dbp.Next() == false, "")
		testutil.Assert(t, dbp.Err() == nil, "")
	})

	t.Run("Seek", func(t *testing.T) {
		table := []struct {
			seek  uint32
			val   uint32
			found bool
		}{
			{
				ls[0] - 1, ls[0], true,
			},
			{
				ls[4], ls[4], true,
			},
			{
				ls[500] - 1, ls[500], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[0], ls[601], true,
			},
			{
				ls[600], ls[601], true,
			},
			{
				ls[999], ls[999], true,
			},
			{
				ls[999] + 10, ls[999], false,
			},
		}

		dbp := newDeltaBlockPostings(buf.Get(), len(ls))

		for _, v := range table {
			testutil.Equals(t, v.found, dbp.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), dbp.At())
			testutil.Assert(t, dbp.Err() == nil, "")
		}
	})
}

func TestBaseDeltaBlockPostings(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(25)) + 2
	}

	buf := encoding.Encbuf{}
	writeBaseDeltaBlockPostings(&buf, ls)
	// t.Log("(deltaBlockPostings) len of 1000 number = ", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		dbp := newBaseDeltaBlockPostings(buf.Get(), len(ls))
		for i := 0; i < num; i++ {
			testutil.Assert(t, dbp.Next() == true, "")
			if uint64(ls[i]) != dbp.At() {
				t.Log(i, dbp.GetOff(), "width=", dbp.GetWidth())
			}
			testutil.Equals(t, uint64(ls[i]), dbp.At())
		}

		testutil.Assert(t, dbp.Next() == false, "")
		testutil.Assert(t, dbp.Err() == nil, "")
	})

	t.Run("Seek", func(t *testing.T) {
		table := []struct {
			seek  uint32
			val   uint32
			found bool
		}{
			{
				ls[0] - 1, ls[0], true,
			},
			{
				ls[4], ls[4], true,
			},
			{
				ls[500] - 1, ls[500], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[0], ls[601], true,
			},
			{
				ls[600], ls[601], true,
			},
			{
				ls[999], ls[999], true,
			},
			{
				ls[999] + 10, ls[999], false,
			},
		}

		dbp := newBaseDeltaBlockPostings(buf.Get(), len(ls))

		for _, v := range table {
			testutil.Equals(t, v.found, dbp.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), dbp.At())
			testutil.Assert(t, dbp.Err() == nil, "")
		}
	})
}

func TestBitmapPostings(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(25)) + 2
		// ls[i] = ls[i-1] + 2
	}

	buf := encoding.Encbuf{}
	writeBitmapPostings(&buf, ls)
	// t.Log("len", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		bp := newBitmapPostings(buf.Get())
		for i := 0; i < num; i++ {
			testutil.Assert(t, bp.Next() == true, "")
			// t.Log("ls[i] =", ls[i], "bp.At() =", bp.At())
			testutil.Equals(t, uint64(ls[i]), bp.At())
		}

		testutil.Assert(t, bp.Next() == false, "")
		testutil.Assert(t, bp.Err() == nil, "")
	})

	t.Run("Seek", func(t *testing.T) {
		table := []struct {
			seek  uint32
			val   uint32
			found bool
		}{
			{
				ls[0] - 1, ls[0], true,
			},
			{
				ls[4], ls[4], true,
			},
			{
				ls[500] - 1, ls[500], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[0], ls[601], true,
			},
			{
				ls[600], ls[601], true,
			},
			{
				ls[999], ls[999], true,
			},
			{
				ls[999] + 10, ls[999], false,
			},
		}

		bp := newBitmapPostings(buf.Get())

		for _, v := range table {
			testutil.Equals(t, v.found, bp.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), bp.At())
			testutil.Assert(t, bp.Err() == nil, "")
		}
	})
}

func TestRoaringBitmapPostings(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(15)) + 2
		// ls[i] = ls[i-1] + 10
	}

	buf := encoding.Encbuf{}
	writeRoaringBitmapPostings(&buf, ls)
	// t.Log("len", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		rbp := newRoaringBitmapPostings(buf.Get())
		for i := 0; i < num; i++ {
			testutil.Assert(t, rbp.Next() == true, "")
			// t.Log("ls[i] =", ls[i], "rbp.At() =", rbp.At())
			testutil.Equals(t, uint64(ls[i]), rbp.At())
		}

		testutil.Assert(t, rbp.Next() == false, "")
		testutil.Assert(t, rbp.Err() == nil, "")
	})

	t.Run("Seek", func(t *testing.T) {
		table := []struct {
			seek  uint32
			val   uint32
			found bool
		}{
			{
				ls[0] - 1, ls[0], true,
			},
			{
				ls[4], ls[4], true,
			},
			{
				ls[500] - 1, ls[500], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[600] + 1, ls[601], true,
			},
			{
				ls[0], ls[601], true,
			},
			{
				ls[600], ls[601], true,
			},
			{
				ls[999], ls[999], true,
			},
			{
				ls[999] + 10, ls[999], false,
			},
		}

		rbp := newRoaringBitmapPostings(buf.Get())

		for _, v := range table {
			// t.Log("i", i)
			testutil.Equals(t, v.found, rbp.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), rbp.At())
			testutil.Assert(t, rbp.Err() == nil, "")
		}
	})
}

func BenchmarkPostings(b *testing.B) {
	num := 100000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(25)) + 2
	}

	// bigEndianPostings.
	bufBE := make([]byte, num*4)
	for i := 0; i < num; i++ {
		b := bufBE[i*4 : i*4+4]
		binary.BigEndian.PutUint32(b, ls[i])
	}
	b.Log("bigEndianPostings size =", len(bufBE))

	// baseDeltaPostings.
	width := bits.Len32(ls[len(ls)-1] - ls[0])
	bufBD := encoding.Encbuf{}
	for i := 0; i < num; i++ {
		bufBD.PutBits(uint64(ls[i]-ls[0]), width)
	}
	b.Log("baseDeltaPostings size =", len(bufBD.Get()))

	// deltaBlockPostings.
	bufDB := encoding.Encbuf{}
	writeDeltaBlockPostings(&bufDB, ls)
	b.Log("deltaBlockPostings size =", len(bufDB.Get()))

	// baseDeltaBlockPostings.
	bufBDB := encoding.Encbuf{}
	writeBaseDeltaBlockPostings(&bufBDB, ls)
	b.Log("baseDeltaBlockPostings size =", len(bufBDB.Get()))

	// bitmapPostings.
	bufBM := encoding.Encbuf{}
	writeBitmapPostings(&bufBM, ls)
	b.Log("bitmapPostings bits", bitmapBits, "size =", len(bufBM.Get()))

	// roaringBitmapPostings.
	bufRBM := encoding.Encbuf{}
	writeRoaringBitmapPostings(&bufRBM, ls)
	b.Log("roaringBitmapPostings bits", bitmapBits, "size =", len(bufRBM.Get()))

	table := []struct {
		seek  uint32
		val   uint32
		found bool
	}{
		{
			ls[0] - 1, ls[0], true,
		},
		{
			ls[4], ls[4], true,
		},
		{
			ls[5000] - 1, ls[5000], true,
		},
		{
			ls[6000] + 1, ls[6001], true,
		},
		{
			ls[6000] + 1, ls[6001], true,
		},
		{
			ls[6000] + 1, ls[6001], true,
		},
		{
			ls[0], ls[6001], true,
		},
		{
			ls[6000], ls[6001], true,
		},
		{
			ls[99999], ls[99999], true,
		},
		{
			ls[99999] + 10, ls[99999], false,
		},
	}

	b.Run("bigEndianIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			bep := newBigEndianPostings(bufBE)

			for i := 0; i < num; i++ {
				testutil.Assert(bench, bep.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), bep.At())
			}
			testutil.Assert(bench, bep.Next() == false, "")
			testutil.Assert(bench, bep.Err() == nil, "")
		}
	})
	b.Run("baseDeltaIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			bdp := newBaseDeltaPostings(bufBD.Get(), ls[0], width, len(ls))

			for i := 0; i < num; i++ {
				testutil.Assert(bench, bdp.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), bdp.At())
			}
			testutil.Assert(bench, bdp.Next() == false, "")
			testutil.Assert(bench, bdp.Err() == nil, "")
		}
	})
	b.Run("deltaBlockIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			dbp := newDeltaBlockPostings(bufDB.Get(), len(ls))

			for i := 0; i < num; i++ {
				testutil.Assert(bench, dbp.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), dbp.At())
			}
			testutil.Assert(bench, dbp.Next() == false, "")
			testutil.Assert(bench, dbp.Err() == nil, "")
		}
	})
	b.Run("baseDeltaBlockIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			bdbp := newBaseDeltaBlockPostings(bufBDB.Get(), len(ls))

			for i := 0; i < num; i++ {
				testutil.Assert(bench, bdbp.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), bdbp.At())
			}
			testutil.Assert(bench, bdbp.Next() == false, "")
			testutil.Assert(bench, bdbp.Err() == nil, "")
		}
	})
	b.Run("bitmapPostingsIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			bm := newBitmapPostings(bufBM.Get())

			for i := 0; i < num; i++ {
				testutil.Assert(bench, bm.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), bm.At())
			}
			testutil.Assert(bench, bm.Next() == false, "")
			testutil.Assert(bench, bm.Err() == nil, "")
		}
	})
	b.Run("roaringBitmapPostingsIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			rbm := newRoaringBitmapPostings(bufRBM.Get())

			for i := 0; i < num; i++ {
				testutil.Assert(bench, rbm.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), rbm.At())
			}
			testutil.Assert(bench, rbm.Next() == false, "")
			testutil.Assert(bench, rbm.Err() == nil, "")
		}
	})

	b.Run("bigEndianSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			bep := newBigEndianPostings(bufBE)

			for _, v := range table {
				testutil.Equals(bench, v.found, bep.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), bep.At())
				testutil.Assert(bench, bep.Err() == nil, "")
			}
		}
	})
	b.Run("baseDeltaSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			bdp := newBaseDeltaPostings(bufBD.Get(), ls[0], width, len(ls))

			for _, v := range table {
				testutil.Equals(bench, v.found, bdp.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), bdp.At())
				testutil.Assert(bench, bdp.Err() == nil, "")
			}
		}
	})
	b.Run("deltaBlockSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			dbp := newDeltaBlockPostings(bufDB.Get(), len(ls))

			for _, v := range table {
				testutil.Equals(bench, v.found, dbp.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), dbp.At())
				testutil.Assert(bench, dbp.Err() == nil, "")
			}
		}
	})
	b.Run("baseDeltaBlockSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			bdbp := newBaseDeltaBlockPostings(bufBDB.Get(), len(ls))

			for _, v := range table {
				testutil.Equals(bench, v.found, bdbp.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), bdbp.At())
				testutil.Assert(bench, bdbp.Err() == nil, "")
			}
		}
	})
	b.Run("bitmapPostingsSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			bm := newBitmapPostings(bufBM.Get())

			for _, v := range table {
				testutil.Equals(bench, v.found, bm.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), bm.At())
				testutil.Assert(bench, bm.Err() == nil, "")
			}
		}
	})
	b.Run("roaringBitmapPostingsSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			rbm := newRoaringBitmapPostings(bufRBM.Get())

			for _, v := range table {
				testutil.Equals(bench, v.found, rbm.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), rbm.At())
				testutil.Assert(bench, rbm.Err() == nil, "")
			}
		}
	})
}

func TestIntersectWithMerge(t *testing.T) {
	// One of the reproducible cases for:
	// https://github.com/prometheus/prometheus/issues/2616
	a := newListPostings(21, 22, 23, 24, 25, 30)

	b := Merge(
		newListPostings(10, 20, 30),
		newListPostings(15, 26, 30),
	)

	p := Intersect(a, b)
	res, err := ExpandPostings(p)

	testutil.Ok(t, err)
	testutil.Equals(t, []uint64{30}, res)
}

func TestWithoutPostings(t *testing.T) {
	var cases = []struct {
		base Postings
		drop Postings

		res Postings
	}{
		{
			base: EmptyPostings(),
			drop: EmptyPostings(),

			res: EmptyPostings(),
		},
		{
			base: EmptyPostings(),
			drop: newListPostings(1, 2),

			res: EmptyPostings(),
		},
		{
			base: newListPostings(1, 2),
			drop: EmptyPostings(),

			res: newListPostings(1, 2),
		},
		{
			base: newListPostings(),
			drop: newListPostings(),

			res: newListPostings(),
		},
		{
			base: newListPostings(1, 2, 3),
			drop: newListPostings(),

			res: newListPostings(1, 2, 3),
		},
		{
			base: newListPostings(1, 2, 3),
			drop: newListPostings(4, 5, 6),

			res: newListPostings(1, 2, 3),
		},
		{
			base: newListPostings(1, 2, 3),
			drop: newListPostings(3, 4, 5),

			res: newListPostings(1, 2),
		},
	}

	for _, c := range cases {
		t.Run("", func(t *testing.T) {
			if c.res == nil {
				t.Fatal("without result expectancy cannot be nil")
			}

			expected, err := ExpandPostings(c.res)
			testutil.Ok(t, err)

			w := Without(c.base, c.drop)

			if c.res == EmptyPostings() {
				testutil.Equals(t, EmptyPostings(), w)
				return
			}

			if w == EmptyPostings() {
				t.Fatal("without unexpected result: EmptyPostings sentinel")
			}

			res, err := ExpandPostings(w)
			testutil.Ok(t, err)
			testutil.Equals(t, expected, res)
		})
	}
}
