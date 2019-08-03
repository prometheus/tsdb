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
	"bufio"
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand"
	"os"
	"sort"
	"strconv"
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

	width := (bits.Len32(ls[len(ls)-1] - ls[0]) + 7) >> 3
	buf := encoding.Encbuf{}
	for i := 0; i < 8 - width; i ++ {
		buf.B = append(buf.B, 0)
	}
	for i := 0; i < num; i++ {
		for j := width - 1; j >= 0; j-- {
			buf.B = append(buf.B, byte(((ls[i]-ls[0])>>(8*uint(j))&0xff)))
		}
	}
	// t.Log("(baseDeltaPostings) len of 1000 number = ", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		bdp := newBaseDeltaPostings(buf.Get(), uint64(ls[0]), width, len(ls))
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

		bdp := newBaseDeltaPostings(buf.Get(), uint64(ls[0]), width, len(ls))

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
		dbp := newBaseDeltaBlockPostings(buf.Get())
		for i := 0; i < num; i++ {
			testutil.Assert(t, dbp.Next() == true, "")
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

		dbp := newBaseDeltaBlockPostings(buf.Get())

		for _, v := range table {
			// fmt.Println(i)
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
			if uint64(ls[i]) != rbp.At() {
				t.Log("ls[i] =", ls[i], "rbp.At() =", rbp.At(), " i =", i)
			}
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

func TestBaseDeltaBlock8Postings(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(15)) + 2
		// ls[i] = ls[i-1] + 10
	}

	buf := encoding.Encbuf{}
	writeBaseDeltaBlock8Postings(&buf, ls)
	// t.Log("len", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		rbp := newBaseDeltaBlock8Postings(buf.Get())
		for i := 0; i < num; i++ {
			testutil.Assert(t, rbp.Next() == true, "")
			if uint64(ls[i]) != rbp.At() {
				t.Log("ls[i] =", ls[i], "rbp.At() =", rbp.At(), " i =", i)
			}
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

		rbp := newBaseDeltaBlock8Postings(buf.Get())

		for _, v := range table {
			// t.Log("i", i)
			testutil.Equals(t, v.found, rbp.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), rbp.At())
			testutil.Assert(t, rbp.Err() == nil, "")
		}
	})
}

func TestBaseDeltaBlock16Postings(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(15)) + 2
		// ls[i] = ls[i-1] + 10
	}

	buf := encoding.Encbuf{}
	writeBaseDeltaBlock16Postings(&buf, ls)
	// t.Log("len", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		rbp := newBaseDeltaBlock16Postings(buf.Get())
		for i := 0; i < num; i++ {
			testutil.Assert(t, rbp.Next() == true, "")
			if uint64(ls[i]) != rbp.At() {
				t.Log("ls[i] =", ls[i], "rbp.At() =", rbp.At(), " i =", i)
			}
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

		rbp := newBaseDeltaBlock16Postings(buf.Get())

		for _, v := range table {
			// t.Log("i", i)
			testutil.Equals(t, v.found, rbp.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), rbp.At())
			testutil.Assert(t, rbp.Err() == nil, "")
		}
	})
}

func TestBaseDeltaBlock16PostingsV2(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint32, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint32(rand.Int31n(15)) + 2
		// ls[i] = ls[i-1] + 10
	}

	buf := encoding.Encbuf{}
	writeBaseDeltaBlock16PostingsV2(&buf, ls)
	// t.Log("len", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		rbp := newBaseDeltaBlock16PostingsV2(buf.Get())
		for i := 0; i < num; i++ {
			testutil.Assert(t, rbp.Next() == true, "")
			if uint64(ls[i]) != rbp.At() {
				t.Log("ls[i] =", ls[i], "rbp.At() =", rbp.At(), " i =", i)
			}
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

		rbp := newBaseDeltaBlock16PostingsV2(buf.Get())

		for _, v := range table {
			// t.Log("i", i)
			testutil.Equals(t, v.found, rbp.Seek(uint64(v.seek)))
			testutil.Equals(t, uint64(v.val), rbp.At())
			testutil.Assert(t, rbp.Err() == nil, "")
		}
	})
}

func TestRoaringBitmapPostings64(t *testing.T) {
	num := 1000
	// mock a list as postings
	ls := make([]uint64, num)
	ls[0] = 2
	for i := 1; i < num; i++ {
		ls[i] = ls[i-1] + uint64(rand.Int63n(15)) + 2
		// ls[i] = ls[i-1] + 10
	}

	buf := encoding.Encbuf{}
	writeRoaringBitmapPostings64(&buf, ls)
	// t.Log("len", len(buf.Get()))

	t.Run("Iteration", func(t *testing.T) {
		rbp := newRoaringBitmapPostings(buf.Get())
		for i := 0; i < num; i++ {
			testutil.Assert(t, rbp.Next() == true, "")
			// t.Log("ls[i] =", ls[i], "rbp.At() =", rbp.At())
			testutil.Equals(t, ls[i], rbp.At())
		}

		testutil.Assert(t, rbp.Next() == false, "")
		testutil.Assert(t, rbp.Err() == nil, "")
	})

	t.Run("Seek", func(t *testing.T) {
		table := []struct {
			seek  uint64
			val   uint64
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
			testutil.Equals(t, v.found, rbp.Seek(v.seek))
			testutil.Equals(t, v.val, rbp.At())
			testutil.Assert(t, rbp.Err() == nil, "")
		}
	})
}

func BenchmarkRandomPostings(b *testing.B) {
	num := 100000
	ls := make([]uint32, num)
	existedNum := make(map[uint32]struct{})
	for i := 0; i < num; i++ {
		for {
			x := uint32(rand.Int31n(1000000))
			if _, ok := existedNum[x]; !ok {
				ls[i] = x
				existedNum[x] = struct{}{}
				break
			}
		}
	}
	sort.Sort(uint32slice(ls))

	// bigEndianPostings.
	bufBE := make([]byte, num*4)
	for i := 0; i < num; i++ {
		b := bufBE[i*4 : i*4+4]
		binary.BigEndian.PutUint32(b, ls[i])
	}
	b.Log("bigEndianPostings size =", len(bufBE))

	bufBDB16 := encoding.Encbuf{}
	temp := make([]uint64, 0, len(ls))
	for _, x := range ls {
		temp = append(temp, uint64(x))
	}
	writeBaseDeltaBlock16Postings64(&bufBDB16, temp)
	b.Log("baseDeltaBlock16Postings (64bit)", len(bufBDB16.Get()))

	table := []struct {
		seek  uint32
		val   uint32
		found bool
	}{
		{
			ls[0] + 1, ls[1], true,
		},
		{
			ls[1000], ls[1000], true,
		},
		{
			ls[1001], ls[1001], true,
		},
		{
			ls[2000]+1, ls[2001], true,
		},
		{
			ls[3000], ls[3000], true,
		},
		{
			ls[3001], ls[3001], true,
		},
		{
			ls[4000]+1, ls[4001], true,
		},
		{
			ls[5000], ls[5000], true,
		},
		{
			ls[5001], ls[5001], true,
		},
		{
			ls[6000]+1, ls[6001], true,
		},
		{
			ls[10000], ls[10000], true,
		},
		{
			ls[10001], ls[10001], true,
		},
		{
			ls[20000]+1, ls[20001], true,
		},
		{
			ls[30000], ls[30000], true,
		},
		{
			ls[30001], ls[30001], true,
		},
		{
			ls[40000]+1, ls[40001], true,
		},
		{
			ls[50000], ls[50000], true,
		},
		{
			ls[50001], ls[50001], true,
		},
		{
			ls[60000]+1, ls[60001], true,
		},
		{
			ls[70000], ls[70000], true,
		},
		{
			ls[70001], ls[70001], true,
		},
		{
			ls[80000]+1, ls[80001], true,
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
			// bench.StopTimer()
			bep := newBigEndianPostings(bufBE)
			// bench.StartTimer()

			for i := 0; i < num; i++ {
				testutil.Assert(bench, bep.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), bep.At())
			}
			testutil.Assert(bench, bep.Next() == false, "")
			testutil.Assert(bench, bep.Err() == nil, "")
		}
	})
	b.Run("baseDeltaBlock16PostingsIteration (64bit)", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			rbm := newBaseDeltaBlock16Postings(bufBDB16.Get())
			// bench.StartTimer()

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
			// bench.StopTimer()
			bep := newBigEndianPostings(bufBE)
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, bep.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), bep.At())
				testutil.Assert(bench, bep.Err() == nil, "")
			}
		}
	})
	b.Run("baseDeltaBlock16PostingsSeek (64bit)", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			rbm := newBaseDeltaBlock16Postings(bufBDB16.Get())
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, rbm.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), rbm.At())
				testutil.Assert(bench, rbm.Err() == nil, "")
			}
		}
	})
}

func BenchmarkRealPostings(b *testing.B) {
	file, err := os.Open("../../realWorldPostings.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	var ls []uint32
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		x, err := strconv.Atoi(scanner.Text())
		if err != nil {
			panic(err)
		}
		ls = append(ls, uint32(x))
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// bigEndianPostings.
	bufBE := make([]byte, len(ls)*4)
	for i := 0; i < len(ls); i++ {
		b := bufBE[i*4 : i*4+4]
		binary.BigEndian.PutUint32(b, ls[i])
	}
	b.Log("bigEndianPostings size =", len(bufBE))

	width := (bits.Len32(ls[len(ls)-1] - ls[0]) + 7) >> 3
	bufBD := encoding.Encbuf{}
	for i := 0; i < 8 - width; i ++ {
		bufBD.B = append(bufBD.B, 0)
	}
	for i := 0; i < len(ls); i++ {
		for j := width - 1; j >= 0; j-- {
			bufBD.B = append(bufBD.B, byte(((ls[i]-ls[0])>>(8*uint(j))&0xff)))
		}
		// bufBD.PutBits(uint64(ls[i]-ls[0]), width)
	}
	b.Log("baseDeltaPostings size =", len(bufBD.Get()))

	bufBDB16 := encoding.Encbuf{}
	temp := make([]uint64, 0, len(ls))
	for _, x := range ls {
		temp = append(temp, uint64(x))
	}
	writeBaseDeltaBlock16Postings64(&bufBDB16, temp)
	b.Log("baseDeltaBlock16Postings (64bit)", len(bufBDB16.Get()))

	table := []struct {
		seek  uint32
		val   uint32
		found bool
	}{
		{
			ls[0] + 1, ls[1], true,
		},
		{
			ls[1000], ls[1000], true,
		},
		{
			ls[1001], ls[1001], true,
		},
		{
			ls[2000]+1, ls[2001], true,
		},
		{
			ls[3000], ls[3000], true,
		},
		{
			ls[3001], ls[3001], true,
		},
		{
			ls[4000]+1, ls[4001], true,
		},
		{
			ls[5000], ls[5000], true,
		},
		{
			ls[5001], ls[5001], true,
		},
		{
			ls[6000]+1, ls[6001], true,
		},
		{
			ls[10000], ls[10000], true,
		},
		{
			ls[10001], ls[10001], true,
		},
		{
			ls[20000]+1, ls[20001], true,
		},
		{
			ls[30000], ls[30000], true,
		},
		{
			ls[30001], ls[30001], true,
		},
		{
			ls[40000]+1, ls[40001], true,
		},
		{
			ls[50000], ls[50000], true,
		},
		{
			ls[50001], ls[50001], true,
		},
		{
			ls[60000]+1, ls[60001], true,
		},
		{
			ls[70000], ls[70000], true,
		},
		{
			ls[70001], ls[70001], true,
		},
		{
			ls[80000]+1, ls[80001], true,
		},
		{
			ls[100000], ls[100000], true,
		},
		{
			ls[150000]+1, ls[150001], true,
		},
		{
			ls[200000], ls[200000], true,
		},
		{
			ls[250000]+1, ls[250001], true,
		},
		{
			ls[300000], ls[300000], true,
		},
	}
	b.Run("bigEndianIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			bep := newBigEndianPostings(bufBE)
			// bench.StartTimer()

			for i := 0; i < len(ls); i++ {
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
			// bench.StopTimer()
			bdp := newBaseDeltaPostings(bufBD.Get(), uint64(ls[0]), width, len(ls))
			// bench.StartTimer()

			for i := 0; i < len(ls); i++ {
				testutil.Assert(bench, bdp.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), bdp.At())
			}
			testutil.Assert(bench, bdp.Next() == false, "")
			testutil.Assert(bench, bdp.Err() == nil, "")
		}
	})
	b.Run("baseDeltaBlock16PostingsIteration (64bit)", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			rbm := newBaseDeltaBlock16Postings(bufBDB16.Get())
			// bench.StartTimer()

			for i := 0; i < len(ls); i++ {
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
			// bench.StopTimer()
			bep := newBigEndianPostings(bufBE)
			// bench.StartTimer()

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
			// bench.StopTimer()
			bdp := newBaseDeltaPostings(bufBD.Get(), uint64(ls[0]), width, len(ls))
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, bdp.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), bdp.At())
				testutil.Assert(bench, bdp.Err() == nil, "")
			}
		}
	})
	b.Run("baseDeltaBlock16PostingsSeek (64bit)", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			rbm := newBaseDeltaBlock16Postings(bufBDB16.Get())
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, rbm.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), rbm.At())
				testutil.Assert(bench, rbm.Err() == nil, "")
			}
		}
	})
}

func BenchmarkRealShortPostings(b *testing.B) {
	ls := []uint64{12825376,12825699,12826041,12826364,12826706,12826880,12827211,12827553,12827885,12828225,12828529,12828852,12829194,12829555,12829878,12830239,12830581,12830904,12831265,12831569,12831892,12832234,12832557,12832937,12833351,12833672,12834014,12834299,12834641,12834983,12835306,12835648,12835971,12836313,12836655,12837006,12837346,12837650,12838011,12838334,12838695,12839009,12839330,12839653,12839995,12840308,12840629,12840971,12841313,12841655,12845998,12846017,12846036,12846967,12846986,12847005,12847993,12848012,12848031,12848962,12848981,12849000,12849988,12850007,12850026,12850510,12850519,12850528,12851503,12851522,12851541,12852529,12852548,12852567,12853555,12853574,12853593,12854581,12854600,12854619,12855493,12855512,12855531,12856462,12856481,12856500,12857488,12857507,12857526,12858571,12858590,12858609,12859540,12859559,12859578,12860623,12860642,12860661,12861649,12861668,12861687,12862618,12862637,12862656,12863701,12863720,12863739,12864613,12864632,12864651,12865582,12865601,12865620,12866608,12866627,12866646,12867577,12867596,12867615,12868717,12868736,12868755,12869980,12869999,12870018,12870949,12870968,12870987,12871975,12871994,12872013,12872830,12872849,12872868,12873856,12873875,12873894,12874882,12874901,12874920,12875851,12875870,12875889,12876877,12876896,12876915,12877846,12877865,12877884,12878872,12878891,12878910,12879898,12879917,12879936,12880981,12881000,12881019,12882007,12882026,12882045,12882919,12882938,12882957,12884002,12884021,12884040,12884971,12884990,12885009,12886054,12886073,12886092,12887023,12887042,12887061,12887992,12888011,12888030,12888961,12888980,12888999,12889987,12890006,12890025,12890929,12890947,12890965,12891892,12891911,12891930,12892918,12892937,12892956,12893944,12893963,12893982,12894970,12894989,12895008,12895445,12895768,12896110,12896433,12896775,12896949,12897280,12897622,12897954,12898294,12898598,12898921,12899263,12899624,12899947,12900308,12900650,12900973,12901334,12901638,12901961,12902303,12902626,12903006,12903420,12903741,12904083,12904368,12904710,12905052,12905375,12905717,12906040,12906382,12906724,12907075,12907415,12907719,12908080,12908403,12908764,12909078,12909399,12909722,12910064,12910377,12910698,12911040,12911382,12911724,12912085,12912408,12912750,12913073,12913415,12913589,12913920,12914262,12914594,12914934,12915238,12915561,12915903,12916264,12916587,12916948,12917290,12917613,12917974,12918278,12918601,12918943,12919266,12919646,12920060,12920381,12920723,12921008,12921350,12921692,12922015,12922357,12922680,12923022,12923364,12923715,12924055,12924359,12924720,12925043,12925404,12925718,12926039,12926362,12926704,12927018,12927339,12927681,12928023,12928365}

	bufBE := make([]byte, len(ls)*4)
	for i := 0; i < len(ls); i++ {
		b := bufBE[i*4 : i*4+4]
		binary.BigEndian.PutUint32(b, uint32(ls[i]))
	}
	b.Log("bigEndianPostings size =", len(bufBE))

	width := (bits.Len64(ls[len(ls)-1] - ls[0]) + 7) >> 3
	bufBD := encoding.Encbuf{}
	for i := 0; i < 8 - width; i ++ {
		bufBD.B = append(bufBD.B, 0)
	}
	for i := 0; i < len(ls); i++ {
		for j := width - 1; j >= 0; j-- {
			bufBD.B = append(bufBD.B, byte(((ls[i]-ls[0])>>(8*uint(j))&0xff)))
		}
		// bufBD.PutBits(uint64(ls[i]-ls[0]), width)
	}
	b.Log("baseDeltaPostings size =", len(bufBD.Get()))

	bufBDB16 := encoding.Encbuf{}
	temp := make([]uint64, 0, len(ls))
	for _, x := range ls {
		temp = append(temp, uint64(x))
	}
	writeBaseDeltaBlock16Postings64(&bufBDB16, temp)
	b.Log("baseDeltaBlock16Postings (64bit)", len(bufBDB16.Get()))

	table := []struct {
		seek  uint64
		val   uint64
		found bool
	}{
		{
			ls[0], ls[0], true,
		},
		{
			ls[5], ls[5], true,
		},
		{
			ls[10], ls[10], true,	
		},
		{
			ls[15], ls[15], true,
		},
		{
			ls[20], ls[20], true,
		},
		{
			ls[25], ls[25], true,
		},
		{
			ls[30], ls[30], true,	
		},
		{
			ls[35], ls[35], true,
		},
		{
			ls[40], ls[40], true,
		},
		{
			ls[45], ls[45], true,
		},
		{
			ls[50], ls[50], true,	
		},
		{
			ls[55], ls[55], true,
		},
		{
			ls[60], ls[60], true,
		},
		{
			ls[65], ls[65], true,
		},
		{
			ls[70], ls[70], true,	
		},
		{
			ls[75], ls[75], true,
		},
		{
			ls[80], ls[80], true,
		},
		{
			ls[85], ls[85], true,
		},
		{
			ls[90], ls[90], true,	
		},
		{
			ls[95], ls[95], true,
		},
		{
			ls[100], ls[100], true,
		},
		{
			ls[105], ls[105], true,
		},
		{
			ls[110], ls[110], true,	
		},
		{
			ls[115], ls[115], true,
		},
		{
			ls[120], ls[120], true,
		},
		{
			ls[125], ls[125], true,
		},
		{
			ls[130], ls[130], true,	
		},
		{
			ls[135], ls[135], true,
		},
		{
			ls[140], ls[140], true,
		},
		{
			ls[145], ls[145], true,
		},
		{
			ls[150], ls[150], true,	
		},
		{
			ls[155], ls[155], true,
		},
		{
			ls[160], ls[160], true,
		},
		{
			ls[165], ls[165], true,
		},
		{
			ls[170], ls[170], true,	
		},
		{
			ls[175], ls[175], true,
		},
		{
			ls[180], ls[180], true,
		},
		{
			ls[185], ls[185], true,
		},
		{
			ls[190], ls[190], true,	
		},
		{
			ls[195], ls[195], true,
		},
		{
			ls[200], ls[200], true,
		},
		{
			ls[205], ls[205], true,
		},
		{
			ls[210], ls[210], true,	
		},
	}
	b.Run("bigEndianIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			bep := newBigEndianPostings(bufBE)
			// bench.StartTimer()

			for i := 0; i < len(ls); i++ {
				testutil.Assert(bench, bep.Next() == true, "")
				testutil.Equals(bench, ls[i], bep.At())
			}
			testutil.Assert(bench, bep.Next() == false, "")
			testutil.Assert(bench, bep.Err() == nil, "")
		}
	})
	b.Run("baseDeltaIteration", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			bdp := newBaseDeltaPostings(bufBD.Get(), uint64(ls[0]), width, len(ls))
			// bench.StartTimer()

			for i := 0; i < len(ls); i++ {
				testutil.Assert(bench, bdp.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), bdp.At())
			}
			testutil.Assert(bench, bdp.Next() == false, "")
			testutil.Assert(bench, bdp.Err() == nil, "")
		}
	})
	b.Run("baseDeltaBlock16PostingsIteration (64bit)", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			rbm := newBaseDeltaBlock16Postings(bufBDB16.Get())
			// bench.StartTimer()

			for i := 0; i < len(ls); i++ {
				testutil.Assert(bench, rbm.Next() == true, "")
				testutil.Equals(bench, ls[i], rbm.At())
			}
			testutil.Assert(bench, rbm.Next() == false, "")
			testutil.Assert(bench, rbm.Err() == nil, "")
		}
	})

	b.Run("bigEndianSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			bep := newBigEndianPostings(bufBE)
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, bep.Seek(v.seek))
				testutil.Equals(bench, v.val, bep.At())
				testutil.Assert(bench, bep.Err() == nil, "")
			}
		}
	})
	b.Run("baseDeltaSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			bdp := newBaseDeltaPostings(bufBD.Get(), uint64(ls[0]), width, len(ls))
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, bdp.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), bdp.At())
				testutil.Assert(bench, bdp.Err() == nil, "")
			}
		}
	})
	b.Run("baseDeltaBlock16PostingsSeek (64bit)", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			rbm := newBaseDeltaBlock16Postings(bufBDB16.Get())
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, rbm.Seek(v.seek))
				testutil.Equals(bench, v.val, rbm.At())
				testutil.Assert(bench, rbm.Err() == nil, "")
			}
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
		// ls[i] = ls[i-1] + 2
	}

	// bigEndianPostings.
	bufBE := make([]byte, num*4)
	for i := 0; i < num; i++ {
		b := bufBE[i*4 : i*4+4]
		binary.BigEndian.PutUint32(b, ls[i])
	}
	b.Log("bigEndianPostings size =", len(bufBE))

	// baseDeltaPostings.
	width := (bits.Len32(ls[len(ls)-1] - ls[0]) + 7) >> 3
	bufBD := encoding.Encbuf{}
	for i := 0; i < 8 - width; i ++ {
		bufBD.B = append(bufBD.B, 0)
	}
	for i := 0; i < num; i++ {
		for j := width - 1; j >= 0; j-- {
			bufBD.B = append(bufBD.B, byte(((ls[i]-ls[0])>>(8*uint(j))&0xff)))
		}
		// bufBD.PutBits(uint64(ls[i]-ls[0]), width)
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

	bufRBM2 := encoding.Encbuf{}
	writeBaseDeltaBlock8Postings(&bufRBM2, ls)
	b.Log("baseDeltaBlock8Postings", len(bufRBM2.Get()))

	bufRBM3 := encoding.Encbuf{}
	writeBaseDeltaBlock16Postings(&bufRBM3, ls)
	b.Log("baseDeltaBlock16Postings", len(bufRBM3.Get()))

	bufBDB16 := encoding.Encbuf{}
	temp := make([]uint64, 0, len(ls))
	for _, x := range ls {
		temp = append(temp, uint64(x))
	}
	writeBaseDeltaBlock16Postings64(&bufBDB16, temp)
	b.Log("baseDeltaBlock16Postings (64bit)", len(bufBDB16.Get()))

	bufRBM4 := encoding.Encbuf{}
	writeBaseDeltaBlock16PostingsV2(&bufRBM4, ls)
	b.Log("baseDeltaBlock16PostingsV2", len(bufRBM4.Get()))

	table := []struct {
		seek  uint32
		val   uint32
		found bool
	}{
		{
			ls[0] - 1, ls[0], true,
		},
		{
			ls[1000], ls[1000], true,
		},
		{
			ls[1001], ls[1001], true,
		},
		{
			ls[2000]+1, ls[2001], true,
		},
		{
			ls[3000], ls[3000], true,
		},
		{
			ls[3001], ls[3001], true,
		},
		{
			ls[4000]+1, ls[4001], true,
		},
		{
			ls[5000], ls[5000], true,
		},
		{
			ls[5001], ls[5001], true,
		},
		{
			ls[6000]+1, ls[6001], true,
		},
		{
			ls[10000], ls[10000], true,
		},
		{
			ls[10001], ls[10001], true,
		},
		{
			ls[20000]+1, ls[20001], true,
		},
		{
			ls[30000], ls[30000], true,
		},
		{
			ls[30001], ls[30001], true,
		},
		{
			ls[40000]+1, ls[40001], true,
		},
		{
			ls[50000], ls[50000], true,
		},
		{
			ls[50001], ls[50001], true,
		},
		{
			ls[60000]+1, ls[60001], true,
		},
		{
			ls[70000], ls[70000], true,
		},
		{
			ls[70001], ls[70001], true,
		},
		{
			ls[80000]+1, ls[80001], true,
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
			// bench.StopTimer()
			bep := newBigEndianPostings(bufBE)
			// bench.StartTimer()

			for i := 0; i < num; i++ {
				testutil.Assert(bench, bep.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), bep.At())
			}
			testutil.Assert(bench, bep.Next() == false, "")
			testutil.Assert(bench, bep.Err() == nil, "")
		}
	})
	// b.Run("baseDeltaIteration", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		bdp := newBaseDeltaPostings(bufBD.Get(), uint64(ls[0]), width, len(ls))
	// 		// bench.StartTimer()

	// 		for i := 0; i < num; i++ {
	// 			testutil.Assert(bench, bdp.Next() == true, "")
	// 			testutil.Equals(bench, uint64(ls[i]), bdp.At())
	// 		}
	// 		testutil.Assert(bench, bdp.Next() == false, "")
	// 		testutil.Assert(bench, bdp.Err() == nil, "")
	// 	}
	// })
	// b.Run("baseDeltaBlockIteration", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		bdbp := newBaseDeltaBlockPostings(bufBDB.Get())
	// 		// bench.StartTimer()

	// 		for i := 0; i < num; i++ {
	// 			testutil.Assert(bench, bdbp.Next() == true, "")
	// 			testutil.Equals(bench, uint64(ls[i]), bdbp.At())
	// 		}
	// 		testutil.Assert(bench, bdbp.Next() == false, "")
	// 		testutil.Assert(bench, bdbp.Err() == nil, "")
	// 	}
	// })
	// b.Run("roaringBitmapPostingsIteration", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		rbm := newRoaringBitmapPostings(bufRBM.Get())
	// 		// bench.StartTimer()

	// 		for i := 0; i < num; i++ {
	// 			testutil.Assert(bench, rbm.Next() == true, "")
	// 			testutil.Equals(bench, uint64(ls[i]), rbm.At())
	// 		}
	// 		testutil.Assert(bench, rbm.Next() == false, "")
	// 		testutil.Assert(bench, rbm.Err() == nil, "")
	// 	}
	// })
	// b.Run("baseDeltaBlock8PostingsIteration", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		rbm := newBaseDeltaBlock8Postings(bufRBM2.Get())
	// 		// bench.StartTimer()

	// 		for i := 0; i < num; i++ {
	// 			testutil.Assert(bench, rbm.Next() == true, "")
	// 			testutil.Equals(bench, uint64(ls[i]), rbm.At())
	// 		}
	// 		testutil.Assert(bench, rbm.Next() == false, "")
	// 		testutil.Assert(bench, rbm.Err() == nil, "")
	// 	}
	// })
	// b.Run("baseDeltaBlock16PostingsIteration", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		rbm := newBaseDeltaBlock16Postings(bufRBM3.Get())
	// 		// bench.StartTimer()

	// 		for i := 0; i < num; i++ {
	// 			testutil.Assert(bench, rbm.Next() == true, "")
	// 			testutil.Equals(bench, uint64(ls[i]), rbm.At())
	// 		}
	// 		testutil.Assert(bench, rbm.Next() == false, "")
	// 		testutil.Assert(bench, rbm.Err() == nil, "")
	// 	}
	// })
	b.Run("baseDeltaBlock16PostingsIteration (64bit)", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			rbm := newBaseDeltaBlock16Postings(bufBDB16.Get())
			// bench.StartTimer()

			for i := 0; i < num; i++ {
				testutil.Assert(bench, rbm.Next() == true, "")
				testutil.Equals(bench, uint64(ls[i]), rbm.At())
			}
			testutil.Assert(bench, rbm.Next() == false, "")
			testutil.Assert(bench, rbm.Err() == nil, "")
		}
	})
	// b.Run("baseDeltaBlock16PostingsV2Iteration", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		rbm := newBaseDeltaBlock16PostingsV2(bufRBM4.Get())
	// 		// bench.StartTimer()

	// 		for i := 0; i < num; i++ {
	// 			testutil.Assert(bench, rbm.Next() == true, "")
	// 			testutil.Equals(bench, uint64(ls[i]), rbm.At())
	// 		}
	// 		testutil.Assert(bench, rbm.Next() == false, "")
	// 		testutil.Assert(bench, rbm.Err() == nil, "")
	// 	}
	// })

	b.Run("bigEndianSeek", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			bep := newBigEndianPostings(bufBE)
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, bep.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), bep.At())
				testutil.Assert(bench, bep.Err() == nil, "")
			}
		}
	})
	// b.Run("baseDeltaSeek", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		bdp := newBaseDeltaPostings(bufBD.Get(), uint64(ls[0]), width, len(ls))
	// 		// bench.StartTimer()

	// 		for _, v := range table {
	// 			testutil.Equals(bench, v.found, bdp.Seek(uint64(v.seek)))
	// 			testutil.Equals(bench, uint64(v.val), bdp.At())
	// 			testutil.Assert(bench, bdp.Err() == nil, "")
	// 		}
	// 	}
	// })
	// b.Run("baseDeltaBlockSeek", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		bdbp := newBaseDeltaBlockPostings(bufBDB.Get())
	// 		// bench.StartTimer()

	// 		for _, v := range table {
	// 			testutil.Equals(bench, v.found, bdbp.Seek(uint64(v.seek)))
	// 			testutil.Equals(bench, uint64(v.val), bdbp.At())
	// 			testutil.Assert(bench, bdbp.Err() == nil, "")
	// 		}
	// 	}
	// })
	// b.Run("roaringBitmapPostingsSeek", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		rbm := newRoaringBitmapPostings(bufRBM.Get())
	// 		// bench.StartTimer()

	// 		for _, v := range table {
	// 			testutil.Equals(bench, v.found, rbm.Seek(uint64(v.seek)))
	// 			testutil.Equals(bench, uint64(v.val), rbm.At())
	// 			testutil.Assert(bench, rbm.Err() == nil, "")
	// 		}
	// 	}
	// })
	// b.Run("baseDeltaBlock8PostingsSeek", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		rbm := newBaseDeltaBlock8Postings(bufRBM2.Get())
	// 		// bench.StartTimer()

	// 		for _, v := range table {
	// 			testutil.Equals(bench, v.found, rbm.Seek(uint64(v.seek)))
	// 			testutil.Equals(bench, uint64(v.val), rbm.At())
	// 			testutil.Assert(bench, rbm.Err() == nil, "")
	// 		}
	// 	}
	// })
	// b.Run("baseDeltaBlock16PostingsSeek", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		rbm := newBaseDeltaBlock16Postings(bufRBM3.Get())
	// 		// bench.StartTimer()

	// 		for _, v := range table {
	// 			testutil.Equals(bench, v.found, rbm.Seek(uint64(v.seek)))
	// 			testutil.Equals(bench, uint64(v.val), rbm.At())
	// 			testutil.Assert(bench, rbm.Err() == nil, "")
	// 		}
	// 	}
	// })
	b.Run("baseDeltaBlock16PostingsSeek (64bit)", func(bench *testing.B) {
		bench.ResetTimer()
		bench.ReportAllocs()
		for j := 0; j < bench.N; j++ {
			// bench.StopTimer()
			rbm := newBaseDeltaBlock16Postings(bufBDB16.Get())
			// bench.StartTimer()

			for _, v := range table {
				testutil.Equals(bench, v.found, rbm.Seek(uint64(v.seek)))
				testutil.Equals(bench, uint64(v.val), rbm.At())
				testutil.Assert(bench, rbm.Err() == nil, "")
			}
		}
	})
	// b.Run("baseDeltaBlock16PostingsV2Seek", func(bench *testing.B) {
	// 	bench.ResetTimer()
	// 	bench.ReportAllocs()
	// 	for j := 0; j < bench.N; j++ {
	// 		// bench.StopTimer()
	// 		rbm := newBaseDeltaBlock16PostingsV2(bufRBM4.Get())
	// 		// bench.StartTimer()

	// 		for _, v := range table {
	// 			testutil.Equals(bench, v.found, rbm.Seek(uint64(v.seek)))
	// 			testutil.Equals(bench, uint64(v.val), rbm.At())
	// 			testutil.Assert(bench, rbm.Err() == nil, "")
	// 		}
	// 	}
	// })
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
