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
	// "time"
	// "fmt"
	"container/heap"
	"encoding/binary"
	"math/bits"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/tsdb/encoding"
	"github.com/prometheus/tsdb/labels"
)

var allPostingsKey = labels.Label{}

// AllPostingsKey returns the label key that is used to store the postings list of all existing IDs.
func AllPostingsKey() (name, value string) {
	return allPostingsKey.Name, allPostingsKey.Value
}

// MemPostings holds postings list for series ID per label pair. They may be written
// to out of order.
// ensureOrder() must be called once before any reads are done. This allows for quick
// unordered batch fills on startup.
type MemPostings struct {
	mtx     sync.RWMutex
	m       map[string]map[string][]uint64
	ordered bool
}

// NewMemPostings returns a memPostings that's ready for reads and writes.
func NewMemPostings() *MemPostings {
	return &MemPostings{
		m:       make(map[string]map[string][]uint64, 512),
		ordered: true,
	}
}

// NewUnorderedMemPostings returns a memPostings that is not safe to be read from
// until ensureOrder was called once.
func NewUnorderedMemPostings() *MemPostings {
	return &MemPostings{
		m:       make(map[string]map[string][]uint64, 512),
		ordered: false,
	}
}

// SortedKeys returns a list of sorted label keys of the postings.
func (p *MemPostings) SortedKeys() []labels.Label {
	p.mtx.RLock()
	keys := make([]labels.Label, 0, len(p.m))

	for n, e := range p.m {
		for v := range e {
			keys = append(keys, labels.Label{Name: n, Value: v})
		}
	}
	p.mtx.RUnlock()

	sort.Slice(keys, func(i, j int) bool {
		if d := strings.Compare(keys[i].Name, keys[j].Name); d != 0 {
			return d < 0
		}
		return keys[i].Value < keys[j].Value
	})
	return keys
}

// Get returns a postings list for the given label pair.
func (p *MemPostings) Get(name, value string) Postings {
	var lp []uint64
	p.mtx.RLock()
	l := p.m[name]
	if l != nil {
		lp = l[value]
	}
	p.mtx.RUnlock()

	if lp == nil {
		return EmptyPostings()
	}
	return newListPostings(lp...)
}

// All returns a postings list over all documents ever added.
func (p *MemPostings) All() Postings {
	return p.Get(AllPostingsKey())
}

// EnsureOrder ensures that all postings lists are sorted. After it returns all further
// calls to add and addFor will insert new IDs in a sorted manner.
func (p *MemPostings) EnsureOrder() {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.ordered {
		return
	}

	n := runtime.GOMAXPROCS(0)
	workc := make(chan []uint64)

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			for l := range workc {
				sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
			}
			wg.Done()
		}()
	}

	for _, e := range p.m {
		for _, l := range e {
			workc <- l
		}
	}
	close(workc)
	wg.Wait()

	p.ordered = true
}

// Delete removes all ids in the given map from the postings lists.
func (p *MemPostings) Delete(deleted map[uint64]struct{}) {
	var keys, vals []string

	// Collect all keys relevant for deletion once. New keys added afterwards
	// can by definition not be affected by any of the given deletes.
	p.mtx.RLock()
	for n := range p.m {
		keys = append(keys, n)
	}
	p.mtx.RUnlock()

	for _, n := range keys {
		p.mtx.RLock()
		vals = vals[:0]
		for v := range p.m[n] {
			vals = append(vals, v)
		}
		p.mtx.RUnlock()

		// For each posting we first analyse whether the postings list is affected by the deletes.
		// If yes, we actually reallocate a new postings list.
		for _, l := range vals {
			// Only lock for processing one postings list so we don't block reads for too long.
			p.mtx.Lock()

			found := false
			for _, id := range p.m[n][l] {
				if _, ok := deleted[id]; ok {
					found = true
					break
				}
			}
			if !found {
				p.mtx.Unlock()
				continue
			}
			repl := make([]uint64, 0, len(p.m[n][l]))

			for _, id := range p.m[n][l] {
				if _, ok := deleted[id]; !ok {
					repl = append(repl, id)
				}
			}
			if len(repl) > 0 {
				p.m[n][l] = repl
			} else {
				delete(p.m[n], l)
			}
			p.mtx.Unlock()
		}
		p.mtx.Lock()
		if len(p.m[n]) == 0 {
			delete(p.m, n)
		}
		p.mtx.Unlock()
	}
}

// Iter calls f for each postings list. It aborts if f returns an error and returns it.
func (p *MemPostings) Iter(f func(labels.Label, Postings) error) error {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	for n, e := range p.m {
		for v, p := range e {
			if err := f(labels.Label{Name: n, Value: v}, newListPostings(p...)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Add a label set to the postings index.
func (p *MemPostings) Add(id uint64, lset labels.Labels) {
	p.mtx.Lock()

	for _, l := range lset {
		p.addFor(id, l)
	}
	p.addFor(id, allPostingsKey)

	p.mtx.Unlock()
}

func (p *MemPostings) addFor(id uint64, l labels.Label) {
	nm, ok := p.m[l.Name]
	if !ok {
		nm = map[string][]uint64{}
		p.m[l.Name] = nm
	}
	list := append(nm[l.Value], id)
	nm[l.Value] = list

	if !p.ordered {
		return
	}
	// There is no guarantee that no higher ID was inserted before as they may
	// be generated independently before adding them to postings.
	// We repair order violations on insert. The invariant is that the first n-1
	// items in the list are already sorted.
	for i := len(list) - 1; i >= 1; i-- {
		if list[i] >= list[i-1] {
			break
		}
		list[i], list[i-1] = list[i-1], list[i]
	}
}

// ExpandPostings returns the postings expanded as a slice.
func ExpandPostings(p Postings) (res []uint64, err error) {
	for p.Next() {
		res = append(res, p.At())
	}
	return res, p.Err()
}

// Postings provides iterative access over a postings list.
type Postings interface {
	// Next advances the iterator and returns true if another value was found.
	Next() bool

	// Seek advances the iterator to value v or greater and returns
	// true if a value was found.
	Seek(v uint64) bool

	// At returns the value at the current iterator position.
	At() uint64

	// Err returns the last error of the iterator.
	Err() error
}

// errPostings is an empty iterator that always errors.
type errPostings struct {
	err error
}

func (e errPostings) Next() bool       { return false }
func (e errPostings) Seek(uint64) bool { return false }
func (e errPostings) At() uint64       { return 0 }
func (e errPostings) Err() error       { return e.err }

var emptyPostings = errPostings{}

// EmptyPostings returns a postings list that's always empty.
// NOTE: Returning EmptyPostings sentinel when index.Postings struct has no postings is recommended.
// It triggers optimized flow in other functions like Intersect, Without etc.
func EmptyPostings() Postings {
	return emptyPostings
}

// ErrPostings returns new postings that immediately error.
func ErrPostings(err error) Postings {
	return errPostings{err}
}

// Intersect returns a new postings list over the intersection of the
// input postings.
func Intersect(its ...Postings) Postings {
	if len(its) == 0 {
		return EmptyPostings()
	}
	if len(its) == 1 {
		return its[0]
	}
	for _, p := range its {
		if p == EmptyPostings() {
			return EmptyPostings()
		}
	}

	return newIntersectPostings(its...)
}

type intersectPostings struct {
	arr []Postings
	cur uint64
}

func newIntersectPostings(its ...Postings) *intersectPostings {
	return &intersectPostings{arr: its}
}

func (it *intersectPostings) At() uint64 {
	return it.cur
}

func (it *intersectPostings) doNext() bool {
Loop:
	for {
		for _, p := range it.arr {
			if !p.Seek(it.cur) {
				return false
			}
			if p.At() > it.cur {
				it.cur = p.At()
				continue Loop
			}
		}
		return true
	}
}

func (it *intersectPostings) Next() bool {
	for _, p := range it.arr {
		if !p.Next() {
			return false
		}
		if p.At() > it.cur {
			it.cur = p.At()
		}
	}
	return it.doNext()
}

func (it *intersectPostings) Seek(id uint64) bool {
	it.cur = id
	return it.doNext()
}

func (it *intersectPostings) Err() error {
	for _, p := range it.arr {
		if p.Err() != nil {
			return p.Err()
		}
	}
	return nil
}

// Merge returns a new iterator over the union of the input iterators.
func Merge(its ...Postings) Postings {
	if len(its) == 0 {
		return EmptyPostings()
	}
	if len(its) == 1 {
		return its[0]
	}

	p, ok := newMergedPostings(its)
	if !ok {
		return EmptyPostings()
	}
	return p
}

type postingsHeap []Postings

func (h postingsHeap) Len() int           { return len(h) }
func (h postingsHeap) Less(i, j int) bool { return h[i].At() < h[j].At() }
func (h *postingsHeap) Swap(i, j int)     { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *postingsHeap) Push(x interface{}) {
	*h = append(*h, x.(Postings))
}

func (h *postingsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type mergedPostings struct {
	h          postingsHeap
	initilized bool
	cur        uint64
	err        error
}

func newMergedPostings(p []Postings) (m *mergedPostings, nonEmpty bool) {
	ph := make(postingsHeap, 0, len(p))

	for _, it := range p {
		// NOTE: mergedPostings struct requires the user to issue an initial Next.
		if it.Next() {
			ph = append(ph, it)
		} else {
			if it.Err() != nil {
				return &mergedPostings{err: it.Err()}, true
			}
		}
	}

	if len(ph) == 0 {
		return nil, false
	}
	return &mergedPostings{h: ph}, true
}

func (it *mergedPostings) Next() bool {
	if it.h.Len() == 0 || it.err != nil {
		return false
	}

	// The user must issue an initial Next.
	if !it.initilized {
		heap.Init(&it.h)
		it.cur = it.h[0].At()
		it.initilized = true
		return true
	}

	for {
		cur := it.h[0]
		if !cur.Next() {
			heap.Pop(&it.h)
			if cur.Err() != nil {
				it.err = cur.Err()
				return false
			}
			if it.h.Len() == 0 {
				return false
			}
		} else {
			// Value of top of heap has changed, re-heapify.
			heap.Fix(&it.h, 0)
		}

		if it.h[0].At() != it.cur {
			it.cur = it.h[0].At()
			return true
		}
	}
}

func (it *mergedPostings) Seek(id uint64) bool {
	if it.h.Len() == 0 || it.err != nil {
		return false
	}
	if !it.initilized {
		if !it.Next() {
			return false
		}
	}
	for it.cur < id {
		cur := it.h[0]
		if !cur.Seek(id) {
			heap.Pop(&it.h)
			if cur.Err() != nil {
				it.err = cur.Err()
				return false
			}
			if it.h.Len() == 0 {
				return false
			}
		} else {
			// Value of top of heap has changed, re-heapify.
			heap.Fix(&it.h, 0)
		}

		it.cur = it.h[0].At()
	}
	return true
}

func (it mergedPostings) At() uint64 {
	return it.cur
}

func (it mergedPostings) Err() error {
	return it.err
}

// Without returns a new postings list that contains all elements from the full list that
// are not in the drop list.
func Without(full, drop Postings) Postings {
	if full == EmptyPostings() {
		return EmptyPostings()
	}

	if drop == EmptyPostings() {
		return full
	}
	return newRemovedPostings(full, drop)
}

type removedPostings struct {
	full, remove Postings

	cur uint64

	initialized bool
	fok, rok    bool
}

func newRemovedPostings(full, remove Postings) *removedPostings {
	return &removedPostings{
		full:   full,
		remove: remove,
	}
}

func (rp *removedPostings) At() uint64 {
	return rp.cur
}

func (rp *removedPostings) Next() bool {
	if !rp.initialized {
		rp.fok = rp.full.Next()
		rp.rok = rp.remove.Next()
		rp.initialized = true
	}
	for {
		if !rp.fok {
			return false
		}

		if !rp.rok {
			rp.cur = rp.full.At()
			rp.fok = rp.full.Next()
			return true
		}

		fcur, rcur := rp.full.At(), rp.remove.At()
		if fcur < rcur {
			rp.cur = fcur
			rp.fok = rp.full.Next()

			return true
		} else if rcur < fcur {
			// Forward the remove postings to the right position.
			rp.rok = rp.remove.Seek(fcur)
		} else {
			// Skip the current posting.
			rp.fok = rp.full.Next()
		}
	}
}

func (rp *removedPostings) Seek(id uint64) bool {
	if rp.cur >= id {
		return true
	}

	rp.fok = rp.full.Seek(id)
	rp.rok = rp.remove.Seek(id)
	rp.initialized = true

	return rp.Next()
}

func (rp *removedPostings) Err() error {
	if rp.full.Err() != nil {
		return rp.full.Err()
	}

	return rp.remove.Err()
}

// ListPostings implements the Postings interface over a plain list.
type ListPostings struct {
	list []uint64
	cur  uint64
}

func NewListPostings(list []uint64) Postings {
	return newListPostings(list...)
}

func newListPostings(list ...uint64) *ListPostings {
	return &ListPostings{list: list}
}

func (it *ListPostings) At() uint64 {
	return it.cur
}

func (it *ListPostings) Next() bool {
	if len(it.list) > 0 {
		it.cur = it.list[0]
		it.list = it.list[1:]
		return true
	}
	it.cur = 0
	return false
}

func (it *ListPostings) Seek(x uint64) bool {
	// If the current value satisfies, then return.
	if it.cur >= x {
		return true
	}
	if len(it.list) == 0 {
		return false
	}

	// Do binary search between current position and end.
	i := sort.Search(len(it.list), func(i int) bool {
		return it.list[i] >= x
	})
	if i < len(it.list) {
		it.cur = it.list[i]
		it.list = it.list[i+1:]
		return true
	}
	it.list = nil
	return false
}

func (it *ListPostings) Err() error {
	return nil
}

// bigEndianPostings implements the Postings interface over a byte stream of
// big endian numbers.
type bigEndianPostings struct {
	list []byte
	cur  uint32
}

func newBigEndianPostings(list []byte) *bigEndianPostings {
	return &bigEndianPostings{list: list}
}

func (it *bigEndianPostings) At() uint64 {
	return uint64(it.cur)
}

func (it *bigEndianPostings) Next() bool {
	if len(it.list) >= 4 {
		it.cur = binary.BigEndian.Uint32(it.list)
		it.list = it.list[4:]
		return true
	}
	return false
}

func (it *bigEndianPostings) Seek(x uint64) bool {
	if uint64(it.cur) >= x {
		return true
	}

	num := len(it.list) / 4
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint32(it.list[i*4:]) >= uint32(x)
	})
	if i < num {
		j := i * 4
		it.cur = binary.BigEndian.Uint32(it.list[j:])
		it.list = it.list[j+4:]
		return true
	}
	it.list = nil
	return false
}

func (it *bigEndianPostings) Err() error {
	return nil
}

// 1 is bigEndian, 2 is baseDelta, 3 is deltaBlock, 4 is baseDeltaBlock, 5 is bitmapPostings, 6 is roaringBitmapPostings.
const postingsType = 7

type bitSlice struct {
	bstream []byte
	width   int
}

func (bs *bitSlice) readByte(idx int, count uint8) byte {
	if count == 0 {
		return bs.bstream[idx]
	}

	byt := bs.bstream[idx] << count
	byt |= bs.bstream[idx+1] >> (8 - count)

	return byt
}

// This is to read the delta bitpack given an offset.
// Check whether out-of-bounds before using.
func (bs *bitSlice) readBits(offset int) uint64 {
	idx := offset / 8
	count := uint8(offset % 8)
	nbits := bs.width
	var u uint64

	for nbits >= 8 {
		byt := bs.readByte(idx, count)

		u = (u << 8) | uint64(byt)
		nbits -= 8
		idx += 1
	}

	if nbits == 0 {
		return u
	}

	if nbits > int(8-count) {
		u = (u << uint(8-count)) | uint64((bs.bstream[idx]<<count)>>count)
		nbits -= int(8 - count)
		idx += 1

		count = 0
	}

	u = (u << uint(nbits)) | uint64((bs.bstream[idx]<<count)>>(8-uint(nbits)))
	return u
}

// ┌──────────┬────────────────┬────────────┬────────────────┬─────┬────────────────┐
// │ num <4b> │ base <uvarint> │ width <1b> │ delta 1 <bits> │ ... │ delta n <bits> │
// └──────────┴────────────────┴────────────┴────────────────┴─────┴────────────────┘
type baseDeltaPostings struct {
	bs    []byte
	width int
	base  uint64
	size  int
	idx   int
	i     int
	cur   uint64
	mask  uint64
	prel  int
}

func newBaseDeltaPostings(bstream []byte, base uint64, width int, size int) *baseDeltaPostings {
	return &baseDeltaPostings{bs: bstream, width: width, base: base, size: size, idx: 8 - width, cur: uint64(base), mask: (uint64(1) << (uint64(width) << 3)) - 1, prel: 8 - width}
}

func (it *baseDeltaPostings) At() uint64 {
	return it.cur
}

func (it *baseDeltaPostings) Next() bool {
	if it.i >= it.size {
		return false
	}
	it.cur = binary.BigEndian.Uint64(it.bs[it.idx-it.prel:])&it.mask + it.base
	it.idx += it.width
	it.i += 1
	return true
}

func (it *baseDeltaPostings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}

	num := it.size - it.i
	x -= it.base
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint64(it.bs[it.idx+i*it.width-it.prel:])&it.mask >= x
	})
	if i < num {
		it.idx += i * it.width
		it.cur = it.base + (binary.BigEndian.Uint64(it.bs[it.idx-it.prel:])&it.mask)
		it.idx += it.width
		it.i += i + 1
		return true
	}
	return false
}

func (it *baseDeltaPostings) Err() error {
	return nil
}

const deltaBlockSize = 32
const deltaBlockBits = 5

// Block format(delta is to the previous value).
// ┌────────────────┬───────────────┬─────────────────┬────────────┬────────────────┬─────┬────────────────┐
// │ base <uvarint> │ idx <uvarint> │ count <uvarint> │ width <1b> │ delta 1 <bits> │ ... │ delta n <bits> │
// └────────────────┴───────────────┴─────────────────┴────────────┴────────────────┴─────┴────────────────┘
type deltaBlockPostings struct {
	bs       bitSlice
	size     int
	count    int // count in current block.
	idxBlock int
	idx      int
	offset   int // offset in bit.
	cur      uint64
}

func newDeltaBlockPostings(bstream []byte, size int) *deltaBlockPostings {
	return &deltaBlockPostings{bs: bitSlice{bstream: bstream}, size: size}
}

func (it *deltaBlockPostings) GetOff() int {
	return it.offset
}
func (it *deltaBlockPostings) GetWidth() int {
	return it.bs.width
}

func (it *deltaBlockPostings) At() uint64 {
	return it.cur
}

func (it *deltaBlockPostings) Next() bool {
	if it.offset >= len(it.bs.bstream)<<3 || it.idx >= it.size {
		return false
	}
	if it.offset%(deltaBlockSize<<3) == 0 {
		val, n := binary.Uvarint(it.bs.bstream[it.offset>>3:])
		if n < 1 {
			return false
		}
		it.cur = val
		it.offset += n << 3
		val, n = binary.Uvarint(it.bs.bstream[it.offset>>3:])
		if n < 1 {
			return false
		}
		it.idx = int(val) + 1
		it.offset += n << 3
		val, n = binary.Uvarint(it.bs.bstream[it.offset>>3:])
		if n < 1 {
			return false
		}
		it.count = int(val)
		it.offset += n << 3
		it.bs.width = int(it.bs.bstream[it.offset>>3])
		it.offset += 8
		it.idxBlock = 1
		return true
	}

	it.cur = it.bs.readBits(it.offset) + it.cur
	it.offset += it.bs.width
	it.idx += 1
	it.idxBlock += 1
	if it.idxBlock == it.count {
		it.offset = ((it.offset-1)/(deltaBlockSize<<3) + 1) * deltaBlockSize << 3
	}
	return true
}

func (it *deltaBlockPostings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}

	startOff := (it.offset - 1) / (deltaBlockSize << 3) * deltaBlockSize
	num := (len(it.bs.bstream)-1)/deltaBlockSize - (it.offset-1)/(deltaBlockSize<<3) + 1
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		val, _ := binary.Uvarint(it.bs.bstream[startOff+i*deltaBlockSize:])
		return val > x
	})
	if i > 0 {
		// Go to the previous block because the previous block
		// may contain the first value >= x.
		i -= 1
	}
	it.offset = (startOff + i*deltaBlockSize) << 3
	for it.Next() {
		if it.At() >= x {
			return true
		}
	}
	return false
}

func (it *deltaBlockPostings) Err() error {
	return nil
}

func writeDeltaBlockPostings(e *encoding.Encbuf, arr []uint32) {
	i := 0
	startLen := len(e.B)
	deltas := []uint32{}
	var remaining int
	var preVal uint32
	var max int
	for i < len(arr) {
		e.PutUvarint32(arr[i])    // Put base.
		e.PutUvarint64(uint64(i)) // Put idx.
		remaining = (deltaBlockSize - (len(e.B)-startLen)%deltaBlockSize - 1) << 3
		deltas = deltas[:0]
		preVal = arr[i]
		max = -1
		i += 1
		for i < len(arr) {
			delta := arr[i] - preVal
			cur := bits.Len32(delta)
			if cur <= max {
				cur = max
			}
			if remaining-cur*(len(deltas)+1)-(((bits.Len(uint(len(deltas)))>>3)+1)<<3) >= 0 {
				deltas = append(deltas, delta)
				max = cur
				preVal = arr[i]
			} else {
				break
			}
			i += 1
		}
		e.PutUvarint64(uint64(len(deltas) + 1))
		e.PutByte(byte(max))
		remaining -= ((bits.Len(uint(len(deltas))) >> 3) + 1) << 3
		for _, delta := range deltas {
			e.PutBits(uint64(delta), max)
			remaining -= max
		}

		if i == len(arr) {
			break
		}

		for remaining >= 64 {
			e.PutBits(uint64(0), 64)
			remaining -= 64
		}

		if remaining > 0 {
			e.PutBits(uint64(0), remaining)
		}
		e.Count = 0

		// There can be one more extra 0.
		e.B = e.B[:len(e.B)-(len(e.B)-startLen)%deltaBlockSize]
	}
}

// Block format(delta is to the base).
// ┌────────────────┬─────────────────┬────────────┬─────────────────┬─────┬─────────────────┐
// │ base <uvarint> │ count <uvarint> │ width <1b> │ delta 1 <bytes> │ ... │ delta n <bytes> │
// └────────────────┴─────────────────┴────────────┴─────────────────┴─────┴─────────────────┘
type baseDeltaBlockPostings struct {
	bs       bitSlice
	count    int // count in current block.
	idxBlock int
	idx      int
	offset   int // offset in bit.
	cur      uint64
	base     uint64
	mask     uint64
	prel     int
}

func newBaseDeltaBlockPostings(bstream []byte) *baseDeltaBlockPostings {
	return &baseDeltaBlockPostings{bs: bitSlice{bstream: bstream}}
}

func (it *baseDeltaBlockPostings) At() uint64 {
	return it.cur
}

func (it *baseDeltaBlockPostings) Next() bool {
	if it.offset >= len(it.bs.bstream) {
		return false
	}
	if it.offset%deltaBlockSize == 0 {
		val, n := binary.Uvarint(it.bs.bstream[it.offset:])
		it.cur = val
		it.base = val
		it.offset += n

		val, n = binary.Uvarint(it.bs.bstream[it.offset:])
		it.count = int(val)
		it.offset += n
		it.bs.width = int(it.bs.bstream[it.offset])
		it.mask = (uint64(1) << uint(8 * it.bs.width)) - 1
		it.prel = 8 - it.bs.width
		it.offset += 1
		it.idxBlock = 1
		return true
	}

	if it.offset-it.prel >= 0 {
		it.cur = binary.BigEndian.Uint64(it.bs.bstream[it.offset-it.prel:])&it.mask + it.base
	} else {
		it.cur = 0
		for i := 0; i < it.bs.width; i++ {
			it.cur = (it.cur << 8) | uint64(it.bs.bstream[it.offset+i])
		}
		it.cur += it.base
	}
	// it.cur = (binary.BigEndian.Uint64(it.bs.bstream[it.offset-it.prel:])&it.mask) + it.base
	it.offset += it.bs.width
	it.idxBlock += 1
	if it.idxBlock == it.count {
		it.offset = (((it.offset - 1) >> deltaBlockBits) + 1) << deltaBlockBits
	}
	return true
}

func (it *baseDeltaBlockPostings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}
	if it.offset >= len(it.bs.bstream) {
		return false
	}
	startOff := (((it.offset) >> deltaBlockBits) + 1) << deltaBlockBits
	num := (len(it.bs.bstream) >> deltaBlockBits) - (startOff >> deltaBlockBits) + 1
	if num > 0 {
		// Fast path to check if the binary search among blocks is needed.
		val, _ := binary.Uvarint(it.bs.bstream[startOff:])
		if val <= x {
			// Do binary search between current position and end.
			i := sort.Search(num, func(i int) bool {
				val, _ := binary.Uvarint(it.bs.bstream[startOff+(i<<deltaBlockBits):])
				return val > x
			})
			if i > 0 {
				// Go to the previous block because the previous block
				// may contain the first value >= x.
				i -= 1
			}
			it.offset = startOff + (i << deltaBlockBits)

			// Read base, and width.
			val, n := binary.Uvarint(it.bs.bstream[it.offset:])
			it.cur = val
			it.base = val
			it.offset += n
			val, n = binary.Uvarint(it.bs.bstream[it.offset:])
			it.count = int(val)
			it.offset += n
			it.bs.width = int(it.bs.bstream[it.offset])
			it.mask = (uint64(1) << uint(8 * it.bs.width)) - 1
			it.prel = 8 - it.bs.width
			it.offset += 1
			it.idxBlock = 1
			if x <= it.base {
				return true
			} else {
				temp := x - it.base
				j := sort.Search(it.count-it.idxBlock, func(i int) bool {
					return (binary.BigEndian.Uint64(it.bs.bstream[it.offset+i*it.bs.width-it.prel:])&it.mask) >= temp
				})
				if j < it.count-it.idxBlock {
					it.offset += j * it.bs.width
					it.cur = (binary.BigEndian.Uint64(it.bs.bstream[it.offset-it.prel:])&it.mask) + it.base
					it.idxBlock += j + 1
					if it.idxBlock == it.count {
						// it.offset = startOff + ((i+1)<<deltaBlockBits)
						it.offset = ((startOff >> deltaBlockBits) + i + 1) << deltaBlockBits
					} else {
						it.offset += it.bs.width
					}
				} else {
					// it.offset = startOff + ((i+1)<<deltaBlockBits)
					it.offset = ((startOff >> deltaBlockBits) + i + 1) << deltaBlockBits
					return it.Next()
				}
				return true
			}
		}
	}

	// Search in current block.
	startOff -= deltaBlockSize
	if it.offset == startOff {
		// Read base, and width.
		val, n := binary.Uvarint(it.bs.bstream[it.offset:])
		it.cur = val
		it.base = val
		it.offset += n
		val, n = binary.Uvarint(it.bs.bstream[it.offset:])
		it.count = int(val)
		it.offset += n
		it.bs.width = int(it.bs.bstream[it.offset])
		it.mask = (uint64(1) << uint(8*it.bs.width)) - 1
		it.prel = 8 - it.bs.width
		it.offset += 1
		it.idxBlock = 1
	}
	if x <= it.base {
		return true
	} else {
		temp := x - it.base
		j := sort.Search(it.count-it.idxBlock, func(i int) bool {
			return (binary.BigEndian.Uint64(it.bs.bstream[it.offset+i*it.bs.width-it.prel:])&it.mask) >= temp
		})
		if j < it.count-it.idxBlock {
			it.offset += j * it.bs.width
			it.cur = (binary.BigEndian.Uint64(it.bs.bstream[it.offset-it.prel:])&it.mask) + it.base
			it.idxBlock += j + 1
			if it.idxBlock == it.count {
				// it.offset = startOff + deltaBlockSize
				it.offset = ((startOff >> deltaBlockBits) + 1) << deltaBlockBits
			} else {
				it.offset += it.bs.width
			}
		} else {
			// it.offset = startOff + deltaBlockSize
			it.offset = ((startOff >> deltaBlockBits) + 1) << deltaBlockBits
			return it.Next()
		}
		return true
	}
	
}

func (it *baseDeltaBlockPostings) Err() error {
	return nil
}

func writeBaseDeltaBlockPostings(e *encoding.Encbuf, arr []uint32) {
	i := 0
	startLen := len(e.B)
	deltas := []uint32{}
	var remaining int
	var base uint32
	var max int
	for i < len(arr) {
		e.PutUvarint32(arr[i]) // Put base.
		remaining = deltaBlockSize - (len(e.B)-startLen)%deltaBlockSize - 1
		deltas = deltas[:0]
		base = arr[i]
		max = -1
		i += 1
		for i < len(arr) {
			delta := arr[i] - base
			cur := (bits.Len32(delta) + 7) >> 3
			if cur == 0 {
				cur = 1
			}
			if remaining-cur*(len(deltas)+1)-((bits.Len(uint(len(deltas)))>>3)+1) >= 0 {
				deltas = append(deltas, delta)
				max = cur
			} else {
				break
			}
			i += 1
		}
		e.PutUvarint64(uint64(len(deltas) + 1))
		e.PutByte(byte(max))
		remaining -= ((bits.Len(uint(len(deltas))) >> 3) + 1)
		for _, delta := range deltas {
			for j := max - 1; j >= 0; j-- {
				e.B = append(e.B, byte((delta >> (uint(j) << 3) & 0xff)))
			}
			remaining -= max
		}

		if i == len(arr) {
			break
		}

		for remaining > 0 {
			e.PutByte(0)
			remaining -= 1
		}
	}
}

func writeBaseDeltaBlockPostings64(e *encoding.Encbuf, arr []uint64) {
	i := 0
	startLen := len(e.B)
	deltas := []uint64{}
	var remaining int
	var base uint64
	var max int
	for i < len(arr) {
		e.PutUvarint64(arr[i]) // Put base.
		remaining = deltaBlockSize - (len(e.B)-startLen)%deltaBlockSize - 1
		deltas = deltas[:0]
		base = arr[i]
		max = -1
		i += 1
		for i < len(arr) {
			delta := arr[i] - base
			cur := (bits.Len64(delta) + 7) >> 3
			if cur == 0 {
				cur = 1
			}
			if remaining-cur*(len(deltas)+1)-((bits.Len(uint(len(deltas)))>>3)+1) >= 0 {
				deltas = append(deltas, delta)
				max = cur
			} else {
				break
			}
			i += 1
		}
		e.PutUvarint64(uint64(len(deltas) + 1))
		e.PutByte(byte(max))
		remaining -= ((bits.Len(uint(len(deltas))) >> 3) + 1)
		for _, delta := range deltas {
			for j := max - 1; j >= 0; j-- {
				e.B = append(e.B, byte((delta >> (uint(j) << 3) & 0xff)))
			}
			remaining -= max
		}

		if i == len(arr) {
			break
		}

		for remaining > 0 {
			e.PutByte(0)
			remaining -= 1
		}
	}
}

// 8bits -> 256/8=32bytes, 12bits -> 4096/8=512bytes, 16bits -> 65536/8=8192bytes.
const bitmapBits = 8

// Bitmap block format.
// ┌──────────┬────────┐
// │ key <4b> │ bitmap │
// └──────────┴────────┘
type bitmapPostings struct {
	bs         []byte
	cur        uint64
	inside     bool
	idx1       int
	idx2       int
	bitmapSize int
	key        uint32
}

func newBitmapPostings(bstream []byte) *bitmapPostings {
	return &bitmapPostings{bs: bstream, bitmapSize: 1 << (bitmapBits - 3)}
}

func (it *bitmapPostings) At() uint64 {
	return it.cur
}

func (it *bitmapPostings) Next() bool {
	if it.inside {
		for it.idx1 < it.bitmapSize {
			if it.bs[it.idx1+4] == byte(0) {
				it.idx1 += 1
				continue
			}
			for it.idx1 < it.bitmapSize {
				if it.bs[it.idx1+4]&(1<<uint(7-it.idx2)) != byte(0) {
					it.cur = uint64(it.key<<bitmapBits) + uint64(it.idx1*8+it.idx2)
					it.idx2 += 1
					if it.idx2 == 8 {
						it.idx1 += 1
						it.idx2 = 0
					}
					return true
				} else {
					it.idx2 += 1
					if it.idx2 == 8 {
						it.idx1 += 1
						it.idx2 = 0
					}
				}
			}
		}
		it.bs = it.bs[it.bitmapSize+4:]
		it.inside = false
		it.idx1 = 0
		return it.Next()
	} else {
		if len(it.bs)-4 >= it.bitmapSize {
			it.key = binary.BigEndian.Uint32(it.bs)
			it.inside = true
			return it.Next()
		} else {
			return false
		}
	}
}

func (it *bitmapPostings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}
	curKey := uint32(x) >> bitmapBits
	// curVal := uint32(x) & uint32((1 << uint(bitmapBits)) - 1)
	i := sort.Search(len(it.bs)/(it.bitmapSize+4), func(i int) bool {
		return binary.BigEndian.Uint32(it.bs[i*(it.bitmapSize+4):]) > curKey
	})
	if i > 0 {
		i -= 1
		if i > 0 {
			it.idx1 = 0
			it.idx2 = 0
			it.bs = it.bs[i*(it.bitmapSize+4):]
			it.inside = false
		}
	}
	for it.Next() {
		if it.At() >= x {
			return true
		}
	}
	return false
}

func (it *bitmapPostings) Err() error {
	return nil
}

func writeBitmapPostings(e *encoding.Encbuf, arr []uint32) {
	key := uint32(0xffffffff)
	bitmapSize := 1 << (bitmapBits - 3)
	mask := uint32((1 << uint(bitmapBits)) - 1)
	var curKey uint32
	var curVal uint32
	var offset int // The starting offset of the bitmap of each block.
	var idx1 int
	var idx2 int
	for _, val := range arr {
		curKey = val >> bitmapBits
		curVal = val & mask
		idx1 = int(curVal) >> 3
		idx2 = int(curVal) % 8
		if curKey != key {
			key = curKey
			e.PutBE32(uint32(key))
			offset = len(e.Get())
			for i := 0; i < bitmapSize; i++ {
				e.PutByte(byte(0))
			}
		}
		e.B[offset+idx1] |= 1 << uint(7-idx2)
	}
}

var rbpMasks []byte
var rbpValueMask uint64
var rbpValueSize int
var rbpBitmapSize int

func init() {
	for i := 7; i >= 0; i-- {
		rbpMasks = append(rbpMasks, byte(1<<uint(i)))
	}
	rbpValueMask = (uint64(1) << uint(bitmapBits)) - 1
	rbpBitmapSize = 1 << (bitmapBits - 3)
	rbpValueSize = bitmapBits >> 3
}

// roaringBitmap block format, type 0 = array, type 1 = bitmap.
// ┌───────────────┬──────────┬──────────────┐
// │ key <uvarint> │ type<1b> │ bitmap/array │
// └───────────────┴──────────┴──────────────┘
// footer format.
// ┌────────────┬─────────────────────┬─────┬─────────────────────┐
// │ width <1b> │ block 1 addr <bits> │ ... │ block n addr <bits> │
// └────────────┴─────────────────────┴─────┴─────────────────────┘
type roaringBitmapPostings struct {
	bs         []byte
	cur        uint64
	inside     bool
	idx        int // The current offset inside the bs.
	idx1       int // The offset in the bitmap in current block in bytes.
	idx2       int // The offset in the current byte in the bitmap ([0,8)).
	footerAddr int
	key        uint64
	numBlock   int
	blockIdx   int
	blockType  byte
	nextBlock  int
	width      int
	addrMask   uint32
}

func newRoaringBitmapPostings(bstream []byte) *roaringBitmapPostings {
	if len(bstream) <= 4 {
		return nil
	}
	x := binary.BigEndian.Uint32(bstream)
	// return &roaringBitmapPostings{bs: bstream[4:], numBlock: int(binary.BigEndian.Uint32(bstream[4+int(x):])), footerAddr: int(x), width: int(bstream[8+int(x)])}
	// return &roaringBitmapPostings{bs: bstream[4:], numBlock: (len(bstream)-int(x))/4 - 1, footerAddr: int(x)}
	// return &roaringBitmapPostings{bs: bstream[4:], numBlock: (len(bstream) - int(x) - 5) / int(bstream[4+int(x)]), footerAddr: int(x), width: int(bstream[4+int(x)]), addrMask: uint32((1 << (8 * uint(bstream[4+int(x)]))) - 1)}
	return &roaringBitmapPostings{bs: bstream[8:], numBlock: int(binary.BigEndian.Uint32(bstream[4:])), footerAddr: int(x), width: int(bstream[8+int(x)]), addrMask: uint32((1 << (8 * uint(bstream[8+int(x)]))) - 1)}
}

func (it *roaringBitmapPostings) At() uint64 {
	return it.cur
}

func (it *roaringBitmapPostings) Next() bool {
	if it.inside { // Already entered the block.
		if it.blockType == 0 { // Type array.
			if it.idx < it.nextBlock {
				it.cur = it.key | uint64(it.bs[it.idx])
				it.idx += 1
				return true
			}
		} else { // Type bitmap.
			for it.idx1 < rbpBitmapSize && it.bs[it.idx+it.idx1] == 0 {
				it.idx1 += 1
			}
			for it.idx1 < rbpBitmapSize {
				if it.bs[it.idx+it.idx1]&rbpMasks[it.idx2] != 0 {
					it.cur = it.key | uint64((it.idx1<<3)+it.idx2)
					it.idx2 += 1
					if it.idx2 == 8 {
						it.idx1 += 1
						it.idx2 = 0
					}
					return true
				} else {
					it.idx2 += 1
					if it.idx2 == 8 {
						it.idx1 += 1
						it.idx2 = 0
					}
				}
			}
			it.idx += rbpBitmapSize
			it.idx1 = 0
			it.idx2 = 0
		}
		it.blockIdx += 1
		it.inside = false
		return it.Next()
	} else { // Not yet entered the block.
		if it.idx < it.footerAddr {
			val, size := binary.Uvarint(it.bs[it.idx:])
			it.key = val << bitmapBits
			it.idx += size
			it.blockType = it.bs[it.idx]
			it.idx += 1
			it.inside = true

			if it.blockType == 0 {
				if it.blockIdx != it.numBlock-1 {
					// it.nextBlock = int(it.readBits((it.footerAddr+5)*8+(it.blockIdx+1)*it.width))
					// it.nextBlock = it.readBytes(it.footerAddr+1+(it.blockIdx+1)*it.width)
					it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-4+it.width:]) & it.addrMask)
					// it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+(it.blockIdx+1)*4:]))
				} else {
					it.nextBlock = it.footerAddr
				}
			}
			return it.Next()
		} else {
			return false
		}
	}
}

func (it *roaringBitmapPostings) seekInBlock(x uint64) bool {
	curVal := byte(x & rbpValueMask)
	if it.blockType == 0 {
		// If encoding with array, binary search.
		num := (it.nextBlock - it.idx)
		j := sort.Search(num, func(i int) bool {
			return it.bs[it.idx+i] >= curVal
		})
		if j == num {
			// The first element in next block should be >= x.
			it.idx = it.nextBlock
			it.inside = false
			return it.Next()
		}

		it.cur = it.key | uint64(it.bs[it.idx+j])
		it.idx += j + 1
		return true
	} else {
		// If encoding with bitmap, go to the exact location of value of x.
		it.idx1 = int(curVal >> 3)
		it.idx2 = int(curVal % 8)
		if it.bs[it.idx+it.idx1]&rbpMasks[it.idx2] != 0 { // Found x.
			it.cur = it.key | uint64(it.idx1*8+it.idx2)
			it.idx2 += 1
			if it.idx2 == 8 {
				it.idx1 += 1
				it.idx2 = 0
			}
			return true
		} else {
			it.idx2 += 1
			if it.idx2 == 8 {
				it.idx1 += 1
				it.idx2 = 0
			}
			return it.Next()
		}
	}
}

func (it *roaringBitmapPostings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}
	curKey := x >> bitmapBits
	if it.inside && it.key>>bitmapBits == curKey {
		// Fast path.
		return it.seekInBlock(x)
	} else {
		i := sort.Search(it.numBlock-it.blockIdx, func(i int) bool {
			// off := int(it.readBits(((it.footerAddr+5)<<3)+(it.blockIdx+i)*it.width))
			// off := int(binary.BigEndian.Uint32(it.bs[it.footerAddr+4*(it.blockIdx+i):]))
			// off := it.readBytes(it.footerAddr+1+(it.blockIdx+i)*it.width)
			off := int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+i)*it.width-4+it.width:]) & it.addrMask)
			k, _ := binary.Uvarint(it.bs[off:])
			return k >= curKey
			// return binary.BigEndian.Uint32(it.bs[off:]) > curKey
		})
		if i == it.numBlock-it.blockIdx {
			return false
		}
		if i != 0 { // i > 0.
			it.idx1 = 0
			it.idx2 = 0
			it.inside = false
			// it.idx = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+4*(it.blockIdx+i):]))
			// it.idx = int(it.readBits(((it.footerAddr+5)<<3)+(it.blockIdx+i)*it.width))
			// it.idx = it.readBytes(it.footerAddr+1+(it.blockIdx+i)*it.width)
			it.idx = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+i)*it.width-4+it.width:]) & it.addrMask)
		}
		it.blockIdx += i
	}

	val, size := binary.Uvarint(it.bs[it.idx:])
	it.key = val << bitmapBits
	it.idx += size
	it.blockType = it.bs[it.idx]
	it.idx += 1
	it.inside = true

	if it.blockType == 0 {
		if it.blockIdx != it.numBlock-1 {
			// it.nextBlock = int(it.readBits((it.footerAddr+5)*8+(it.blockIdx+1)*it.width))
			// it.nextBlock = it.readBytes(it.footerAddr+1+(it.blockIdx+1)*it.width)
			it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-4+it.width:]) & it.addrMask)
			// it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+(it.blockIdx+1)*4:]))
		} else {
			it.nextBlock = it.footerAddr
		}
	}
	return it.seekInBlock(x)
}

func (it *roaringBitmapPostings) Err() error {
	return nil
}

// Read key of the block starting from off.
// func (it *roaringBitmapPostings) readKey(off int) uint32 {
// 	key := uint32(0)
// 	for i := 0; i < 4 - it.valueSize; i ++ {
// 		key = (key << 8) + uint32(it.bs[off+i])
// 	}
// 	return key
// }

func (it *roaringBitmapPostings) readBytes(off int) int {
	val := 0
	for i := 0; i < it.width; i++ {
		val = (val << 8) | int(it.bs[off+i])
	}
	return val
}

func (it *roaringBitmapPostings) readByte(idx int, count uint8) byte {
	if count == 0 {
		return it.bs[idx]
	}
	byt := it.bs[idx] << count
	byt |= it.bs[idx+1] >> (8 - count)

	return byt
}

func (it *roaringBitmapPostings) readBits(offset int) uint64 {
	idx := offset >> 3
	count := uint8(offset % 8)
	nbits := it.width
	var u uint64

	for nbits >= 8 {
		byt := it.readByte(idx, count)

		u = (u << 8) | uint64(byt)
		nbits -= 8
		idx += 1
	}

	if nbits == 0 {
		return u
	}

	if nbits > int(8-count) {
		u = (u << uint(8-count)) | uint64((it.bs[idx]<<count)>>count)
		nbits -= int(8 - count)
		idx += 1

		count = 0
	}

	u = (u << uint(nbits)) | uint64((it.bs[idx]<<count)>>(8-uint(nbits)))
	return u
}

func writeRoaringBitmapBlock(e *encoding.Encbuf, vals []uint32, key uint32, thres int, bitmapSize int, valueSize int) {
	var offset int  // The starting offset of the bitmap of each block.
	var idx1 uint32 // The offset in the bitmap in current block in bytes.
	var idx2 uint32 // The offset in the current byte in the bitmap ([0,8)).
	e.PutUvarint32(key)
	if len(vals) > thres {
		e.PutByte(byte(1))
		offset = len(e.Get())
		for i := 0; i < bitmapSize; i++ {
			e.PutByte(byte(0))
		}
		for _, val := range vals {
			idx1 = val >> 3
			idx2 = val % 8
			e.B[uint32(offset)+idx1] |= 1 << uint(7-idx2)
		}
	} else {
		c := make([]byte, 4)
		e.PutByte(byte(0))
		for _, val := range vals {
			binary.BigEndian.PutUint32(c[:], val)
			for i := 4 - valueSize; i < 4; i++ {
				e.PutByte(c[i])
			}
		}
	}
}

func writeRoaringBitmapBlock64(e *encoding.Encbuf, vals []uint64, key uint64, thres int, bitmapSize int, valueSize int) {
	var offset int  // The starting offset of the bitmap of each block.
	var idx1 uint64 // The offset in the bitmap in current block in bytes.
	var idx2 uint64 // The offset in the current byte in the bitmap ([0,8)).
	e.PutUvarint64(key)
	if len(vals) > thres {
		e.PutByte(byte(1))
		offset = len(e.Get())
		for i := 0; i < bitmapSize; i++ {
			e.PutByte(byte(0))
		}
		for _, val := range vals {
			idx1 = val >> 3
			idx2 = val % 8
			e.B[uint64(offset)+idx1] |= 1 << uint(7-idx2)
		}
	} else {
		c := make([]byte, 8)
		e.PutByte(byte(0))
		for _, val := range vals {
			binary.BigEndian.PutUint64(c[:], val)
			for i := 8 - valueSize; i < 8; i++ {
				e.PutByte(c[i])
			}
		}
	}
}

func putBytes(e *encoding.Encbuf, val uint32, width int) {
	for i := width - 1; i >= 0; i-- {
		e.PutByte(byte((val >> (8 * uint(i)) & 0xff)))
	}
}

func writeRoaringBitmapPostings(e *encoding.Encbuf, arr []uint32) {
	key := uint32(0xffffffff)                   // The initial key should be unique.
	bitmapSize := 1 << (bitmapBits - 3)         // Bitmap size in bytes.
	valueSize := bitmapBits >> 3                // The size of the element in array in bytes.
	thres := (1 << bitmapBits) / bitmapBits     // Threshold of number of elements in the block for choosing encoding type.
	mask := uint32((1 << uint(bitmapBits)) - 1) // Mask for the elements in the block.
	var curKey uint32
	var curVal uint32
	var idx int               // Index of current element in arr.
	var startingOffs []uint32 // The starting offsets of each block.
	var vals []uint32            // The converted values in the current block.
	startOff := len(e.Get())
	e.PutBE32(0) // Footer starting offset.
	e.PutBE32(0) // Number of blocks.
	for idx < len(arr) {
		curKey = arr[idx] >> bitmapBits // Key of block.
		curVal = arr[idx] & mask        // Value inside block.
		if curKey != key {
			// Move to next block.
			if idx != 0 {
				startingOffs = append(startingOffs, uint32(len(e.B)))
				writeRoaringBitmapBlock(e, vals, key, thres, bitmapSize, valueSize)
				vals = vals[:0]
			}
			key = curKey
		}
		vals = append(vals, curVal)
		idx += 1
	}
	startingOffs = append(startingOffs, uint32(len(e.B)))
	writeRoaringBitmapBlock(e, vals, key, thres, bitmapSize, valueSize)

	// Put footer starting offset.
	binary.BigEndian.PutUint32(e.B[startOff:], uint32(len(e.B)-8-startOff))
	binary.BigEndian.PutUint32(e.B[startOff+4:], uint32(len(startingOffs)))
	width := bits.Len32(startingOffs[len(startingOffs)-1] - 8 - uint32(startOff))
	if width == 0 {
		// key 0 will result in 0 width.
		width += 1
	}
	// e.PutBE32(uint32(len(startingOffs))) // Number of blocks.
	// e.PutByte(byte(width))
	// for _, off := range startingOffs {
	// 	e.PutBits(uint64(off - 4 - uint32(startOff)), width)
	// }

	e.PutByte(byte((width + 7) / 8))
	for _, off := range startingOffs {
		putBytes(e, off-8-uint32(startOff), (width+7)/8)
	}

	// for _, off := range startingOffs {
	// 	e.PutBE32(off - 4 - uint32(startOff))
	// }
}

func writeRoaringBitmapPostings64(e *encoding.Encbuf, arr []uint64) {
	key := uint64(0xffffffffffffffff)           // The initial key should be unique.
	bitmapSize := 1 << (bitmapBits - 3)         // Bitmap size in bytes.
	valueSize := bitmapBits >> 3                // The size of the element in array in bytes.
	thres := (1 << bitmapBits) / bitmapBits     // Threshold of number of elements in the block for choosing encoding type.
	mask := (uint64(1) << uint(bitmapBits)) - 1 // Mask for the elements in the block.
	var curKey uint64
	var curVal uint64
	var idx int               // Index of current element in arr.
	var startingOffs []uint32 // The starting offsets of each block.
	var vals []uint64         // The converted values in the current block.
	startOff := len(e.Get())
	e.PutBE32(0) // Footer starting offset.
	e.PutBE32(0) // Number of blocks.
	for idx < len(arr) {
		curKey = arr[idx] >> bitmapBits // Key of block.
		curVal = arr[idx] & mask        // Value inside block.
		if curKey != key {
			// Move to next block.
			if idx != 0 {
				startingOffs = append(startingOffs, uint32(len(e.B)))
				writeRoaringBitmapBlock64(e, vals, key, thres, bitmapSize, valueSize)
				vals = vals[:0]
			}
			key = curKey
		}
		vals = append(vals, curVal)
		idx += 1
	}
	startingOffs = append(startingOffs, uint32(len(e.B)))
	writeRoaringBitmapBlock64(e, vals, key, thres, bitmapSize, valueSize)

	// Put footer starting offset.
	binary.BigEndian.PutUint32(e.B[startOff:], uint32(len(e.B)-8-startOff))
	binary.BigEndian.PutUint32(e.B[startOff+4:], uint32(len(startingOffs)))
	width := bits.Len32(startingOffs[len(startingOffs)-1] - 8 - uint32(startOff))
	if width == 0 {
		// key 0 will result in 0 width.
		width += 1
	}

	e.PutByte(byte((width + 7) / 8))
	for _, off := range startingOffs {
		putBytes(e, off-8-uint32(startOff), (width+7)/8)
	}
}

type baseDeltaBlock8Postings struct {
	bs         []byte
	cur        uint64
	inside     bool
	idx        int // The current offset inside the bs.
	footerAddr int
	key        uint64
	numBlock   int
	blockIdx   int
	nextBlock  int
	width      int
	prel       int
	addrMask   uint32
}

func newBaseDeltaBlock8Postings(bstream []byte) *baseDeltaBlock8Postings {
	if len(bstream) <= 4 {
		return nil
	}
	x := binary.BigEndian.Uint32(bstream)
	width := int(bstream[8+int(x)])
	return &baseDeltaBlock8Postings{bs: bstream[8:], numBlock: int(binary.BigEndian.Uint32(bstream[4:])), footerAddr: int(x), width: width, prel: 4 - width, addrMask: uint32((1 << (8 * uint(width))) - 1)}
}

func (it *baseDeltaBlock8Postings) At() uint64 {
	return it.cur
}

func (it *baseDeltaBlock8Postings) Next() bool {
	if it.inside { // Already entered the block.
		if it.idx < it.nextBlock {
			it.cur = it.key | uint64(it.bs[it.idx])
			it.idx += 1
			return true
		}
		it.blockIdx += 1
		it.inside = false
		return it.Next()
	} else { // Not yet entered the block.
		if it.idx < it.footerAddr {
			val, size := binary.Uvarint(it.bs[it.idx:])
			it.key = val << bitmapBits
			it.idx += size
			it.inside = true
			if it.blockIdx != it.numBlock-1 {
				it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-it.prel:]) & it.addrMask)
			} else {
				it.nextBlock = it.footerAddr
			}
			it.cur = it.key | uint64(it.bs[it.idx])
			it.idx += 1
			return true
		} else {
			return false
		}
	}
}

func (it *baseDeltaBlock8Postings) seekInBlock(x uint64) bool {
	curVal := byte(x & rbpValueMask)
	num := it.nextBlock - it.idx
	j := sort.Search(num, func(i int) bool {
		return it.bs[it.idx+i] >= curVal
	})
	if j == num {
		// Fast-path to the next block.
		// The first element in next block should be >= x.
		it.idx = it.nextBlock
		it.blockIdx += 1
		if it.idx < it.footerAddr {
			val, size := binary.Uvarint(it.bs[it.idx:])
			it.key = val << bitmapBits
			it.idx += size
			it.inside = true
			if it.blockIdx != it.numBlock-1 {
				it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-it.prel:]) & it.addrMask)
			} else {
				it.nextBlock = it.footerAddr
			}
			it.cur = it.key | uint64(it.bs[it.idx])
			it.idx += 1
			return true
		} else {
			return false
		}
	}
	it.cur = it.key | uint64(it.bs[it.idx+j])
	it.idx += j + 1
	return true
}

func (it *baseDeltaBlock8Postings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}
	curKey := x >> bitmapBits
	if it.inside && it.key>>bitmapBits == curKey {
		// Fast path.
		return it.seekInBlock(x)
	} else {
		i := sort.Search(it.numBlock-it.blockIdx, func(i int) bool {
			off := int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+i)*it.width-it.prel:]) & it.addrMask)
			k, _ := binary.Uvarint(it.bs[off:])
			return k >= curKey
		})
		if i == it.numBlock-it.blockIdx {
			return false
		}
		it.blockIdx += i
		if i != 0 { // i > 0.
			it.inside = false
			it.idx = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+it.blockIdx*it.width-it.prel:]) & it.addrMask)
		}
	}
	val, size := binary.Uvarint(it.bs[it.idx:])
	it.key = val << bitmapBits
	it.idx += size
	it.inside = true

	if it.blockIdx != it.numBlock-1 {
		it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-it.prel:]) & it.addrMask)
	} else {
		it.nextBlock = it.footerAddr
	}
	return it.seekInBlock(x)
}

func (it *baseDeltaBlock8Postings) Err() error {
	return nil
}

func writeBaseDelta8Block(e *encoding.Encbuf, vals []uint32, key uint32, valueSize int) {
	e.PutUvarint32(key)
	c := make([]byte, 4)
	for _, val := range vals {
		binary.BigEndian.PutUint32(c[:], val)
		for i := 4 - valueSize; i < 4; i++ {
			e.PutByte(c[i])
		}
	}
}

func writeBaseDeltaBlock8Postings(e *encoding.Encbuf, arr []uint32) {
	key := uint32(0xffffffff)                   // The initial key should be unique.
	valueSize := bitmapBits >> 3                // The size of the element in array in bytes.
	mask := uint32((1 << uint(bitmapBits)) - 1) // Mask for the elements in the block.
	var curKey uint32
	var curVal uint32
	var idx int               // Index of current element in arr.
	var startingOffs []uint32 // The starting offsets of each block.
	var vals []uint32         // The converted values in the current block.
	startOff := len(e.Get())
	e.PutBE32(0) // Footer starting offset.
	e.PutBE32(0) // Number of blocks.
	for idx < len(arr) {
		curKey = arr[idx] >> bitmapBits // Key of block.
		curVal = arr[idx] & mask        // Value inside block.
		if curKey != key {
			// Move to next block.
			if idx != 0 {
				startingOffs = append(startingOffs, uint32(len(e.B)))
				writeBaseDelta8Block(e, vals, key, valueSize)
				vals = vals[:0]
			}
			key = curKey
		}
		vals = append(vals, curVal)
		idx += 1
	}
	startingOffs = append(startingOffs, uint32(len(e.B)))
	writeBaseDelta8Block(e, vals, key, valueSize)

	// Put footer starting offset.
	binary.BigEndian.PutUint32(e.B[startOff:], uint32(len(e.B)-8-startOff))
	binary.BigEndian.PutUint32(e.B[startOff+4:], uint32(len(startingOffs)))
	width := bits.Len32(startingOffs[len(startingOffs)-1] - 8 - uint32(startOff))
	if width == 0 {
		// key 0 will result in 0 width.
		width += 1
	}

	e.PutByte(byte((width + 7) / 8))
	for _, off := range startingOffs {
		putBytes(e, off-8-uint32(startOff), (width+7)/8)
	}
}

type baseDeltaBlock16Postings struct {
	bs         []byte
	cur        uint64
	inside     bool
	idx        int // The current offset inside the bs.
	footerAddr int
	key        uint64
	numBlock   int
	blockIdx   int // The current block idx.
	nextBlock  int
}

func newBaseDeltaBlock16Postings(bstream []byte) *baseDeltaBlock16Postings {
	x := binary.BigEndian.Uint32(bstream) // Read the footer address.
	return &baseDeltaBlock16Postings{bs: bstream[8:], numBlock: int(binary.BigEndian.Uint32(bstream[4:])), footerAddr: int(x)}
}

func (it *baseDeltaBlock16Postings) At() uint64 {
	return it.cur
}

func (it *baseDeltaBlock16Postings) Next() bool {
	if it.inside { // Already entered the block.
		if it.idx < it.nextBlock {
			it.cur = it.key | uint64(binary.BigEndian.Uint16(it.bs[it.idx:]))
			it.idx += 2
			return true
		}
		it.blockIdx += 1 // Go to the next block.
	}
	// Currently not entered any block.
	if it.idx < it.footerAddr {
		it.key = binary.BigEndian.Uint64(it.bs[it.idx:])
		it.idx += 8
		it.inside = true
		it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+((it.blockIdx+1)<<2):]))
		it.cur = it.key | uint64(binary.BigEndian.Uint16(it.bs[it.idx:]))
		it.idx += 2
		return true
	} else {
		return false
	}
}

func (it *baseDeltaBlock16Postings) seekInBlock(x uint64) bool {
	curVal := x & 0xffff
	num := (it.nextBlock - it.idx) >> 1
	j := sort.Search(num, func(i int) bool {
		return uint64(binary.BigEndian.Uint16(it.bs[it.idx+(i<<1):])) >= curVal
	})
	if j == num {
		// Fast-path to the next block.
		// The first element in next block should be >= x.
		it.idx = it.nextBlock
		it.blockIdx += 1
		if it.idx < it.footerAddr {
			it.key = binary.BigEndian.Uint64(it.bs[it.idx:])
			it.idx += 8
			it.inside = true
			it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+((it.blockIdx+1)<<2):]))
			it.cur = it.key | uint64(binary.BigEndian.Uint16(it.bs[it.idx:]))
			it.idx += 2
			return true
		} else {
			return false
		}
	}
	it.cur = it.key | uint64(binary.BigEndian.Uint16(it.bs[it.idx+(j<<1):]))
	it.idx += (j + 1) << 1
	return true
}

func (it *baseDeltaBlock16Postings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}
	curKey := (x >> 16) << 16
	if it.inside && it.key == curKey {
		// Fast path for x in current block.
		return it.seekInBlock(x)
	} else {
		i := sort.Search(it.numBlock-it.blockIdx, func(i int) bool {
			off := int(binary.BigEndian.Uint32(it.bs[it.footerAddr+((it.blockIdx+i)<<2):]))
			// k, _ := binary.Uvarint(it.bs[off:])
			k := binary.BigEndian.Uint64(it.bs[off:])
			return k >= curKey
		})
		if i == it.numBlock-it.blockIdx {
			return false
		}
		it.blockIdx += i
		if i != 0 { // i > 0.
			it.inside = false
			it.idx = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+((it.blockIdx)<<2):]))
		}
	}
	it.key = binary.BigEndian.Uint64(it.bs[it.idx:])
	it.idx += 8

	it.inside = true

	it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+((it.blockIdx+1)<<2):]))
	return it.seekInBlock(x)
}

func (it *baseDeltaBlock16Postings) Err() error {
	return nil
}

func writeBaseDelta16Block(e *encoding.Encbuf, vals []uint32, key uint32, valueSize int) {
	e.PutBE64(uint64(key))
	c := make([]byte, 4)
	for _, val := range vals {
		binary.BigEndian.PutUint32(c[:], val)
		for i := 4 - valueSize; i < 4; i++ {
			e.PutByte(c[i])
		}
	}
}

func writeBaseDelta16Block64(e *encoding.Encbuf, vals []uint64, key uint64, valueSize int) {
	e.PutBE64(key)
	c := make([]byte, 8)
	for _, val := range vals {
		binary.BigEndian.PutUint64(c[:], val)
		for i := 8 - valueSize; i < 8; i++ {
			e.PutByte(c[i])
		}
	}
}

func writeBaseDeltaBlock16Postings(e *encoding.Encbuf, arr []uint32) int {
	key := uint32(0xffffffff)           // The initial key should be unique.
	valueSize := 16 >> 3                // The size of the element in array in bytes.
	mask := uint32((1 << uint(16)) - 1) // Mask for the elements in the block.
	invertedMask := ^mask
	var curKey uint32
	var curVal uint32
	var idx int               // Index of current element in arr.
	var startingOffs []uint32 // The starting offsets of each block.
	var vals []uint32         // The converted values in the current block.
	startOff := len(e.Get())
	e.PutBE32(0) // Footer starting offset.
	e.PutBE32(0) // Number of blocks.
	for idx < len(arr) {
		curKey = arr[idx] & invertedMask // Key of block.
		curVal = arr[idx] & mask         // Value inside block.
		if curKey != key {
			// Move to next block.
			if idx != 0 {
				startingOffs = append(startingOffs, uint32(len(e.B)))
				writeBaseDelta16Block(e, vals, key, valueSize)
				vals = vals[:0]
			}
			key = curKey
		}
		vals = append(vals, curVal)
		idx += 1
	}
	startingOffs = append(startingOffs, uint32(len(e.B)))
	writeBaseDelta16Block(e, vals, key, valueSize)
	startingOffs = append(startingOffs, uint32(len(e.B)))

	binary.BigEndian.PutUint32(e.B[startOff:], uint32(len(e.B)-8-startOff)) // Put footer starting offset.
	binary.BigEndian.PutUint32(e.B[startOff+4:], uint32(len(startingOffs)-1)) // Put number of blocks.
	for _, off := range startingOffs {
		e.PutBE32(off-8-uint32(startOff))
	}
	// e.PutUvarint32(startingOffs[0]-8-uint32(startOff))
	// width := bits.Len32(startingOffs[len(startingOffs)-1] - 4 - uint32(startOff))
	// if width == 0 {
	// 	// key 0 will result in 0 width.
	// 	width += 1
	// }
	// e.PutByte(byte((width + 7) / 8))
	// for _, off := range startingOffs {
	// 	putBytes(e, off - (startingOffs[len(startingOffs)-1] - 4 - uint32(startOff)), (width + 7) / 8)
	// }
	return len(startingOffs) - 1
}

func writeBaseDeltaBlock16Postings64(e *encoding.Encbuf, arr []uint64) {
	key := uint64(0xffffffff)           // The initial key should be unique.
	valueSize := 16 >> 3                // The size of the element in array in bytes.
	mask := uint64((1 << uint(16)) - 1) // Mask for the elements in the block.
	invertedMask := ^mask
	var curKey uint64
	var curVal uint64
	var idx int               // Index of current element in arr.
	var startingOffs []uint32 // The starting offsets of each block.
	var vals []uint64         // The converted values in the current block.
	startOff := len(e.Get())
	e.PutBE32(0) // Footer starting offset.
	e.PutBE32(0) // Number of blocks.
	for idx < len(arr) {
		curKey = arr[idx] & invertedMask // Key of block.
		curVal = arr[idx] & mask         // Value inside block.
		if curKey != key {
			// Move to next block.
			if idx != 0 {
				startingOffs = append(startingOffs, uint32(len(e.B)))
				writeBaseDelta16Block64(e, vals, key, valueSize)
				vals = vals[:0]
			}
			key = curKey
		}
		vals = append(vals, curVal)
		idx += 1
	}
	startingOffs = append(startingOffs, uint32(len(e.B)))
	writeBaseDelta16Block64(e, vals, key, valueSize)
	startingOffs = append(startingOffs, uint32(len(e.B)))

	binary.BigEndian.PutUint32(e.B[startOff:], uint32(len(e.B)-8-startOff)) // Put footer starting offset.
	binary.BigEndian.PutUint32(e.B[startOff+4:], uint32(len(startingOffs)-1)) // Put number of blocks.
	for _, off := range startingOffs {
		e.PutBE32(off-8-uint32(startOff))
	}
}

type baseDeltaBlock16PostingsV2 struct {
	bs         []byte
	cur        uint64
	inside     bool
	idx        int // The current offset inside the bs.
	footerAddr int
	base       uint64
	numBlock   int
	blockIdx   int // The current block idx.
	nextBlock  int
	width      int
	prel       int
	addrMask   uint32
}

func newBaseDeltaBlock16PostingsV2(bstream []byte) *baseDeltaBlock16PostingsV2 {
	x := binary.BigEndian.Uint32(bstream) // Read the footer address.
	width := int(bstream[8+int(x)])
	return &baseDeltaBlock16PostingsV2{bs: bstream[8:], numBlock: int(binary.BigEndian.Uint32(bstream[4:])), footerAddr: int(x), width: width, prel: 4 - width, addrMask: uint32((1 << (8 * uint(width))) - 1)}
}

func (it *baseDeltaBlock16PostingsV2) At() uint64 {
	return it.cur
}

func (it *baseDeltaBlock16PostingsV2) Next() bool {
	if it.inside { // Already entered the block.
		if it.idx < it.nextBlock {
			it.cur = it.base + uint64(binary.BigEndian.Uint16(it.bs[it.idx:]))
			it.idx += 2
			return true
		}
		it.blockIdx += 1 // Go to the next block.
	}
	// Currently not entered any block.
	if it.idx < it.footerAddr {
		val, size := binary.Uvarint(it.bs[it.idx:]) // Read the base.
		it.base = val
		it.idx += size
		it.inside = true
		if it.blockIdx != it.numBlock-1 {
			it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-it.prel:]) & it.addrMask)
		} else {
			it.nextBlock = it.footerAddr
		}
		it.cur = it.base
		return true
	} else {
		return false
	}
}

func (it *baseDeltaBlock16PostingsV2) seekInBlock(x uint64) bool {
	temp := x - it.base
	num := (it.nextBlock - it.idx) >> 1
	j := sort.Search(num, func(i int) bool {
		return uint64(binary.BigEndian.Uint16(it.bs[it.idx+(i<<1):])) >= temp
	})
	if j == num {
		// Fast-path to the next block.
		// The first element in next block should be >= x.
		it.idx = it.nextBlock
		it.blockIdx += 1
		if it.idx < it.footerAddr {
			val, size := binary.Uvarint(it.bs[it.idx:])
			it.base = val
			it.idx += size
			it.inside = true
			if it.blockIdx != it.numBlock-1 {
				it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-it.prel:]) & it.addrMask)
			} else {
				it.nextBlock = it.footerAddr
			}
			it.cur = it.base
			return true
		} else {
			return false
		}
	}
	it.cur = it.base + uint64(binary.BigEndian.Uint16(it.bs[it.idx+(j<<1):]))
	it.idx += (j + 1) << 1
	return true
}

func (it *baseDeltaBlock16PostingsV2) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}
	if it.inside && bits.Len64(x - it.base) <= 16 {
		// Fast path for x in current block.
		return it.seekInBlock(x)
	} else {
		i := sort.Search(it.numBlock-it.blockIdx, func(i int) bool {
			off := int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+i)*it.width-it.prel:]) & it.addrMask)
			k, _ := binary.Uvarint(it.bs[off:])
			return k > x
		})
		if i > 0 {
			i -= 1
		}
		it.blockIdx += i
		if i != 0 { // i > 0.
			it.inside = false
			it.idx = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+it.blockIdx*it.width-it.prel:]) & it.addrMask)
		}
	}
	val, size := binary.Uvarint(it.bs[it.idx:])
	it.base = val
	it.idx += size
	it.inside = true
	if it.blockIdx != it.numBlock-1 {
		it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-it.prel:]) & it.addrMask)
	} else {
		it.nextBlock = it.footerAddr
	}
	if it.base >= x {
		it.cur = it.base
		return true
	}

	// If the length of the diff larger than 16, directly go to the next block
	// because the first value of the next block should be >= x.
	if bits.Len64(x - val) > 16 {
		if it.blockIdx == it.numBlock-1 {
			return false
		} else {
			it.blockIdx += 1
			it.idx = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+it.blockIdx*it.width-it.prel:]) & it.addrMask)
			val, size := binary.Uvarint(it.bs[it.idx:])
			it.base = val
			it.idx += size
			it.inside = true
			if it.blockIdx != it.numBlock-1 {
				it.nextBlock = int(binary.BigEndian.Uint32(it.bs[it.footerAddr+1+(it.blockIdx+1)*it.width-it.prel:]) & it.addrMask)
			} else {
				it.nextBlock = it.footerAddr
			}
			it.cur = it.base + uint64(binary.BigEndian.Uint16(it.bs[it.idx:]))
			it.idx += 2
			return true
		}
	}
	return it.seekInBlock(x)
}

func (it *baseDeltaBlock16PostingsV2) Err() error {
	return nil
}

func writeBaseDelta16BlockV2(e *encoding.Encbuf, vals []uint32, base uint32) {
	e.PutUvarint32(base)
	c := make([]byte, 2)
	for _, val := range vals {
		binary.BigEndian.PutUint16(c[:], uint16(val))
		e.PutByte(c[0])
		e.PutByte(c[1])
	}
}

func writeBaseDeltaBlock16PostingsV2(e *encoding.Encbuf, arr []uint32) {
	var base uint32
	var idx int               // Index of current element in arr.
	var startingOffs []uint32 // The starting offsets of each block.
	var vals []uint32         // The converted values in the current block.
	startOff := len(e.Get())
	e.PutBE32(0) // Footer starting offset.
	e.PutBE32(0) // Number of blocks.
	base = arr[idx]
	idx += 1
	for idx < len(arr) {
		delta := arr[idx] - base
		if bits.Len32(delta) > 16 {
			startingOffs = append(startingOffs, uint32(len(e.B)))
			writeBaseDelta16BlockV2(e, vals, base)
			base = arr[idx]
			idx += 1
			vals = vals[:0]
			continue
		}
		vals = append(vals, delta)
		idx += 1
	}
	startingOffs = append(startingOffs, uint32(len(e.B)))
	writeBaseDelta16BlockV2(e, vals, base)

	binary.BigEndian.PutUint32(e.B[startOff:], uint32(len(e.B)-8-startOff)) // Put footer starting offset.
	binary.BigEndian.PutUint32(e.B[startOff+4:], uint32(len(startingOffs))) // Put number of blocks.
	width := bits.Len32(startingOffs[len(startingOffs)-1] - 8 - uint32(startOff))
	if width == 0 {
		// key 0 will result in 0 width.
		width += 1
	}

	e.PutByte(byte((width + 7) / 8))
	for _, off := range startingOffs {
		putBytes(e, off-8-uint32(startOff), (width+7)/8)
	}
}
