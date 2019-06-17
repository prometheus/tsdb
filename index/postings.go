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

// 1 is bigEndian, 2 is baseDelta, 3 is deltaBlock, 4 is baseDeltaBlock, 5 is bitmapPostings.
const postingsType = 5

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
	bs   bitSlice
	base uint32
	size int
	idx  int
	cur  uint64
}

func newBaseDeltaPostings(bstream []byte, base uint32, width int, size int) *baseDeltaPostings {
	return &baseDeltaPostings{bs: bitSlice{bstream: bstream, width: width}, base: base, size: size, cur: uint64(base)}
}

func (it *baseDeltaPostings) At() uint64 {
	return it.cur
}

func (it *baseDeltaPostings) Next() bool {
	if it.size > it.idx {
		it.cur = it.bs.readBits(it.idx*it.bs.width) + uint64(it.base)
		it.idx += 1
		return true
	}
	return false
}

func (it *baseDeltaPostings) Seek(x uint64) bool {
	if it.cur >= x {
		return true
	}

	num := it.size - it.idx
	// Do binary search between current position and end.
	x -= uint64(it.base)
	i := sort.Search(num, func(i int) bool {
		return it.bs.readBits((i+it.idx)*it.bs.width) >= x
	})
	if i < num {
		it.cur = it.bs.readBits((i+it.idx)*it.bs.width) + uint64(it.base)
		it.idx += i
		return true
	}
	it.idx += i
	return false
}

func (it *baseDeltaPostings) Err() error {
	return nil
}

const deltaBlockSize = 128

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
// ┌────────────────┬───────────────┬─────────────────┬────────────┬────────────────┬─────┬────────────────┐
// │ base <uvarint> │ idx <uvarint> │ count <uvarint> │ width <1b> │ delta 1 <bits> │ ... │ delta n <bits> │
// └────────────────┴───────────────┴─────────────────┴────────────┴────────────────┴─────┴────────────────┘
type baseDeltaBlockPostings struct {
	bs       bitSlice
	size     int
	count    int // count in current block.
	idxBlock int
	idx      int
	offset   int // offset in bit.
	cur      uint64
	base     uint64
}

func newBaseDeltaBlockPostings(bstream []byte, size int) *baseDeltaBlockPostings {
	return &baseDeltaBlockPostings{bs: bitSlice{bstream: bstream}, size: size}
}

func (it *baseDeltaBlockPostings) GetOff() int {
	return it.offset
}
func (it *baseDeltaBlockPostings) GetWidth() int {
	return it.bs.width
}

func (it *baseDeltaBlockPostings) At() uint64 {
	return it.cur
}

func (it *baseDeltaBlockPostings) Next() bool {
	if it.offset >= len(it.bs.bstream)<<3 || it.idx >= it.size {
		return false
	}
	if it.offset%(deltaBlockSize<<3) == 0 {
		val, n := binary.Uvarint(it.bs.bstream[it.offset>>3:])
		if n < 1 {
			return false
		}
		it.cur = val
		it.base = val
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

	it.cur = it.bs.readBits(it.offset) + it.base
	it.offset += it.bs.width
	it.idx += 1
	it.idxBlock += 1
	if it.idxBlock == it.count {
		it.offset = ((it.offset-1)/(deltaBlockSize<<3) + 1) * deltaBlockSize << 3
	}
	return true
}

func (it *baseDeltaBlockPostings) Seek(x uint64) bool {
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

	// Read base, idx, and width.
	it.Next()
	if x <= it.base {
		return true
	} else {
		temp := x - it.base
		j := sort.Search(it.count-it.idxBlock, func(i int) bool {
			return it.bs.readBits(it.offset+i*it.bs.width) >= temp
		})

		if j < it.count-it.idxBlock {
			it.offset += j * it.bs.width
			it.cur = it.bs.readBits(it.offset) + it.base
			it.offset += it.bs.width
			it.idxBlock += j + 1
			it.idx += j + 1
			if it.idxBlock == it.count {
				it.offset = ((it.offset-1)/(deltaBlockSize<<3) + 1) * deltaBlockSize << 3
			}
		} else {
			it.offset = (startOff + (i+1)*deltaBlockSize) << 3
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
		e.PutUvarint32(arr[i])    // Put base.
		e.PutUvarint64(uint64(i)) // Put idx.
		remaining = (deltaBlockSize - (len(e.B)-startLen)%deltaBlockSize - 1) << 3
		deltas = deltas[:0]
		base = arr[i]
		max = -1
		i += 1
		for i < len(arr) {
			delta := arr[i] - base
			cur := bits.Len32(delta)
			if remaining-cur*(len(deltas)+1)-(((bits.Len(uint(len(deltas)))>>3)+1)<<3) >= 0 {
				deltas = append(deltas, delta)
				max = cur
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
