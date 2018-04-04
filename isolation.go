package tsdb

import "sync"

// IsolationState holds the isolation information.
type IsolationState struct {
	// We will ignore all writes above the max, or that are incomplete.
	maxWriteID       uint64
	incompleteWrites map[uint64]struct{}
	lowWaterMark     uint64 // Lowest of incompleteWrites/maxWriteId.
	isolation        *isolation

	// Doubly linked list of active reads.
	next *IsolationState
	prev *IsolationState
}

// Close closes the state.
func (i *IsolationState) Close() {
	i.isolation.readMtx.Lock()
	i.next.prev = i.prev
	i.prev.next = i.next
	i.isolation.readMtx.Unlock()
}

// isolation is the global isolation state.
type isolation struct {
	// Mutex for accessing writeLastId and writesOpen.
	writeMtx sync.Mutex
	// Each write is given an internal id.
	lastWriteID uint64
	// Which writes are currently in progress.
	writesOpen map[uint64]struct{}
	// Mutex for accessing readsOpen.
	// If taking both writeMtx and readMtx, take writeMtx first.
	readMtx sync.Mutex
	// All current in use isolationStates. This is a doubly-linked list.
	readsOpen *IsolationState
}

func newIsolation() *isolation {
	isoState := &IsolationState{}
	isoState.next = isoState
	isoState.prev = isoState

	return &isolation{
		writesOpen: map[uint64]struct{}{},
		readsOpen:  isoState,
	}
}

// lowWatermark returns the writeId below which
// we no longer need to track which writes were from
// which writeId.
func (i *isolation) lowWatermark() uint64 {
	i.writeMtx.Lock() // Take writeMtx first.
	defer i.writeMtx.Unlock()
	i.readMtx.Lock()
	defer i.readMtx.Unlock()
	if i.readsOpen.prev == i.readsOpen {
		return i.lastWriteID
	}
	return i.readsOpen.prev.lowWaterMark
}

// State returns an object used to control isolation
// between a query and writes. Must be closed when complete.
func (i *isolation) State() *IsolationState {
	i.writeMtx.Lock() // Take write mutex before read mutex.
	defer i.writeMtx.Unlock()
	isoState := &IsolationState{
		maxWriteID:       i.lastWriteID,
		lowWaterMark:     i.lastWriteID,
		incompleteWrites: make(map[uint64]struct{}, len(i.writesOpen)),
		isolation:        i,
	}
	for k := range i.writesOpen {
		isoState.incompleteWrites[k] = struct{}{}
		if k < isoState.lowWaterMark {
			isoState.lowWaterMark = k
		}
	}

	i.readMtx.Lock()
	defer i.readMtx.Unlock()
	isoState.prev = i.readsOpen
	isoState.next = i.readsOpen.next
	i.readsOpen.next.prev = isoState
	i.readsOpen.next = isoState
	return isoState
}

// newWriteID increments the transaction counter and returns a new transaction ID.
func (i *isolation) newWriteID() uint64 {
	i.writeMtx.Lock()
	i.lastWriteID++
	writeID := i.lastWriteID
	i.writesOpen[writeID] = struct{}{}
	i.writeMtx.Unlock()

	return writeID
}

func (i *isolation) closeWrite(writeID uint64) {
	i.writeMtx.Lock()
	delete(i.writesOpen, writeID)
	i.writeMtx.Unlock()
}

// The transactionID ring buffer.
type txRing struct {
	txIDs     []uint64
	txIDFirst int // Position of the first id in the ring.
	txIDCount int // How many ids in the ring.
}

func newTxRing(cap int) *txRing {
	return &txRing{
		txIDs: make([]uint64, cap),
	}
}

func (txr *txRing) add(writeID uint64) {
	if txr.txIDCount == len(txr.txIDs) {
		// Ring buffer is full, expand by doubling.
		newRing := make([]uint64, txr.txIDCount*2)
		idx := copy(newRing[:], txr.txIDs[txr.txIDFirst%len(txr.txIDs):])
		copy(newRing[idx:], txr.txIDs[:txr.txIDFirst%len(txr.txIDs)])
		txr.txIDs = newRing
		txr.txIDFirst = 0
	}

	txr.txIDs[(txr.txIDFirst+txr.txIDCount)%len(txr.txIDs)] = writeID
	txr.txIDCount++
}

func (txr *txRing) cleanupWriteIDsBelow(bound uint64) {
	pos := txr.txIDFirst

	for txr.txIDCount > 0 {
		if txr.txIDs[pos] < bound {
			txr.txIDFirst++
			txr.txIDCount--
		} else {
			break
		}

		pos++
		if pos == len(txr.txIDs) {
			pos = 0
		}
	}

	txr.txIDFirst = txr.txIDFirst % len(txr.txIDs)
}

// cutoffN will only keep the latest N transactions in the ring.
// Will also downsize the ring if possible.
func (txr *txRing) cutoffN(n int) {
	if txr.txIDCount <= n {
		return
	}

	txr.txIDFirst += (txr.txIDCount - n)
	txr.txIDCount = n

	newBufSize := len(txr.txIDs)
	for n < newBufSize/2 {
		newBufSize = newBufSize / 2
	}

	if newBufSize == len(txr.txIDs) {
		return
	}

	newRing := make([]uint64, newBufSize)
	idx := copy(newRing[:], txr.txIDs[txr.txIDFirst%len(txr.txIDs):])
	copy(newRing[idx:], txr.txIDs[:txr.txIDFirst%len(txr.txIDs)])

	txr.txIDs = newRing
	txr.txIDFirst = 0
}

func (txr *txRing) iterator() *txRingIterator {
	return &txRingIterator{
		pos: txr.txIDFirst % len(txr.txIDs),
		ids: txr.txIDs,
	}
}

// txRingIterator lets you iterate over the ring. It doesn't terminate,
// it DOESNT terminate.
type txRingIterator struct {
	ids []uint64

	pos int
}

func (it *txRingIterator) At() uint64 {
	return it.ids[it.pos]
}

func (it *txRingIterator) Next() {
	it.pos++
	if it.pos == len(it.ids) {
		it.pos = 0
	}
}
