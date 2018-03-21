package mutex

import (
	"runtime"
	"sync/atomic"
)

// Atomic is a mutual exclusion atomic lock.
// The zero value for a Mutex is an unlocked mutex.
//
// A Mutex must not be copied after first use.
type Atomic struct {
	state uint32
}

// Lock locks m.
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *Atomic) Lock() {
	for !atomic.CompareAndSwapUint32(&m.state, 0, 1) {
		runtime.Gosched() //without this it locks up on GOMAXPROCS > 1
	}
}

// Unlock unlocks m.
func (m *Atomic) Unlock() {
	atomic.StoreUint32(&m.state, 0)
}
