package tsdb

import (
	"container/list"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type errHandler interface {
	handleError(err error)
}

type errorHandler struct {
	mtx         *sync.Mutex
	wgr         *sync.WaitGroup
	handlers    *list.List
	fatal_error int32
}

var (
	once sync.Once
	errh *errorHandler
)

func getErrorHandler() *errorHandler {
	once.Do(func() {
		errh = &errorHandler{mtx: &sync.Mutex{}, wgr: &sync.WaitGroup{}, handlers: list.New()}
	})
	return errh
}

func (erh *errorHandler) add(handler errHandler) {
	erh.mtx.Lock()
	defer erh.mtx.Unlock()
	for e := erh.handlers.Front(); e != nil; e = e.Next() {
		if e.Value == handler {
			return
		}
	}
	erh.handlers.PushBack(handler)
}

func (erh *errorHandler) errorHandled() {
	erh.wgr.Done()
}

func (erh *errorHandler) handleError(err error) {
	if atomic.AddInt32(&errh.fatal_error, 1) != 1 {
		return
	}
	go func() {
		go func() {
			time.Sleep(15 * time.Second)
			erh.wgr.Add(-erh.handlers.Len())
		}()
		for e := erh.handlers.Front(); e != nil; e = e.Next() {
			erh.wgr.Add(1)
			go func(eh errHandler) {
				eh.handleError(err)
			}(e.Value.(errHandler))
		}
		erh.wgr.Wait()
		os.Exit(1)
	}()
}

func (erh *errorHandler) getFatalErrorCount() int32 {
	return atomic.LoadInt32(&erh.fatal_error)
}
