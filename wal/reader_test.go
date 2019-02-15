package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/tsdb/testutil"
)

type reader interface {
	Next() bool
	Err() error
	Record() []byte
	Offset() int64
}

type record struct {
	t recType
	b []byte
}

var readerConstructors = map[string]func(io.Reader) reader{
	"Reader": func(r io.Reader) reader {
		return NewReader(r)
	},
	"LiveReader": func(r io.Reader) reader {
		lr := NewLiveReader(r)
		lr.eofNonErr = true
		return lr
	},
}

var data = make([]byte, 100000)
var testReaderCases = []struct {
	t    []record
	exp  [][]byte
	fail bool
}{
	// Sequence of valid records.
	{
		t: []record{
			{recFull, data[0:200]},
			{recFirst, data[200:300]},
			{recLast, data[300:400]},
			{recFirst, data[400:800]},
			{recMiddle, data[800:900]},
			{recPageTerm, make([]byte, pageSize-900-recordHeaderSize*5-1)}, // exactly lines up with page boundary.
			{recLast, data[900:900]},
			{recFirst, data[900:1000]},
			{recMiddle, data[1000:1200]},
			{recMiddle, data[1200:30000]},
			{recMiddle, data[30000:30001]},
			{recMiddle, data[30001:30001]},
			{recLast, data[30001:32000]},
		},
		exp: [][]byte{
			data[0:200],
			data[200:400],
			data[400:900],
			data[900:32000],
		},
	},
	// Exactly at the limit of one page minus the header size
	{
		t: []record{
			{recFull, data[0 : pageSize-recordHeaderSize]},
		},
		exp: [][]byte{
			data[:pageSize-recordHeaderSize],
		},
	},
	// More than a full page, this exceeds our buffer and can never happen
	// when written by the WAL.
	{
		t: []record{
			{recFull, data[0 : pageSize+1]},
		},
		fail: true,
	},
	// Two records the together are too big for a page.
	// NB currently the non-live reader succeeds on this. I think this is a bug.
	{
		t: []record{
			{recFull, data[:pageSize/2]},
			{recFull, data[:pageSize/2]},
		},
		exp: [][]byte{
			data[:pageSize/2],
		},
		fail: true,
	},
	// Invalid orders of record types.
	{
		t:    []record{{recMiddle, data[:200]}},
		fail: true,
	},
	{
		t:    []record{{recLast, data[:200]}},
		fail: true,
	},
	{
		t: []record{
			{recFirst, data[:200]},
			{recFull, data[200:400]},
		},
		fail: true,
	},
	{
		t: []record{
			{recFirst, data[:100]},
			{recMiddle, data[100:200]},
			{recFull, data[200:400]},
		},
		fail: true,
	},
	// Non-zero data after page termination.
	{
		t: []record{
			{recFull, data[:100]},
			{recPageTerm, append(make([]byte, pageSize-recordHeaderSize-102), 1)},
		},
		exp:  [][]byte{data[:100]},
		fail: true,
	},
}

func encodedRecord(t recType, b []byte) []byte {
	if t == recPageTerm {
		return append([]byte{0}, b...)
	}
	r := make([]byte, recordHeaderSize)
	r[0] = byte(t)
	binary.BigEndian.PutUint16(r[1:], uint16(len(b)))
	binary.BigEndian.PutUint32(r[3:], crc32.Checksum(b, castagnoliTable))
	return append(r, b...)
}

// TestReader feeds the reader a stream of encoded records with different types.
func TestReader(t *testing.T) {
	for name, fn := range readerConstructors {
		for i, c := range testReaderCases {
			t.Run(fmt.Sprintf("%s/%d", name, i), func(t *testing.T) {
				var buf []byte
				for _, r := range c.t {
					buf = append(buf, encodedRecord(r.t, r.b)...)
				}
				r := fn(bytes.NewReader(buf))

				for j := 0; r.Next(); j++ {
					t.Logf("record %d", j)
					rec := r.Record()

					if j >= len(c.exp) {
						t.Fatal("received more records than expected")
					}
					testutil.Equals(t, c.exp[j], rec, "Bytes within record did not match expected Bytes")
				}
				if !c.fail && r.Err() != nil {
					t.Fatalf("unexpected error: %s", r.Err())
				}
				if c.fail && r.Err() == nil {
					t.Fatalf("expected error but got none")
				}
			})
		}
	}
}

func TestReader_Live(t *testing.T) {
	for i := range testReaderCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			writeFd, err := ioutil.TempFile("", "TestReader_Live")
			testutil.Ok(t, err)
			defer os.Remove(writeFd.Name())

			go func(i int) {
				for _, rec := range testReaderCases[i].t {
					rec := encodedRecord(rec.t, rec.b)
					_, err := writeFd.Write(rec)
					testutil.Ok(t, err)
					runtime.Gosched()
				}
				writeFd.Close()
			}(i)

			// Read from a second FD on the same file.
			readFd, err := os.Open(writeFd.Name())
			testutil.Ok(t, err)
			reader := NewLiveReader(readFd)
			for _, exp := range testReaderCases[i].exp {
				for !reader.Next() {
					testutil.Assert(t, reader.Err() == io.EOF, "expect EOF, got: %v", reader.Err())
					runtime.Gosched()
				}

				actual := reader.Record()
				testutil.Equals(t, exp, actual, "read wrong record")
			}

			testutil.Assert(t, !reader.Next(), "unexpected record")
			if testReaderCases[i].fail {
				testutil.Assert(t, reader.Err() != nil, "expected error")
			}
		})
	}
}

func TestWAL_FuzzWriteRead_Live(t *testing.T) {
	dir, err := ioutil.TempDir("", "wal_fuzz_live")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	w, err := NewSize(nil, nil, dir, 128*pageSize)
	testutil.Ok(t, err)

	// In the background, generate a stream of random records and write them
	// to the WAL.
	const count = 1000
	input := make(chan []byte, count) // buffering required as we sometimes batch WAL writes.
	done := make(chan struct{})
	go func() {
		var recs [][]byte
		for i := 0; i < count; i++ {
			var sz int64
			switch i % 5 {
			case 0, 1:
				sz = 50
			case 2, 3:
				sz = pageSize
			default:
				sz = pageSize * 8
			}

			rec := make([]byte, rand.Int63n(sz))
			_, err := rand.Read(rec)
			testutil.Ok(t, err)

			input <- rec

			// Randomly batch up records.
			recs = append(recs, rec)
			if rand.Intn(4) < 3 {
				testutil.Ok(t, w.Log(recs...))
				recs = recs[:0]
			}
		}
		testutil.Ok(t, w.Log(recs...))
		time.Sleep(100 * time.Millisecond)
		close(done)
	}()

	// Tail the WAL and compare the results.
	m, _, err := w.Segments()
	testutil.Ok(t, err)

	seg, err := OpenReadSegment(SegmentName(dir, m))
	testutil.Ok(t, err)

	r := NewLiveReader(seg)
	segmentTicker := time.NewTicker(100 * time.Millisecond)
	readTicker := time.NewTicker(10 * time.Millisecond)

	readSegment := func(r *LiveReader) bool {
		for r.Next() {
			rec := r.Record()
			expected, ok := <-input
			testutil.Assert(t, ok, "unexpected record")
			testutil.Assert(t, bytes.Equal(expected, rec), "record does not match expected")
		}
		testutil.Assert(t, r.Err() == io.EOF, "expected EOF, got: %v", r.Err())
		return true
	}

outer:
	for {
		select {
		case <-segmentTicker.C:
			// check if new segments exist
			_, last, err := w.Segments()
			testutil.Ok(t, err)
			if last <= seg.i {
				continue
			}

			// read to end of segment.
			readSegment(r)

			fi, err := os.Stat(SegmentName(dir, seg.i))
			testutil.Ok(t, err)
			testutil.Assert(t, r.Offset() == fi.Size(), "expected to have read whole segment, but read %d of %d", r.Offset(), fi.Size())

			seg, err = OpenReadSegment(SegmentName(dir, seg.i+1))
			testutil.Ok(t, err)
			r = NewLiveReader(seg)

		case <-readTicker.C:
			readSegment(r)

		case <-done:
			readSegment(r)
			break outer
		}
	}

	testutil.Assert(t, r.Err() == io.EOF, "expected EOF")
}

func TestLiveReaderCorrupt_ShortFile(t *testing.T) {
	// Write a corrupt WAL segment, there is one record of pageSize in length,
	// but the segment is only half written.
	dir, err := ioutil.TempDir("", "wal_live_corrupt")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	w, err := NewSize(nil, nil, dir, pageSize)
	testutil.Ok(t, err)

	rec := make([]byte, pageSize-recordHeaderSize)
	_, err = rand.Read(rec)
	testutil.Ok(t, err)

	err = w.Log(rec)
	testutil.Ok(t, err)

	err = w.Close()
	testutil.Ok(t, err)

	segmentFile, err := os.OpenFile(filepath.Join(dir, "00000000"), os.O_RDWR, 0666)
	testutil.Ok(t, err)

	err = segmentFile.Truncate(pageSize / 2)
	testutil.Ok(t, err)

	err = segmentFile.Close()
	testutil.Ok(t, err)

	// Try and LiveReader it.
	m, _, err := w.Segments()
	testutil.Ok(t, err)

	seg, err := OpenReadSegment(SegmentName(dir, m))
	testutil.Ok(t, err)

	r := NewLiveReader(seg)
	testutil.Assert(t, r.Next() == false, "expected no records")
	testutil.Assert(t, r.Err() == io.EOF, "expected error, got: %v", r.Err())
}

func TestLiveReaderCorrupt_RecordTooLongAndShort(t *testing.T) {
	// Write a corrupt WAL segment, when record len > page size.
	dir, err := ioutil.TempDir("", "wal_live_corrupt")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	w, err := NewSize(nil, nil, dir, pageSize*2)
	testutil.Ok(t, err)

	rec := make([]byte, pageSize-recordHeaderSize)
	_, err = rand.Read(rec)
	testutil.Ok(t, err)

	err = w.Log(rec)
	testutil.Ok(t, err)

	err = w.Close()
	testutil.Ok(t, err)

	segmentFile, err := os.OpenFile(filepath.Join(dir, "00000000"), os.O_RDWR, 0666)
	testutil.Ok(t, err)

	// Override the record length
	buf := make([]byte, 3)
	buf[0] = byte(recFull)
	binary.BigEndian.PutUint16(buf[1:], 0xFFFF)
	_, err = segmentFile.WriteAt(buf, 0)
	testutil.Ok(t, err)

	err = segmentFile.Close()
	testutil.Ok(t, err)

	// Try and LiveReader it.
	m, _, err := w.Segments()
	testutil.Ok(t, err)

	seg, err := OpenReadSegment(SegmentName(dir, m))
	testutil.Ok(t, err)

	r := NewLiveReader(seg)
	testutil.Assert(t, r.Next() == false, "expected no records")
	testutil.Assert(t, r.Err().Error() == "record would overflow current page: 65542 > 32768", "expected error, got: %v", r.Err())
}
