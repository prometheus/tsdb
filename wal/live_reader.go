package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	readerCorruptionErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_reader_corruption_errors",
		Help: "Errors encountered when reading the WAL.",
	}, []string{"error"})
)

// NewLiveReader returns a new live reader.
func NewLiveReader(logger log.Logger, r io.Reader) *LiveReader {
	return &LiveReader{
		logger: logger,
		rdr:    r,

		// Until we understand how they come about, make readers permissive
		// to records spanning pages.
		permissive: true,
	}
}

// LiveReader reads WAL records from an io.Reader. It allows reading of WALs
// that are still in the process of being written, and returns records as soon
// as they can be read.
type LiveReader struct {
	logger     log.Logger
	rdr        io.Reader
	err        error
	rec        []byte
	hdr        [recordHeaderSize]byte
	buf        [pageSize]byte
	readIndex  int   // Index in buf to start at for next read.
	writeIndex int   // Index in buf to start at for next write.
	total      int64 // Total bytes processed during reading in calls to Next().
	index      int   // Used to track partial records, should be 0 at the start of every new record.

	// For testing, we can treat EOF as a non-error.
	eofNonErr bool

	// We sometime see records span page boundaries.  Should never happen, but it
	// does.  Until we track down why, set permissive to true to tolerate it.
	// NB the non-ive Reader implementation allows for this.
	permissive bool
}

// Err returns any errors encountered reading the WAL.  io.EOFs are not terminal
// and Next can be tried again.  Non-EOFs are terminal, and the reader should
// not be used again.  It is up to the user to decide when to stop trying should
// io.EOF be returned.
func (r *LiveReader) Err() error {
	if r.eofNonErr && r.err == io.EOF {
		return nil
	}
	return r.err
}

// Offset returns the number of bytes consumed from this segment.
func (r *LiveReader) Offset() int64 {
	return r.total
}

func (r *LiveReader) fillBuffer() (int, error) {
	n, err := r.rdr.Read(r.buf[r.writeIndex:len(r.buf)])
	r.writeIndex += n
	return n, err
}

// Next returns true if Record() will contain a full record.
// If Next returns false, you should always checked the contents of Error().
// Return false guarantees there are no more records if the segment is closed
// and not corrupt, otherwise if Err() == io.EOF you should try again when more
// data has been written.
func (r *LiveReader) Next() bool {
	for {
		// If buildRecord returns a non-EOF error, its game up - the segment is
		// corrupt. If buildRecord returns an EOF, we try and read more. If
		// that fails to read anything (n=0 && err=EOF), we return EOF and
		// the user can try again later.
		// If we have a full page, buildRecord is guaranteed to return a record
		// or a non-EOF; it has checks the records fit in pages.
		if ok, err := r.buildRecord(); ok {
			return true
		} else if err != nil && err != io.EOF {
			r.err = err
			return false
		}

		// If we've filled the page and not found a record, this
		// means records have started to span pages.  Shouldn't happen
		// but does and until we found out why, we need to deal with this.
		if r.permissive && r.writeIndex == pageSize && r.readIndex > 0 {
			copy(r.buf[r.readIndex:], r.buf[:])
			r.writeIndex -= r.readIndex
			r.readIndex = 0
			continue
		}

		if r.readIndex == pageSize {
			r.writeIndex = 0
			r.readIndex = 0
		}

		if r.writeIndex != pageSize {
			n, err := r.fillBuffer()
			if n == 0 || (err != nil && err != io.EOF) {
				r.err = err
				return false
			}
		}
	}
}

// Record returns the current record.
// The returned byte slice is only valid until the next call to Next.
func (r *LiveReader) Record() []byte {
	return r.rec
}

// Rebuild a full record from potentially partial records. Returns false
// if there was an error or if we weren't able to read a record for any reason.
// Returns true if we read a full record. Any record data is appended to
// LiveReader.rec
func (r *LiveReader) buildRecord() (bool, error) {
	for {
		// Check that we have data in the internal buffer to read.
		if r.writeIndex <= r.readIndex {
			return false, nil
		}

		// Attempt to read a record, partial or otherwise.
		temp, n, err := readRecord(r.logger, r.buf[:], r.readIndex, r.writeIndex, r.hdr[:], r.permissive)
		if err != nil {
			return false, err
		}

		r.readIndex += n
		r.total += int64(n)
		if temp == nil {
			return false, nil
		}

		rt := recType(r.hdr[0])
		if rt == recFirst || rt == recFull {
			r.rec = r.rec[:0]
		}
		r.rec = append(r.rec, temp...)

		if err := validateRecord(rt, r.index); err != nil {
			r.index = 0
			return false, err
		}
		if rt == recLast || rt == recFull {
			r.index = 0
			return true, nil
		}
		// Only increment i for non-zero records since we use it
		// to determine valid content record sequences.
		r.index++
	}
}

// Returns an error if the recType and i indicate an invalid record sequence.
// As an example, if i is > 0 because we've read some amount of a partial record
// (recFirst, recMiddle, etc. but not recLast) and then we get another recFirst or recFull
// instead of a recLast or recMiddle we would have an invalid record.
func validateRecord(typ recType, i int) error {
	switch typ {
	case recFull:
		if i != 0 {
			return errors.New("unexpected full record")
		}
		return nil
	case recFirst:
		if i != 0 {
			return errors.New("unexpected first record, dropping buffer")
		}
		return nil
	case recMiddle:
		if i == 0 {
			return errors.New("unexpected middle record, dropping buffer")
		}
		return nil
	case recLast:
		if i == 0 {
			return errors.New("unexpected last record, dropping buffer")
		}
		return nil
	default:
		return errors.Errorf("unexpected record type %d", typ)
	}
}

// Read a sub-record (see recType) from the buffer. It could potentially
// be a full record (recFull) if the record fits within the bounds of a single page.
// Returns a byte slice of the record data read, the number of bytes read, and an error
// if there's a non-zero byte in a page term record or the record checksum fails.
// This is a non-method function to make it clear it does not mutate the reader.
func readRecord(logger log.Logger, buf []byte, start, end int, header []byte, permissive bool) ([]byte, int, error) {
	// Special case: for recPageTerm, check that are all zeros to end of page,
	// consume them but don't return them.
	if buf[start] == byte(recPageTerm) {
		if end != pageSize {
			return nil, 0, io.EOF
		}

		for i := start; i < end; i++ {
			if buf[i] != 0 {
				return nil, 0, errors.New("unexpected non-zero byte in page term bytes")
			}
		}

		return nil, end - start, nil
	}

	// Not a recPageTerm; read the record and check the checksum.
	if end-start < recordHeaderSize {
		return nil, 0, io.EOF
	}

	copy(header, buf[start:start+recordHeaderSize])
	length := int(binary.BigEndian.Uint16(header[1:]))
	crc := binary.BigEndian.Uint32(header[3:])
	if start+recordHeaderSize+length > pageSize {
		if !permissive {
			return nil, 0, fmt.Errorf("record would overflow current page: %d > %d", start+recordHeaderSize+length, pageSize)
		}
		readerCorruptionErrors.WithLabelValues("record_span_page").Inc()
		level.Warn(logger).Log("msg", "record spans page boundaries", "start", start, "end", recordHeaderSize+length, "pageSize", pageSize)
	}
	if recordHeaderSize+length > pageSize {
		return nil, 0, fmt.Errorf("record length greater than a single page: %d > %d", recordHeaderSize+length, pageSize)
	}
	if start+recordHeaderSize+length > end {
		return nil, 0, io.EOF
	}

	rec := buf[start+recordHeaderSize : start+recordHeaderSize+length]
	if c := crc32.Checksum(rec, castagnoliTable); c != crc {
		return nil, 0, errors.Errorf("unexpected checksum %x, expected %x", c, crc)
	}

	return rec, length + recordHeaderSize, nil
}

func min(i, j int) int {
	if i < j {
		return i
	}
	return j
}
