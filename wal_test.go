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

package tsdb

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
	"github.com/prometheus/tsdb/wal"
)

// Symmetrical test of reading and writing to the WAL via its main interface.
func TestSegmentWAL_Log_Restore(t *testing.T) {
	const (
		numMetrics = 50
		iterations = 5
		stepSize   = 5
	)
	// Generate testing data. It does not make semantical sense but
	// for the purpose of this test.
	series, err := labels.ReadLabels(filepath.Join("testdata", "20kseries.json"), numMetrics)
	testutil.Ok(t, err)

	dir, err := ioutil.TempDir("", "test_wal_log_restore")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	var (
		recordedSeries  [][]RefSeries
		recordedSamples [][]RefSample
		recordedDeletes [][]Stone
	)
	var totalSamples int

	// Open WAL a bunch of times, validate all previous data can be read,
	// write more data to it, close it.
	for k := 0; k < numMetrics; k += numMetrics / iterations {
		w, err := OpenSegmentWAL(dir, nil, 0, nil)
		testutil.Ok(t, err)

		// Set smaller segment size so we can actually write several files.
		w.segmentSize = 1000 * 1000

		r := w.Reader()

		var (
			resultSeries  [][]RefSeries
			resultSamples [][]RefSample
			resultDeletes [][]Stone
		)

		serf := func(series []RefSeries) {
			if len(series) > 0 {
				clsets := make([]RefSeries, len(series))
				copy(clsets, series)
				resultSeries = append(resultSeries, clsets)
			}
		}
		smplf := func(smpls []RefSample) {
			if len(smpls) > 0 {
				csmpls := make([]RefSample, len(smpls))
				copy(csmpls, smpls)
				resultSamples = append(resultSamples, csmpls)
			}
		}

		delf := func(stones []Stone) {
			if len(stones) > 0 {
				cst := make([]Stone, len(stones))
				copy(cst, stones)
				resultDeletes = append(resultDeletes, cst)
			}
		}

		testutil.Ok(t, r.Read(serf, smplf, delf))

		testutil.Equals(t, recordedSamples, resultSamples)
		testutil.Equals(t, recordedSeries, resultSeries)
		testutil.Equals(t, recordedDeletes, resultDeletes)

		series := series[k : k+(numMetrics/iterations)]

		// Insert in batches and generate different amounts of samples for each.
		for i := 0; i < len(series); i += stepSize {
			var samples []RefSample
			var stones []Stone

			for j := 0; j < i*10; j++ {
				samples = append(samples, RefSample{
					Ref: uint64(j % 10000),
					T:   int64(j * 2),
					V:   rand.Float64(),
				})
			}

			for j := 0; j < i*20; j++ {
				ts := rand.Int63()
				stones = append(stones, Stone{rand.Uint64(), Intervals{{ts, ts + rand.Int63n(10000)}}})
			}

			lbls := series[i : i+stepSize]
			series := make([]RefSeries, 0, len(series))
			for j, l := range lbls {
				series = append(series, RefSeries{
					Ref:    uint64(i + j),
					Labels: l,
				})
			}

			testutil.Ok(t, w.LogSeries(series))
			testutil.Ok(t, w.LogSamples(samples))
			testutil.Ok(t, w.LogDeletes(stones))

			if len(lbls) > 0 {
				recordedSeries = append(recordedSeries, series)
			}
			if len(samples) > 0 {
				recordedSamples = append(recordedSamples, samples)
				totalSamples += len(samples)
			}
			if len(stones) > 0 {
				recordedDeletes = append(recordedDeletes, stones)
			}
		}

		testutil.Ok(t, w.Close())
	}
}

func TestMigrateWAL_Empty(t *testing.T) {
	// The migration proecedure must properly deal with a zero-length segment,
	// which is valid in the new format.
	dir, err := ioutil.TempDir("", "walmigrate")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	wdir := path.Join(dir, "wal")

	// Initialize empty WAL.
	w, err := wal.New(nil, nil, wdir)
	testutil.Ok(t, err)
	testutil.Ok(t, w.Close())

	testutil.Ok(t, MigrateWAL(nil, wdir))
}

func TestMigrateWAL_Fuzz(t *testing.T) {
	dir, err := ioutil.TempDir("", "walmigrate")
	testutil.Ok(t, err)
	defer os.RemoveAll(dir)

	wdir := path.Join(dir, "wal")

	// Should pass if no WAL exists yet.
	testutil.Ok(t, MigrateWAL(nil, wdir))

	oldWAL, err := OpenSegmentWAL(wdir, nil, time.Minute, nil)
	testutil.Ok(t, err)

	// Write some data.
	testutil.Ok(t, oldWAL.LogSeries([]RefSeries{
		{Ref: 100, Labels: labels.FromStrings("abc", "def", "123", "456")},
		{Ref: 1, Labels: labels.FromStrings("abc", "def2", "1234", "4567")},
	}))
	testutil.Ok(t, oldWAL.LogSamples([]RefSample{
		{Ref: 1, T: 100, V: 200},
		{Ref: 2, T: 300, V: 400},
	}))
	testutil.Ok(t, oldWAL.LogSeries([]RefSeries{
		{Ref: 200, Labels: labels.FromStrings("xyz", "def", "foo", "bar")},
	}))
	testutil.Ok(t, oldWAL.LogSamples([]RefSample{
		{Ref: 3, T: 100, V: 200},
		{Ref: 4, T: 300, V: 400},
	}))
	testutil.Ok(t, oldWAL.LogDeletes([]Stone{
		{ref: 1, intervals: []Interval{{100, 200}}},
	}))

	testutil.Ok(t, oldWAL.Close())

	// Perform migration.
	testutil.Ok(t, MigrateWAL(nil, wdir))

	w, err := wal.New(nil, nil, wdir)
	testutil.Ok(t, err)

	// We can properly write some new data after migration.
	var enc RecordEncoder
	testutil.Ok(t, w.Log(enc.Samples([]RefSample{
		{Ref: 500, T: 1, V: 1},
	}, nil)))

	testutil.Ok(t, w.Close())

	// Read back all data.
	sr, err := wal.NewSegmentsReader(wdir)
	testutil.Ok(t, err)

	r := wal.NewReader(sr)
	var res []interface{}
	var dec RecordDecoder

	for r.Next() {
		rec := r.Record()

		switch dec.Type(rec) {
		case RecordSeries:
			s, err := dec.Series(rec, nil)
			testutil.Ok(t, err)
			res = append(res, s)
		case RecordSamples:
			s, err := dec.Samples(rec, nil)
			testutil.Ok(t, err)
			res = append(res, s)
		case RecordTombstones:
			s, err := dec.Tombstones(rec, nil)
			testutil.Ok(t, err)
			res = append(res, s)
		default:
			t.Fatalf("unknown record type %d", dec.Type(rec))
		}
	}
	testutil.Ok(t, r.Err())

	testutil.Equals(t, []interface{}{
		[]RefSeries{
			{Ref: 100, Labels: labels.FromStrings("abc", "def", "123", "456")},
			{Ref: 1, Labels: labels.FromStrings("abc", "def2", "1234", "4567")},
		},
		[]RefSample{{Ref: 1, T: 100, V: 200}, {Ref: 2, T: 300, V: 400}},
		[]RefSeries{
			{Ref: 200, Labels: labels.FromStrings("xyz", "def", "foo", "bar")},
		},
		[]RefSample{{Ref: 3, T: 100, V: 200}, {Ref: 4, T: 300, V: 400}},
		[]Stone{{ref: 1, intervals: []Interval{{100, 200}}}},
		[]RefSample{{Ref: 500, T: 1, V: 1}},
	}, res)

	// Migrating an already migrated WAL shouldn't do anything.
	testutil.Ok(t, MigrateWAL(nil, wdir))
}
