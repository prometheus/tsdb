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
	"os"
	"path"
	"testing"
	"time"

	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
	"github.com/prometheus/tsdb/wal"
)

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
