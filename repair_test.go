// Copyright 2018 The Prometheus Authors
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
// limitations under the License.package tsdb

package tsdb

import (
	"os"
	"reflect"
	"testing"

	"github.com/prometheus/tsdb/chunks"

	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

func TestRepairBadIndexVersion(t *testing.T) {
	// The broken index used in this test was written by the following script
	// at a broken revision.
	//
	// func main() {
	// 	w, err := index.NewWriter("index")
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	err = w.AddSymbols(map[string]struct{}{
	// 		"a": struct{}{},
	// 		"b": struct{}{},
	// 		"1": struct{}{},
	// 		"2": struct{}{},
	// 	})
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	err = w.AddSeries(1, labels.FromStrings("a", "1", "b", "1"))
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	err = w.AddSeries(2, labels.FromStrings("a", "2", "b", "1"))
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	err = w.WritePostings("b", "1", index.NewListPostings([]uint64{1, 2}))
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	if err := w.Close(); err != nil {
	// 		panic(err)
	// 	}
	// }

	// In its current state, lookups should fail with the fixed code.
	const dir = "testdata/repair_index_version/01BZJ9WJQPWHGNC2W4J9TA62KC/"
	//lint:ignore SA4006 we expect error so no need to use result
	meta, err := readMetaFile(dir)
	if err == nil {
		t.Fatal("error expected but got none")
	}
	// Touch chunks dir in block.
	os.MkdirAll(dir+"chunks", 0777)

	r, err := index.NewFileReader(dir + "index")
	if err != nil {
		t.Fatal(err)
	}
	p, err := r.Postings("b", "1")
	if err != nil {
		t.Fatal(err)
	}
	for p.Next() {
		t.Logf("next ID %d", p.At())

		var lset labels.Labels
		if err := r.Series(p.At(), &lset, nil); err == nil {
			t.Fatal("expected error but got none")
		}
	}
	if p.Err() != nil {
		t.Fatal(err)
	}

	// On DB opening all blocks in the base dir should be repaired.
	db, err := Open("testdata/repair_index_version", nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	r, err = index.NewFileReader(dir + "index")
	if err != nil {
		t.Fatal(err)
	}
	p, err = r.Postings("b", "1")
	if err != nil {
		t.Fatal(err)
	}
	res := []labels.Labels{}

	for p.Next() {
		t.Logf("next ID %d", p.At())

		var lset labels.Labels
		var chks []chunks.Meta
		if err := r.Series(p.At(), &lset, &chks); err != nil {
			t.Fatal(err)
		}
		res = append(res, lset)
	}
	if p.Err() != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res, []labels.Labels{
		{{"a", "1"}, {"b", "1"}},
		{{"a", "2"}, {"b", "1"}},
	}) {
		t.Fatalf("unexpected result %v", res)
	}

	meta, err = readMetaFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Version != 1 {
		t.Fatalf("unexpected meta version %d", meta.Version)
	}
}
