// Copyright 2019 The Prometheus Authors
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

package importer

import (
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/testutil"
)

var contentType = "application/openmetrics-text; version=0.0.1; charset=utf-8"

func TestParseMetrics(t *testing.T) {
	tests := []struct {
		ToParse  []byte
		Expected []*metricSample
		IsOk     bool
	}{
		{
			ToParse:  []byte(``),
			Expected: nil,
			IsOk:     true,
		},
		{
			ToParse: []byte(`# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400"} 3 1395066363000
`),
			Expected: []*metricSample{
				&metricSample{
					TimestampMs: 1395066363000,
					Value:       1027,
					Labels:      labels.FromStrings("__name__", "http_requests_total", "method", "post", "code", "200"),
				},
				&metricSample{
					TimestampMs: 1395066363000,
					Value:       3,
					Labels:      labels.FromStrings("__name__", "http_requests_total", "method", "post", "code", "400"),
				},
			},
			IsOk: true,
		},
		{
			ToParse: []byte(`# HELP something_weird Something weird
# TYPE something_weird gauge
something_weird{problem="division by zero"} +Inf -3982045
`),
			Expected: []*metricSample{
				&metricSample{
					TimestampMs: -3982045,
					Value:       math.Inf(1),
					Labels:      labels.FromStrings("__name__", "something_weird", "problem", "division by zero"),
				},
			},
			IsOk: true,
		},
		{
			ToParse: []byte(`# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
`),
			Expected: []*metricSample{
				&metricSample{
					TimestampMs: 0,
					Value:       3102,
					Labels:      labels.FromStrings("__name__", "rpc_duration_seconds", "quantile", "0.01"),
				},
				&metricSample{
					TimestampMs: 0,
					Value:       3272,
					Labels:      labels.FromStrings("__name__", "rpc_duration_seconds", "quantile", "0.05"),
				},
			},
			IsOk: true,
		},
		{
			ToParse: []byte(`# HELP no_type_metric This is a metric with no TYPE string
no_type_metric{type="bad_news_bears"} 0.0 111
`),
			Expected: []*metricSample{
				&metricSample{
					TimestampMs: 111,
					Value:       0.0,
					Labels:      labels.FromStrings("__name__", "no_type_metric", "type", "bad_news_bears"),
				},
			},
			IsOk: true,
		},
		{
			ToParse: []byte(`# HELP bad_ts This is a metric with an extreme timestamp
# TYPE bad_ts gauge
bad_ts{type="bad_timestamp"} 420 -1e99
`),
			Expected: []*metricSample{
				&metricSample{
					TimestampMs: int64(math.MinInt64 / 1000),
					Value:       420,
					Labels:      labels.FromStrings("__name__", "bad_ts", "type", "bad_timestamp"),
				},
			},
			IsOk: true,
		},
		{
			ToParse: []byte(`# HELP bad_ts This is a metric with an extreme timestamp
# TYPE bad_ts gauge
bad_ts{type="bad_timestamp"} 420 1e99
`),
			Expected: []*metricSample{
				&metricSample{
					TimestampMs: int64(math.MinInt64 / 1000),
					Value:       420,
					Labels:      labels.FromStrings("__name__", "bad_ts", "type", "bad_timestamp"),
				},
			},
			IsOk: true,
		},
		{
			ToParse: []byte(`no_help_no_type{foo="bar"} 42 6900
`),
			Expected: []*metricSample{
				&metricSample{
					TimestampMs: 6900,
					Value:       42,
					Labels:      labels.FromStrings("__name__", "no_help_no_type", "foo", "bar"),
				},
			},
			IsOk: true,
		},
		{
			ToParse: []byte(`bare_metric 42.24
`),
			Expected: []*metricSample{
				&metricSample{
					TimestampMs: 0,
					Value:       42.24,
					Labels:      labels.FromStrings("__name__", "bare_metric"),
				},
			},
			IsOk: true,
		},
		{
			ToParse: []byte(`# HELP bad_metric This a bad metric
# TYPE bad_metric bad_type
bad_metric{type="has no type information"} 0.0 111
`),
			Expected: nil,
			IsOk:     false,
		},
		{
			ToParse: []byte(`# HELP no_nl This test has no newline so will fail
# TYPE no_nl gauge
no_nl{type="no newline"}`),
			Expected: nil,
			IsOk:     false,
		},
	}
	for _, test := range tests {
		metricSamples, _, _, err := parseMetrics(test.ToParse, contentType)
		if test.IsOk {
			testutil.Ok(t, err)
			testutil.Assert(t, len(test.Expected) == len(metricSamples), "not enough samples parsed")
			for idx, wantSample := range test.Expected {
				haveSample := metricSamples[idx]
				testutil.Assert(t, reflect.DeepEqual(wantSample, haveSample), "metric samples are different from expectations")
			}
		} else {
			testutil.NotOk(t, err)
		}
	}
}

func TestPushToDisk(t *testing.T) {
	metricSamples := []*metricSample{
		&metricSample{
			TimestampMs: 01,
			Value:       512,
			Labels:      labels.FromStrings("__name__", "lionel", "family_name", "messi"),
		},
	}
	tmpDbDir, err := ioutil.TempDir("", "ptd")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpDbDir)
	minValidTimestamp := int64(math.MinInt64)
	_, err = pushToDisk(metricSamples, tmpDbDir, minValidTimestamp, nil)
	testutil.Ok(t, err)
}

func TestCopyToDatabase(t *testing.T) {
	metricSamples := []*metricSample{
		&metricSample{
			TimestampMs: 01,
			Value:       512,
			Labels:      labels.FromStrings("__name__", "lionel", "family_name", "messi"),
		},
	}
	tmpDbDir, err := ioutil.TempDir("", "ptd")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpDbDir)
	minValidTimestamp := int64(math.MinInt64)
	snapshotPath, err := pushToDisk(metricSamples, tmpDbDir, minValidTimestamp, nil)
	testutil.Ok(t, err)
	err = copyToDatabase(snapshotPath, tmpDbDir)
	testutil.Ok(t, err)
}

func TestImportFromFile(t *testing.T) {
	text := `# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
`
	tmpFile, err := ioutil.TempFile("", "iff")
	testutil.Ok(t, err)
	defer tmpFile.Close()
	tmpFile.WriteString(text)
	tmpDbDir, err := ioutil.TempDir("", "iff-db")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpDbDir)
	err = ImportFromFile(tmpFile.Name(), contentType, tmpDbDir, false, nil)
	testutil.Ok(t, err)

	// No file found case
	err = ImportFromFile("/foo/bar/baz/buzz", contentType, "/buzz/baz/bar/foo", false, nil)
	testutil.NotOk(t, err)

	// Bad text file
	text = `# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds bad_type
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
`
	tmpFile2, err := ioutil.TempFile("", "iff")
	testutil.Ok(t, err)
	defer tmpFile2.Close()
	tmpFile2.WriteString(text)
	tmpDbDir2, err := ioutil.TempDir("", "iff-db")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpDbDir2)
	err = ImportFromFile(tmpFile2.Name(), contentType, tmpDbDir2, false, nil)
	testutil.NotOk(t, err)
}
