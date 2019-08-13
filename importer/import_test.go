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
	"testing"

	"github.com/prometheus/tsdb/testutil"
)

var contentType = "application/openmetrics-text; version=0.0.1; charset=utf-8"

func TestParseMetrics(t *testing.T) {
	tests := []struct {
		ToParse string
		IsOk    bool
	}{
		{
			ToParse: ``,
			IsOk:    true,
		},
		{
			ToParse: `# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1565133713989
http_requests_total{method="post",code="400"} 3 1575133713979
`,
			IsOk: true,
		},
		{
			ToParse: `# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400"} 3 1395066363000
`,
			IsOk: true,
		},
		{
			ToParse: `# HELP something_weird Something weird
# TYPE something_weird gauge
something_weird{problem="infinite timestamp"} +Inf -3982045
`,
			IsOk: false,
		},
		{
			ToParse: `# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
`,
			IsOk: true,
		},
		{
			ToParse: `# HELP no_type_metric This is a metric with no TYPE string
no_type_metric{type="bad_news_bears"} 0.0 111
`,
			IsOk: true,
		},
		{
			ToParse: `# HELP bad_ts This is a metric with an extreme timestamp
# TYPE bad_ts gauge
bad_ts{type="bad_timestamp"} 420 -1e99
`,
			IsOk: false,
		},
		{
			ToParse: `# HELP bad_ts This is a metric with an extreme timestamp
# TYPE bad_ts gauge
bad_ts{type="bad_timestamp"} 420 1e99
`,
			IsOk: false,
		},
		{
			ToParse: `no_help_no_type{foo="bar"} 42 6900
`,
			IsOk: true,
		},
		{
			ToParse: `bare_metric 42.24
`,
			IsOk: true,
		},
		{
			ToParse: `# HELP bad_metric This a bad metric
# TYPE bad_metric bad_type
bad_metric{type="has no type information"} 0.0 111
`,
			IsOk: false,
		},
		{
			ToParse: `# HELP no_nl This test has no newline so will fail
# TYPE no_nl gauge
no_nl{type="no newline"}`,
			IsOk: false,
		},
	}
	for _, test := range tests {
		tmpDbDir, err := ioutil.TempDir("", "importer")
		testutil.Ok(t, err)
		_, err = pushMetrics([]byte(test.ToParse), contentType, tmpDbDir, int64(math.MinInt64), int64(math.MaxInt64), false, nil)
		if test.IsOk {
			testutil.Ok(t, err)
		} else {
			testutil.NotOk(t, err)
		}
		_ = os.RemoveAll(tmpDbDir)
	}
}

func TestPushMetrics(t *testing.T) {
	metrics := `# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1565133713989
http_requests_total{method="post",code="400"} 3 1575133713979
`
	tmpDbDir, err := ioutil.TempDir("", "importer")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpDbDir)
	_, err = pushMetrics([]byte(metrics), contentType, tmpDbDir, int64(math.MinInt64), int64(math.MaxInt64), false, nil)
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

//func TestLargeDataset(t *testing.T) {
//	//tmpDbDir := "/Users/dpanjabi/tmp/iff-db"
//	//_ = os.RemoveAll(tmpDbDir)
//	//err := os.MkdirAll(tmpDbDir, 0777)
//	//testutil.Ok(t, err)
//	tmpDbDir, err := ioutil.TempDir("", "iff-db")
//	testutil.Ok(t, err)
//	defer os.RemoveAll(tmpDbDir)
//	filename := "/Users/dpanjabi/projects/src/github.com/prometheus/go-prom-importer/dummy_prometheus_metrics.dat"
//	err = ImportFromFile(filename, contentType, tmpDbDir, false, nil)
//	testutil.Ok(t, err)
//}
