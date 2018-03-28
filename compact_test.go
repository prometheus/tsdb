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
	"path/filepath"
	"testing"

	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/testutil"
)

func TestSplitByRange(t *testing.T) {
	cases := []struct {
		trange int64
		ranges [][2]int64
		output [][][2]int64
	}{
		{
			trange: 60,
			ranges: [][2]int64{{0, 10}},
			output: [][][2]int64{
				{{0, 10}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{0, 60}},
			output: [][][2]int64{
				{{0, 60}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{0, 10}, {9, 15}, {30, 60}},
			output: [][][2]int64{
				{{0, 10}, {9, 15}, {30, 60}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{70, 90}, {125, 130}, {130, 180}, {1000, 1001}},
			output: [][][2]int64{
				{{70, 90}},
				{{125, 130}, {130, 180}},
				{{1000, 1001}},
			},
		},
		// Mis-aligned or too-large blocks are ignored.
		{
			trange: 60,
			ranges: [][2]int64{{50, 70}, {70, 80}},
			output: [][][2]int64{
				{{70, 80}},
			},
		},
		{
			trange: 72,
			ranges: [][2]int64{{0, 144}, {144, 216}, {216, 288}},
			output: [][][2]int64{
				{{144, 216}},
				{{216, 288}},
			},
		},
		// Various awkward edge cases easy to hit with negative numbers.
		{
			trange: 60,
			ranges: [][2]int64{{-10, -5}},
			output: [][][2]int64{
				{{-10, -5}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{-60, -50}, {-10, -5}},
			output: [][][2]int64{
				{{-60, -50}, {-10, -5}},
			},
		},
		{
			trange: 60,
			ranges: [][2]int64{{-60, -50}, {-10, -5}, {0, 15}},
			output: [][][2]int64{
				{{-60, -50}, {-10, -5}},
				{{0, 15}},
			},
		},
	}

	for _, c := range cases {
		// Transform input range tuples into dirMetas.
		blocks := make([]dirMeta, 0, len(c.ranges))
		for _, r := range c.ranges {
			blocks = append(blocks, dirMeta{
				meta: &BlockMeta{
					MinTime: r[0],
					MaxTime: r[1],
				},
			})
		}

		// Transform output range tuples into dirMetas.
		exp := make([][]dirMeta, len(c.output))
		for i, group := range c.output {
			for _, r := range group {
				exp[i] = append(exp[i], dirMeta{
					meta: &BlockMeta{MinTime: r[0], MaxTime: r[1]},
				})
			}
		}

		testutil.Equals(t, exp, splitByRange(blocks, c.trange))
	}
}

// See https://github.com/prometheus/prometheus/issues/3064
func TestNoPanicFor0Tombstones(t *testing.T) {
	metas := []dirMeta{
		{
			dir: "1",
			meta: &BlockMeta{
				MinTime: 0,
				MaxTime: 100,
			},
		},
		{
			dir: "2",
			meta: &BlockMeta{
				MinTime: 101,
				MaxTime: 200,
			},
		},
	}

	c, err := NewLeveledCompactor(nil, nil, []int64{50}, nil)
	testutil.Ok(t, err)

	c.plan(metas)
}

func TestLeveledCompactor_plan(t *testing.T) {
	// This mimicks our default ExponentialBlockRanges with min block size equals to 20.
	compactor, err := NewLeveledCompactor(nil, nil, ExponentialBlockRanges(int64(time.Duration(2*time.Hour).Seconds()*1000), 10, 3), nil)
	testutil.Ok(t, err)

	cases := []struct {
		metas    []dirMeta
		expected []string
	}{
		// NEW investigation of https://github.com/dswarbrick case.
		// Followed his logs and assuming that from 2.2.1 start the storage was nuked.
		// Interesting logs:
		/*
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.59888874Z caller=main.go:220 msg="Starting Prometheus" version="(version=2.2.1, branch=HEAD, revision=bc6058c81272a8d938c05e75607371284236aadc)"
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.598969895Z caller=main.go:221 build_context="(go=go1.10, user=root@149e5b3f0829, date=20180314-14:15:45)"
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.598993442Z caller=main.go:222 host_details="(Linux 4.14.0-0.bpo.3-amd64 #1 SMP Debian 4.14.13-1~bpo9+1 (2018-01-14) x86_64 fkb-prom-test (none))"
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.599017547Z caller=main.go:223 fd_limits="(soft=1024, hard=4096)"
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.605340535Z caller=web.go:382 component=web msg="Start listening for connections" address=0.0.0.0:9090
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.605325812Z caller=main.go:504 msg="Starting TSDB ..."
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.615925057Z caller=main.go:514 msg="TSDB started"
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.616009502Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 10:05:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:05:52.61839357Z caller=main.go:491 msg="Server is ready to receive web requests."
			Mar 26 10:13:35 fkb-prom prometheus-blackbox-exporter[7406]: level=info ts=2018-03-26T08:13:35.318708853Z caller=main.go:213 msg="Starting blackbox_exporter" version="(version=0.11.0+ds, branch=debian/sid, revision=0.11.0+ds-4~bpo9+1)"
			Mar 26 10:13:35 fkb-prom prometheus-blackbox-exporter[7406]: level=info ts=2018-03-26T08:13:35.319703514Z caller=main.go:220 msg="Loaded config file"
			Mar 26 10:13:35 fkb-prom prometheus-blackbox-exporter[7406]: level=info ts=2018-03-26T08:13:35.319958567Z caller=main.go:324 msg="Listening on address" address=:9115
			Mar 26 10:21:40 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:21:40.532411996Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 10:23:17 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:23:17.062607203Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 10:27:12 fkb-prom prometheus-blackbox-exporter[16341]: level=info ts=2018-03-26T08:27:12.841238497Z caller=main.go:213 msg="Starting blackbox_exporter" version="(version=0.11.0+ds, branch=debian/sid, revision=0.11.0+ds-4~bpo9+1)"
			Mar 26 10:27:12 fkb-prom prometheus-blackbox-exporter[16341]: level=info ts=2018-03-26T08:27:12.842343141Z caller=main.go:220 msg="Loaded config file"
			Mar 26 10:27:12 fkb-prom prometheus-blackbox-exporter[16341]: level=info ts=2018-03-26T08:27:12.842811401Z caller=main.go:324 msg="Listening on address" address=:9115
			Mar 26 10:32:04 fkb-prom prometheus-blackbox-exporter[16428]: level=info ts=2018-03-26T08:32:04.954705057Z caller=main.go:213 msg="Starting blackbox_exporter" version="(version=0.11.0+ds, branch=debian/sid, revision=0.11.0+ds-4~bpo9+1)"
			Mar 26 10:32:04 fkb-prom prometheus-blackbox-exporter[16428]: level=info ts=2018-03-26T08:32:04.955012235Z caller=main.go:220 msg="Loaded config file"
			Mar 26 10:32:04 fkb-prom prometheus-blackbox-exporter[16428]: level=info ts=2018-03-26T08:32:04.955251288Z caller=main.go:324 msg="Listening on address" address=:9115
			Mar 26 10:36:26 fkb-prom prometheus-blackbox-exporter[16483]: level=info ts=2018-03-26T08:36:26.203961695Z caller=main.go:213 msg="Starting blackbox_exporter" version="(version=0.11.0+ds, branch=debian/sid, revision=0.11.0+ds-4~bpo9+1)"
			Mar 26 10:36:26 fkb-prom prometheus-blackbox-exporter[16483]: level=info ts=2018-03-26T08:36:26.204249526Z caller=main.go:220 msg="Loaded config file"
			Mar 26 10:36:26 fkb-prom prometheus-blackbox-exporter[16483]: level=info ts=2018-03-26T08:36:26.205062638Z caller=main.go:324 msg="Listening on address" address=:9115
			Mar 26 10:41:50 fkb-prom prometheus-blackbox-exporter[16572]: level=info ts=2018-03-26T08:41:50.357786653Z caller=main.go:213 msg="Starting blackbox_exporter" version="(version=0.11.0+ds, branch=debian/sid, revision=0.11.0+ds-4~bpo9+1)"
			Mar 26 10:41:50 fkb-prom prometheus-blackbox-exporter[16572]: level=info ts=2018-03-26T08:41:50.358156756Z caller=main.go:220 msg="Loaded config file"
			Mar 26 10:41:50 fkb-prom prometheus-blackbox-exporter[16572]: level=info ts=2018-03-26T08:41:50.359003133Z caller=main.go:324 msg="Listening on address" address=:9115
			Mar 26 10:41:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:41:52.442409509Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 10:58:29 fkb-prom prometheus[7270]: level=info ts=2018-03-26T08:58:29.699566465Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 13:00:00 fkb-prom prometheus[7270]: level=info ts=2018-03-26T11:00:00.761744326Z caller=compact.go:393 component=tsdb msg="compact blocks" count=1 mint=1522051200000 maxt=1522058400000
			Mar 26 13:00:01 fkb-prom prometheus[7270]: level=info ts=2018-03-26T11:00:01.934470041Z caller=head.go:348 component=tsdb msg="head GC completed" duration=71.516089ms
			Mar 26 13:00:01 fkb-prom prometheus[7270]: level=info ts=2018-03-26T11:00:01.935357279Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.97µs
			Mar 26 13:06:30 fkb-prom prometheus[7270]: level=info ts=2018-03-26T11:06:30.998526089Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 13:38:36 fkb-prom prometheus[7270]: level=info ts=2018-03-26T11:38:36.882394599Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 15:00:00 fkb-prom prometheus[7270]: level=info ts=2018-03-26T13:00:00.021889247Z caller=compact.go:393 component=tsdb msg="compact blocks" count=1 mint=1522058400000 maxt=1522065600000
			Mar 26 15:00:01 fkb-prom prometheus[7270]: level=info ts=2018-03-26T13:00:01.573224161Z caller=head.go:348 component=tsdb msg="head GC completed" duration=88.973445ms
			Mar 26 15:00:02 fkb-prom prometheus[7270]: level=info ts=2018-03-26T13:00:02.660807031Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=1.087465535s
			Mar 26 15:21:55 fkb-prom prometheus[7270]: level=info ts=2018-03-26T13:21:55.331942318Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 17:00:00 fkb-prom prometheus[7270]: level=info ts=2018-03-26T15:00:00.074085112Z caller=compact.go:393 component=tsdb msg="compact blocks" count=1 mint=1522065600000 maxt=1522072800000
			Mar 26 17:00:01 fkb-prom prometheus[7270]: level=info ts=2018-03-26T15:00:01.747341462Z caller=head.go:348 component=tsdb msg="head GC completed" duration=112.850857ms
			Mar 26 17:00:02 fkb-prom prometheus[7270]: level=info ts=2018-03-26T15:00:02.020982957Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=273.52937ms
			Mar 26 17:09:13 fkb-prom prometheus[7270]: level=info ts=2018-03-26T15:09:13.18712119Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 17:14:43 fkb-prom prometheus[7270]: level=info ts=2018-03-26T15:14:43.117827797Z caller=main.go:588 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 17:42:14 fkb-prom prometheus[7270]: level=error ts=2018-03-26T15:42:14.108466523Z caller=file.go:328 component="discovery manager scrape" discovery=file msg="Error reading file" path=/etc/prometheus/targets/ceph-node.yml err="yaml: line 39: did not find expected '-' indicator"
			Mar 26 17:42:28 fkb-prom prometheus[7270]: level=error ts=2018-03-26T15:42:28.55452157Z caller=file.go:328 component="discovery manager scrape" discovery=file msg="Error reading file" path=/etc/prometheus/targets/ceph-node.yml err="yaml: line 39: did not find expected '-' indicator"
			Mar 26 17:42:28 fkb-prom prometheus[7270]: level=error ts=2018-03-26T15:42:28.556000964Z caller=file.go:328 component="discovery manager scrape" discovery=file msg="Error reading file" path=/etc/prometheus/targets/ceph-node.yml err="yaml: line 39: did not find expected '-' indicator"
			Mar 26 17:42:28 fkb-prom prometheus[7270]: level=error ts=2018-03-26T15:42:28.556386147Z caller=file.go:328 component="discovery manager scrape" discovery=file msg="Error reading file" path=/etc/prometheus/targets/ceph-node.yml err="yaml: line 39: did not find expected '-' indicator"
			Mar 26 17:42:28 fkb-prom prometheus[7270]: level=error ts=2018-03-26T15:42:28.556812784Z caller=file.go:328 component="discovery manager scrape" discovery=file msg="Error reading file" path=/etc/prometheus/targets/ceph-node.yml err="yaml: line 39: did not find expected '-' indicator"
			Mar 26 17:42:28 fkb-prom prometheus[7270]: level=error ts=2018-03-26T15:42:28.557173536Z caller=file.go:328 component="discovery manager scrape" discovery=file msg="Error reading file" path=/etc/prometheus/targets/ceph-node.yml err="yaml: line 39: did not find expected '-' indicator"
			Mar 26 17:42:28 fkb-prom prometheus[7270]: level=error ts=2018-03-26T15:42:28.557592134Z caller=file.go:328 component="discovery manager scrape" discovery=file msg="Error reading file" path=/etc/prometheus/targets/ceph-node.yml err="yaml: line 39: did not find expected '-' indicator"
			Mar 26 17:42:31 fkb-prom prometheus[7270]: level=error ts=2018-03-26T15:42:31.834703337Z caller=file.go:328 component="discovery manager scrape" discovery=file msg="Error reading file" path=/etc/prometheus/targets/ceph-node.yml err="yaml: line 39: did not find expected '-' indicator"
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=warn ts=2018-03-26T16:15:52.886328081Z caller=main.go:374 msg="Received SIGTERM, exiting gracefully..."
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.886462743Z caller=main.go:398 msg="Stopping scrape discovery manager..."
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.886512146Z caller=main.go:411 msg="Stopping notify discovery manager..."
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.886544789Z caller=main.go:432 msg="Stopping scrape manager..."
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.886665509Z caller=main.go:394 msg="Scrape discovery manager stopped"
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.887021827Z caller=main.go:407 msg="Notify discovery manager stopped"
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.89377583Z caller=main.go:426 msg="Scrape manager stopped"
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.906642264Z caller=manager.go:460 component="rule manager" msg="Stopping rule manager..."
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.906697399Z caller=manager.go:466 component="rule manager" msg="Rule manager stopped"
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.90671944Z caller=notifier.go:512 component=notifier msg="Stopping notification manager..."
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.906742407Z caller=main.go:573 msg="Notifier manager stopped"
			Mar 26 18:15:52 fkb-prom prometheus[7270]: level=info ts=2018-03-26T16:15:52.90705456Z caller=main.go:584 msg="See you next time!"
			Mar 26 18:15:52 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:15:52.978665853Z caller=main.go:213 msg="Starting Prometheus" version="(version=2.2.1+ds, branch=debian/sid, revision=2.2.1+ds-1)"
			Mar 26 18:15:52 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:15:52.978741362Z caller=main.go:214 build_context="(go=go1.10, user=pkg-go-maintainers@lists.alioth.debian.org, date=20180326-04:34:44)"
			Mar 26 18:15:52 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:15:52.978766479Z caller=main.go:215 host_details="(Linux 4.14.0-0.bpo.3-amd64 #1 SMP Debian 4.14.13-1~bpo9+1 (2018-01-14) x86_64 fkb-prom-test (none))"
			Mar 26 18:15:52 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:15:52.978789911Z caller=main.go:216 fd_limits="(soft=1024, hard=4096)"
			Mar 26 18:15:52 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:15:52.983388861Z caller=main.go:497 msg="Starting TSDB ..."
			Mar 26 18:15:52 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:15:52.984891884Z caller=web.go:358 component=web msg="Start listening for connections" address=0.0.0.0:9090
			Mar 26 18:16:03 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:16:03.383539639Z caller=main.go:507 msg="TSDB started"
			Mar 26 18:16:03 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:16:03.384256731Z caller=main.go:581 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 26 18:16:03 fkb-prom prometheus[14007]: level=info ts=2018-03-26T16:16:03.386058787Z caller=main.go:484 msg="Server is ready to receive web requests."
			Mar 26 19:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-26T17:00:00.030484881Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522072800000 maxt=1522080000000
			Mar 26 19:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-26T17:00:01.513440233Z caller=head.go:348 component=tsdb msg="head GC completed" duration=69.263949ms
			Mar 26 19:00:04 fkb-prom prometheus[14007]: level=info ts=2018-03-26T17:00:04.734772369Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.220683947s
			Mar 26 19:00:04 fkb-prom prometheus[14007]: level=info ts=2018-03-26T17:00:04.924733328Z caller=compact.go:394 component=tsdb msg="compact blocks" count=2 mint=1522051200000 maxt=1522065600000
			Mar 26 21:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-26T19:00:00.038052516Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522080000000 maxt=1522087200000
			Mar 26 21:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-26T19:00:01.681575949Z caller=head.go:348 component=tsdb msg="head GC completed" duration=87.711825ms
			Mar 26 21:00:03 fkb-prom prometheus[14007]: level=info ts=2018-03-26T19:00:03.984132906Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=2.30244825s
			Mar 26 23:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-26T21:00:00.034653331Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522087200000 maxt=1522094400000
			Mar 26 23:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-26T21:00:01.708983074Z caller=head.go:348 component=tsdb msg="head GC completed" duration=83.611539ms
			Mar 26 23:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-26T21:00:05.396818535Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.687726566s
			Mar 26 23:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-26T21:00:05.578323742Z caller=compact.go:394 component=tsdb msg="compact blocks" count=3 mint=1522065600000 maxt=1522087200000
			Mar 27 01:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-26T23:00:00.032835241Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522094400000 maxt=1522101600000
			Mar 27 01:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-26T23:00:01.674963475Z caller=head.go:348 component=tsdb msg="head GC completed" duration=81.005135ms
			Mar 27 01:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-26T23:00:01.940625989Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=265.55074ms
			Mar 27 01:00:02 fkb-prom prometheus[14007]: level=info ts=2018-03-26T23:00:02.120044829Z caller=compact.go:394 component=tsdb msg="compact blocks" count=2 mint=1522051200000 maxt=1522087200000
			Mar 27 03:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-27T01:00:00.034469049Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522101600000 maxt=1522108800000
			Mar 27 03:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-27T01:00:01.623507204Z caller=head.go:348 component=tsdb msg="head GC completed" duration=76.481462ms
			Mar 27 03:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T01:00:05.232292099Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.608679064s
			Mar 27 05:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-27T03:00:00.038780308Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522108800000 maxt=1522116000000
			Mar 27 05:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-27T03:00:01.586386165Z caller=head.go:348 component=tsdb msg="head GC completed" duration=80.845291ms
			Mar 27 05:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T03:00:05.265947301Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.679420234s
			Mar 27 05:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T03:00:05.454004242Z caller=compact.go:394 component=tsdb msg="compact blocks" count=3 mint=1522087200000 maxt=1522108800000
			Mar 27 07:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-27T05:00:00.032399168Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522116000000 maxt=1522123200000
			Mar 27 07:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-27T05:00:01.629385337Z caller=head.go:348 component=tsdb msg="head GC completed" duration=67.524023ms
			Mar 27 07:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T05:00:05.199327344Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.569832085s
			Mar 27 09:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-27T07:00:00.034770127Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522123200000 maxt=1522130400000
			Mar 27 09:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-27T07:00:01.653577314Z caller=head.go:348 component=tsdb msg="head GC completed" duration=78.992772ms
			Mar 27 09:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-27T07:00:01.906391868Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=252.703373ms
			Mar 27 09:52:04 fkb-prom prometheus-blackbox-exporter[28936]: level=info ts=2018-03-27T07:52:04.438928114Z caller=main.go:213 msg="Starting blackbox_exporter" version="(version=0.12.0+ds, branch=debian/sid, revision=0.12.0+ds-1)"
			Mar 27 09:52:04 fkb-prom prometheus-blackbox-exporter[28936]: level=info ts=2018-03-27T07:52:04.439264325Z caller=main.go:220 msg="Loaded config file"
			Mar 27 09:52:04 fkb-prom prometheus-blackbox-exporter[28936]: level=info ts=2018-03-27T07:52:04.439360917Z caller=main.go:324 msg="Listening on address" address=:9115
			Mar 27 10:50:15 fkb-prom prometheus-blackbox-exporter[29116]: level=info ts=2018-03-27T08:50:15.776821896Z caller=main.go:213 msg="Starting blackbox_exporter" version="(version=0.12.0+ds, branch=debian/sid, revision=0.12.0+ds-1)"
			Mar 27 10:50:15 fkb-prom prometheus-blackbox-exporter[29116]: level=info ts=2018-03-27T08:50:15.77722708Z caller=main.go:220 msg="Loaded config file"
			Mar 27 10:50:15 fkb-prom prometheus-blackbox-exporter[29116]: level=info ts=2018-03-27T08:50:15.777369385Z caller=main.go:324 msg="Listening on address" address=:9115
			Mar 27 11:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-27T09:00:00.034468506Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522130400000 maxt=1522137600000
			Mar 27 11:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-27T09:00:01.704610366Z caller=head.go:348 component=tsdb msg="head GC completed" duration=120.244736ms
			Mar 27 11:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T09:00:05.446973019Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.742247472s
			Mar 27 11:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T09:00:05.617106584Z caller=compact.go:394 component=tsdb msg="compact blocks" count=3 mint=1522108800000 maxt=1522130400000
			Mar 27 13:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-27T11:00:00.034338127Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522137600000 maxt=1522144800000
			Mar 27 13:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-27T11:00:01.661118126Z caller=head.go:348 component=tsdb msg="head GC completed" duration=99.617246ms
			Mar 27 13:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T11:00:05.19521978Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.533428915s
			Mar 27 13:55:43 fkb-prom prometheus[14007]: level=info ts=2018-03-27T11:55:43.609270219Z caller=main.go:581 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 27 14:17:40 fkb-prom prometheus[14007]: level=info ts=2018-03-27T12:17:40.531254913Z caller=main.go:581 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 27 14:36:13 fkb-prom prometheus[14007]: level=info ts=2018-03-27T12:36:13.52989557Z caller=main.go:581 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
			Mar 27 15:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-27T13:00:00.014868002Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522144800000 maxt=1522152000000
			Mar 27 15:00:02 fkb-prom prometheus[14007]: level=info ts=2018-03-27T13:00:02.019882966Z caller=head.go:348 component=tsdb msg="head GC completed" duration=119.977491ms
			Mar 27 15:00:02 fkb-prom prometheus[14007]: level=info ts=2018-03-27T13:00:02.233535497Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=212.919311ms
			Mar 27 17:00:00 fkb-prom prometheus[14007]: level=info ts=2018-03-27T15:00:00.012415816Z caller=compact.go:394 component=tsdb msg="compact blocks" count=1 mint=1522152000000 maxt=1522159200000
			Mar 27 17:00:01 fkb-prom prometheus[14007]: level=info ts=2018-03-27T15:00:01.902424594Z caller=head.go:348 component=tsdb msg="head GC completed" duration=139.927718ms
			Mar 27 17:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T15:00:05.547723473Z caller=head.go:357 component=tsdb msg="WAL truncation completed" duration=3.64519051s
			Mar 27 17:00:05 fkb-prom prometheus[14007]: level=info ts=2018-03-27T15:00:05.769323478Z caller=compact.go:394 component=tsdb msg="compact blocks" count=3 mint=1522130400000 maxt=1522152000000
			Mar 27 17:00:08 fkb-prom prometheus[14007]: level=info ts=2018-03-27T15:00:08.140033546Z caller=compact.go:394 component=tsdb msg="compact blocks" count=2 mint=1522087200000 maxt=1522130400000
			Mar 27 17:00:11 fkb-prom prometheus[14007]: level=info ts=2018-03-27T15:00:11.078986362Z caller=compact.go:394 component=tsdb msg="compact blocks" count=2 mint=1522051200000 maxt=1522152000000
			Mar 27 17:00:14 fkb-prom prometheus[14007]: level=error ts=2018-03-27T15:00:14.672804202Z caller=db.go:281 component=tsdb msg="compaction failed" err="reload blocks: invalid block sequence: block time ranges overlap (1522087200000, 1522152000000)"
		 */
		{
			// ts=2018-03-26T17:00:00.030484881Z
			metas: []dirMeta{
				metaRange("1", 1522051200000, 1522058400000, nil),
				metaRange("2", 1522058400000, 1522065600000, nil),
				metaRange("3", 1522065600000, 1522072800000, nil),
				// Prom restart time.
				metaRange("4", 1522072800000, 1522080000000, nil),
			},
			expected: []string{"1", "2"}, // Why not 3? We have 3 sequential 2h blocks. Not a critical though.
		},
		{
			// ts=2018-03-26T21:00:00.034653331Z
			metas: []dirMeta{
				metaRange("3", 1522065600000, 1522072800000, nil),
				metaRange("4", 1522072800000, 1522080000000, nil),
				metaRange("5", 1522051200000, 1522065600000, nil), // compacted 4h
				metaRange("6", 1522080000000, 1522087200000, nil),
				metaRange("7", 1522087200000, 1522094400000, nil),
			},
			expected: []string{"3", "4", "6"},
		},
		{
			// ts=2018-03-26T23:00:00.032835241Z
			metas: []dirMeta{
				metaRange("5", 1522051200000, 1522065600000, nil), // compacted 4h
				metaRange("7", 1522087200000, 1522094400000, nil),
				metaRange("8", 1522065600000, 1522087200000, nil), // compacted 6h
				metaRange("9", 1522094400000, 1522101600000, nil),
			},
			expected: []string{"5", "8"},
		},
		{
			// ts=2018-03-27T03:00:00.038780308Z
			metas: []dirMeta{
				metaRange("7", 1522087200000, 1522094400000, nil),
				metaRange("9", 1522094400000, 1522101600000, nil),
				metaRange("10", 1522051200000, 1522087200000, nil), // 10h
				metaRange("11", 1522101600000, 1522108800000, nil),
				metaRange("12", 1522108800000, 1522116000000, nil),
			},
			expected: []string{"7", "9", "11"},
		},
		{
			// ts=2018-03-27T09:00:00.034468506Z
			metas: []dirMeta{
				metaRange("10", 1522051200000, 1522087200000, nil), // 10h
				metaRange("12", 1522108800000, 1522116000000, nil),
				metaRange("13", 1522087200000, 1522108800000, nil), // 6h
				metaRange("14", 1522116000000, 1522123200000, nil),
				metaRange("15", 1522123200000, 1522130400000, nil),
				metaRange("16", 1522130400000, 1522137600000, nil),
				// -blackbox-exporter stuff (unrelated?)
			},
			expected: []string{"12", "14", "15"},
		},
		{
			// ts=2018-03-27T15:00:00.012415816Z
			metas: []dirMeta{
				metaRange("10", 1522051200000, 1522087200000, nil), // 10h
				metaRange("13", 1522087200000, 1522108800000, nil), // 6h
				metaRange("16", 1522130400000, 1522137600000, nil),
				metaRange("17", 1522108800000, 1522130400000, nil), // 6h
				metaRange("18", 1522137600000, 1522144800000, nil),
				metaRange("19", 1522144800000, 1522152000000, nil),
				metaRange("20", 1522152000000, 1522159200000, nil),
			},
			expected: []string{"16", "18", "19"},
		},
		{
			// ts=2018-03-27T15:00:05.769323478Z
			metas: []dirMeta{
				metaRange("10", 1522051200000, 1522087200000, nil), // 10h
				metaRange("13", 1522087200000, 1522108800000, nil), // 6h
				metaRange("17", 1522108800000, 1522130400000, nil), // 6h
				metaRange("20", 1522152000000, 1522159200000, nil),
				metaRange("21", 1522130400000, 1522152000000, nil), // 6h
			},
			expected: []string{"13", "17", "21"},
			// This above is what compactor does for our local test, but seems to be different for dswarbrick/
			// The above expected compaction makes sense. We should compact into 1522087200000 - 1522152000000
			// (!!) However for dswarbrick compactor did:
			// level=info ts=2018-03-27T15:00:08.140033546Z caller=compact.go:394 component=tsdb msg="compact blocks" count=2 mint=1522087200000 maxt=1522130400000
			// so 12h block. Somehow he missed last 6h which indicated 2.2.0 code issue. O.o
			//
			// However - previous compacts and log line suggest 2.2.1 is running.
			// Not sure how that happens without any side effects (side impacts on storage).
			// Current state: Cannot repro.
		},
	}

	for _, c := range cases {
		if !t.Run("", func(t *testing.T) {
			res, err := compactor.plan(c.metas)
			testutil.Ok(t, err)

			testutil.Equals(t, c.expected, res)
		}) {
			return
		}
	}
}

func TestRangeWithFailedCompactionWontGetSelected(t *testing.T) {
	compactor, err := NewLeveledCompactor(nil, nil, []int64{
		20,
		60,
		240,
		720,
		2160,
	}, nil)
	testutil.Ok(t, err)

	cases := []struct {
		metas []dirMeta
	}{
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 40, 60, nil),
				metaRange("4", 60, 80, nil),
			},
		},
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 60, 80, nil),
				metaRange("4", 80, 100, nil),
			},
		},
		{
			metas: []dirMeta{
				metaRange("1", 0, 20, nil),
				metaRange("2", 20, 40, nil),
				metaRange("3", 40, 60, nil),
				metaRange("4", 60, 120, nil),
				metaRange("5", 120, 180, nil),
				metaRange("6", 180, 200, nil),
			},
		},
	}

	for _, c := range cases {
		c.metas[1].meta.Compaction.Failed = true
		res, err := compactor.plan(c.metas)
		testutil.Ok(t, err)

		testutil.Equals(t, []string(nil), res)
	}
}

func TestCompactionFailWillCleanUpTempDir(t *testing.T) {
	compactor, err := NewLeveledCompactor(nil, log.NewNopLogger(), []int64{
		20,
		60,
		240,
		720,
		2160,
	}, nil)
	testutil.Ok(t, err)

	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)
	defer os.RemoveAll(tmpdir)

	testutil.NotOk(t, compactor.write(tmpdir, &BlockMeta{}, erringBReader{}))
	_, err = os.Stat(filepath.Join(tmpdir, BlockMeta{}.ULID.String()) + ".tmp")
	testutil.Assert(t, os.IsNotExist(err), "directory is not cleaned up")
}

func metaRange(name string, mint, maxt int64, stats *BlockStats) dirMeta {
	meta := &BlockMeta{MinTime: mint, MaxTime: maxt}
	if stats != nil {
		meta.Stats = *stats
	}
	return dirMeta{
		dir:  name,
		meta: meta,
	}
}

type erringBReader struct{}

func (erringBReader) Index() (IndexReader, error)          { return nil, errors.New("index") }
func (erringBReader) Chunks() (ChunkReader, error)         { return nil, errors.New("chunks") }
func (erringBReader) Tombstones() (TombstoneReader, error) { return nil, errors.New("tombstones") }
