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

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		cli                  = kingpin.New(filepath.Base(os.Args[0]), "CLI tool for tsdb")
		benchCmd             = cli.Command("bench", "run benchmarks")
		benchWriteCmd        = benchCmd.Command("write", "run a write performance benchmark")
		benchWriteOutPath    = benchWriteCmd.Flag("out", "set the output path").Default("benchout/").String()
		benchWriteNumMetrics = benchWriteCmd.Flag("metrics", "number of metrics to read").Default("10000").Int()
		benchSamplesFile     = benchWriteCmd.Arg("file", "input file with samples data, default is (../../testdata/20kseries.json)").Default("../../testdata/20kseries.json").String()
		listCmd              = cli.Command("ls", "list db blocks")
		listCmdHumanReadable = listCmd.Flag("human-readable", "print human readable values").Short('h').Bool()
		listPath             = listCmd.Arg("db path", "database path (default is benchout/storage)").Default("benchout/storage").String()
		walBlockCmd          = cli.Command("wal-block", "write a block from the wal file")
		dbDir                = walBlockCmd.Arg("db dir", "db dir (default is ./)").Default("./").String()
		logger               = level.NewFilter(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), level.AllowInfo())
	)

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case benchWriteCmd.FullCommand():
		wb := &writeBenchmark{
			outPath:     *benchWriteOutPath,
			numMetrics:  *benchWriteNumMetrics,
			samplesFile: *benchSamplesFile,
		}
		wb.run()
	case listCmd.FullCommand():
		db, err := tsdb.Open(*listPath, nil, nil, nil)
		if err != nil {
			exitWithError(err)
		}
		printBlocks(db.Blocks(), *listCmdHumanReadable)
	case walBlockCmd.FullCommand():
		block, err := walBlock(logger, *dbDir)
		if err != nil {
			exitWithError(err)
		}

		logger.Log("path", block.Dir())
		printBlocks([]*tsdb.Block{block}, true)
	}
	flag.CommandLine.Set("log.level", "debug")
}

type writeBenchmark struct {
	outPath     string
	samplesFile string
	cleanup     bool
	numMetrics  int

	storage *tsdb.DB

	cpuprof   *os.File
	memprof   *os.File
	blockprof *os.File
	mtxprof   *os.File
}

func walBlock(logger log.Logger, dbDir string) (*tsdb.Block, error) {

	var (
		err    error
		db     *tsdb.DB
		walDir = path.Join(dbDir, "wal")
	)
	totalTime := measureTime("opening the db", func() {
		db, err = tsdb.Open(dbDir, logger, nil, tsdb.DefaultOptions)
	})

	if err != nil {
		return nil, errors.Wrapf(err, "couldn't open db dir:%v", dbDir)
	}

	// Get the maxt of the last block in the db.
	// The wal block needs to start from there to avoid overlaps.
	blocks := db.Blocks()
	if len(blocks) == 0 {
		return nil, fmt.Errorf("no blocks found in dir: %v", dbDir)
	}
	lastBlockMaxT := blocks[len(blocks)-1].Meta().MaxTime
	level.Info(logger).Log("msg", "db last block maxt",
		"maxt", lastBlockMaxT,
		"maxt-h", getFormatedTime(lastBlockMaxT, true),
	)

	head := db.Head()
	// If the maxt hasn't changed than the head doesn't contain any samples.
	if head.MaxTime() == lastBlockMaxT || head.MaxTime() == math.MinInt64 {
		return nil, fmt.Errorf("head doesn't contain any samples, dir:%s", dbDir)
	}

	totalTime += measureTime("truncating the head", func() {
		err = head.Truncate(lastBlockMaxT)
	})
	if err != nil {
		return nil, errors.Wrapf(err, "truncating the head to:%v", lastBlockMaxT)
	}

	level.Info(logger).Log("msg", "head truncated to the last block maxt",
		"mint", head.MinTime(),
		"mint-h", getFormatedTime(head.MinTime(), true),
		"maxt", head.MaxTime(),
		"maxt-h", getFormatedTime(head.MaxTime(), true),
	)

	compactor, err := tsdb.NewLeveledCompactor(nil, logger, tsdb.DefaultOptions.BlockRanges, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create leveled compactor")
	}

	var ulid ulid.ULID
	totalTime += measureTime("read series from memory and write a block", func() {
		ulid, err = compactor.Write(walDir, head, head.MinTime(), head.MaxTime(), nil)
		if err != nil {
			errors.Wrap(err, "read series from memory and write a block")
		}
	})
	if err != nil {
		return nil, err
	}

	var b *tsdb.Block
	totalTime += measureTime("opening new block", func() {
		b, err = tsdb.OpenBlock(filepath.Join(walDir, ulid.String()), nil)
		errors.Wrap(err, "opening new block")
	})
	if err != nil {
		return nil, err
	}

	fmt.Printf("\n>> total time for all stages:%v \n\n", totalTime)

	return b, nil

	// return nil, nil

	// if _, err := os.Stat(walDir); err != nil {
	// 	return nil, errors.Wrapf(err, "couldn't stat the wal dir:%v", walDir)
	// }
	// if err := tsdb.MigrateWAL(logger, walDir); err != nil {
	// 	return nil, errors.Wrap(err, "migrate WAL")
	// }
	// wal, err := wal.New(logger, nil, walDir)
	// if err != nil {
	// 	return nil, err
	// }
	// head, err := tsdb.NewHead(nil, logger, wal, tsdb.DefaultOptions.BlockRanges[0])
	// if err != nil {
	// 	return nil, err
	// }
	// // Closing the head shouldn't be needed, but just in case.
	// defer func() {
	// 	if err := head.Close(); err != nil {
	// 		level.Error(logger).Log("msg", "closing the head failed", "err", err)
	// 	}
	// }()
	// totalTime := measureTime("reading the wal files into memory", func() {
	// 	errors.Wrap(head.Truncate(lastBlockMaxT), "setting the minimum WAL timestamp")
	// 	level.Info(logger).Log("msg", "head start time", "mint", head.MinTime())
	// 	errors.Wrap(head.Init(), "reading the wal files into memory")
	// })
	// if err != nil {
	// 	return nil, err
	// }

}

func (b *writeBenchmark) run() {
	if b.outPath == "" {
		dir, err := ioutil.TempDir("", "tsdb_bench")
		if err != nil {
			exitWithError(err)
		}
		b.outPath = dir
		b.cleanup = true
	}
	if err := os.RemoveAll(b.outPath); err != nil {
		exitWithError(err)
	}
	if err := os.MkdirAll(b.outPath, 0777); err != nil {
		exitWithError(err)
	}

	dir := filepath.Join(b.outPath, "storage")

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	st, err := tsdb.Open(dir, l, nil, &tsdb.Options{
		WALFlushInterval:  200 * time.Millisecond,
		RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		BlockRanges:       tsdb.ExponentialBlockRanges(2*60*60*1000, 5, 3),
	})
	if err != nil {
		exitWithError(err)
	}
	b.storage = st

	var metrics []labels.Labels

	measureTime("readData", func() {
		f, err := os.Open(b.samplesFile)
		if err != nil {
			exitWithError(err)
		}
		defer f.Close()

		metrics, err = readPrometheusLabels(f, b.numMetrics)
		if err != nil {
			exitWithError(err)
		}
	})

	var total uint64

	dur := measureTime("ingestScrapes", func() {
		b.startProfiling()
		total, err = b.ingestScrapes(metrics, 3000)
		if err != nil {
			exitWithError(err)
		}
	})

	fmt.Println(" > total samples:", total)
	fmt.Println(" > samples/sec:", float64(total)/dur.Seconds())

	measureTime("stopStorage", func() {
		if err := b.storage.Close(); err != nil {
			exitWithError(err)
		}
		if err := b.stopProfiling(); err != nil {
			exitWithError(err)
		}
	})
}

const timeDelta = 30000

func (b *writeBenchmark) ingestScrapes(lbls []labels.Labels, scrapeCount int) (uint64, error) {
	var mu sync.Mutex
	var total uint64

	for i := 0; i < scrapeCount; i += 100 {
		var wg sync.WaitGroup
		lbls := lbls
		for len(lbls) > 0 {
			l := 1000
			if len(lbls) < 1000 {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			go func() {
				n, err := b.ingestScrapesShard(batch, 100, int64(timeDelta*i))
				if err != nil {
					// exitWithError(err)
					fmt.Println(" err", err)
				}
				mu.Lock()
				total += n
				mu.Unlock()
				wg.Done()
			}()
		}
		wg.Wait()
	}
	fmt.Println("ingestion completed")

	return total, nil
}

func (b *writeBenchmark) ingestScrapesShard(metrics []labels.Labels, scrapeCount int, baset int64) (uint64, error) {
	ts := baset

	type sample struct {
		labels labels.Labels
		value  int64
		ref    *uint64
	}

	scrape := make([]*sample, 0, len(metrics))

	for _, m := range metrics {
		scrape = append(scrape, &sample{
			labels: m,
			value:  123456789,
		})
	}
	total := uint64(0)

	for i := 0; i < scrapeCount; i++ {
		app := b.storage.Appender()
		ts += timeDelta

		for _, s := range scrape {
			s.value += 1000

			if s.ref == nil {
				ref, err := app.Add(s.labels, ts, float64(s.value))
				if err != nil {
					panic(err)
				}
				s.ref = &ref
			} else if err := app.AddFast(*s.ref, ts, float64(s.value)); err != nil {

				if errors.Cause(err) != tsdb.ErrNotFound {
					panic(err)
				}

				ref, err := app.Add(s.labels, ts, float64(s.value))
				if err != nil {
					panic(err)
				}
				s.ref = &ref
			}

			total++
		}
		if err := app.Commit(); err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *writeBenchmark) startProfiling() {
	var err error

	// Start CPU profiling.
	b.cpuprof, err = os.Create(filepath.Join(b.outPath, "cpu.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create cpu profile: %v", err))
	}
	if err := pprof.StartCPUProfile(b.cpuprof); err != nil {
		exitWithError(fmt.Errorf("bench: could not start CPU profile: %v", err))
	}

	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create memory profile: %v", err))
	}
	runtime.MemProfileRate = 64 * 1024

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create block profile: %v", err))
	}
	runtime.SetBlockProfileRate(20)

	b.mtxprof, err = os.Create(filepath.Join(b.outPath, "mutex.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create mutex profile: %v", err))
	}
	runtime.SetMutexProfileFraction(20)
}

func (b *writeBenchmark) stopProfiling() error {
	if b.cpuprof != nil {
		pprof.StopCPUProfile()
		b.cpuprof.Close()
		b.cpuprof = nil
	}
	if b.memprof != nil {
		if err := pprof.Lookup("heap").WriteTo(b.memprof, 0); err != nil {
			return fmt.Errorf("error writing mem profile: %v", err)
		}
		b.memprof.Close()
		b.memprof = nil
	}
	if b.blockprof != nil {
		if err := pprof.Lookup("block").WriteTo(b.blockprof, 0); err != nil {
			return fmt.Errorf("error writing block profile: %v", err)
		}
		b.blockprof.Close()
		b.blockprof = nil
		runtime.SetBlockProfileRate(0)
	}
	if b.mtxprof != nil {
		if err := pprof.Lookup("mutex").WriteTo(b.mtxprof, 0); err != nil {
			return fmt.Errorf("error writing mutex profile: %v", err)
		}
		b.mtxprof.Close()
		b.mtxprof = nil
		runtime.SetMutexProfileFraction(0)
	}
	return nil
}

func measureTime(stage string, f func()) time.Duration {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	f()
	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
	return time.Since(start)
}

func mapToLabels(m map[string]interface{}, l *labels.Labels) {
	for k, v := range m {
		*l = append(*l, labels.Label{Name: k, Value: v.(string)})
	}
}

func readPrometheusLabels(r io.Reader, n int) ([]labels.Labels, error) {
	scanner := bufio.NewScanner(r)

	var mets []labels.Labels
	hashes := map[uint64]struct{}{}
	i := 0

	for scanner.Scan() && i < n {
		m := make(labels.Labels, 0, 10)

		r := strings.NewReplacer("\"", "", "{", "", "}", "")
		s := r.Replace(scanner.Text())

		labelChunks := strings.Split(s, ",")
		for _, labelChunk := range labelChunks {
			split := strings.Split(labelChunk, ":")
			m = append(m, labels.Label{Name: split[0], Value: split[1]})
		}
		// Order of the k/v labels matters, don't assume we'll always receive them already sorted.
		sort.Sort(m)
		h := m.Hash()
		if _, ok := hashes[h]; ok {
			continue
		}
		mets = append(mets, m)
		hashes[h] = struct{}{}
		i++
	}
	return mets, nil
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func printBlocks(blocks []*tsdb.Block, humanReadable bool) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES")
	for _, b := range blocks {
		meta := b.Meta()

		fmt.Fprintf(tw,
			"%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			getFormatedTime(meta.MinTime, humanReadable),
			getFormatedTime(meta.MaxTime, humanReadable),
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
		)
	}
}

func getFormatedTime(timestamp int64, humanReadable bool) string {
	if humanReadable {
		return time.Unix(timestamp/1000, 0).String()
	}
	return strconv.FormatInt(timestamp, 10)
}
