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
	"os"
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
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		cli                  = kingpin.New(filepath.Base(os.Args[0]), "CLI tool for tsdb")
		benchCmd             = cli.Command("bench", "run benchmarks")
		benchWriteCmd        = benchCmd.Command("write", "run a write performance benchmark")
		benchWriteOutPath    = benchWriteCmd.Flag("out", "set the output path").Default("benchout").String()
		benchWriteNumMetrics = benchWriteCmd.Flag("metrics", "number of metrics to read").Default("10000").Int()
		benchSamplesFile     = benchWriteCmd.Arg("file", "input file with samples data, default is ("+filepath.Join("..", "testdata", "20kseries.json")+")").Default(filepath.Join("..", "testdata", "20kseries.json")).String()
		listCmd              = cli.Command("ls", "list db blocks")
		listCmdHumanReadable = listCmd.Flag("human-readable", "print human readable values").Short('h').Bool()
		listPath             = listCmd.Arg("db path", "database path (default is "+filepath.Join("benchout", "storage")+")").Default(filepath.Join("benchout", "storage")).String()
		scanCmd              = cli.Command("scan", "scans the db and repairs or deletes corrupted blocks")
		scanCmdHumanReadable = scanCmd.Flag("human-readable", "print human readable values").Short('h').Bool()
		scanPath             = scanCmd.Arg("dir", "database path (default is current dir ./)").Default("./").ExistingDir()
		logger               = level.NewFilter(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), level.AllowError())
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
		printBlocks(db.Blocks(), listCmdHumanReadable)

	case scanCmd.FullCommand():
		if err := scanTmps(*scanPath, scanCmdHumanReadable); err != nil {
			exitWithError(err)
		}

		scan, err := tsdb.NewDBScanner(*scanPath, logger)
		if err != nil {
			exitWithError(err)
		}
		if err := scanTombstones(scan, scanCmdHumanReadable); err != nil {
			exitWithError(err)
		}
		if err := scanIndexes(scan, scanCmdHumanReadable); err != nil {
			exitWithError(err)
		}
		if err := scanOverlappingBlocks(scan, scanCmdHumanReadable); err != nil {
			exitWithError(err)
		}

		fmt.Println("Scan complete!")
	}
	flag.CommandLine.Set("log.level", "debug")
}

func scanOverlappingBlocks(scan tsdb.Scanner, hformat *bool) error {
	overlaps, err := scan.Overlapping()
	if err != nil {
		return err
	}
	if len(overlaps) > 0 {
		fmt.Println("Overlaping blocks.")
		fmt.Println("Deleting these will remove all data in the listed time range.")
		var blocksDel []*tsdb.Block
		for t, overBcks := range overlaps {
			var ULIDS string
			for _, b := range overBcks {
				ULIDS = ULIDS + b.Meta().ULID.String() + " "
			}
			fmt.Printf("overlapping blocks : %v %v-%v \n", ULIDS, time.Unix(t.Min/1000, 0).Format("06/01/02 15:04:05"), time.Unix(t.Max/1000, 0).Format("15:04:05 06/01/02"))

			var largest int
			for i, b := range overBcks {
				if b.Meta().Stats.NumSamples > overBcks[largest].Meta().Stats.NumSamples {
					largest = i
				}
			}
			fmt.Printf("\nBlock %v contains highest samples count and is ommited from the deletion list! \n\n", overBcks[largest])
			// Remove the largest block from the slice.
			o := append(overBcks[:largest], overBcks[largest+1:]...)
			// Add this range to all blocks for deletion.
			blocksDel = append(blocksDel, o...)
		}

		var paths []string
		for _, b := range blocksDel {
			paths = append(paths, b.Dir())
		}
		printBlocks(blocksDel, hformat)
		moveTo := filepath.Join(scan.Dir(), "overlappingBlocks")
		confirmed, err := confirm("Confirm moving the overlapping blocks to: " + moveTo)
		if err != nil {
			return err
		}
		if confirmed {
			for _, file := range paths {
				fileutil.Replace(file, moveTo)
			}
		}
	}
	return nil
}

func scanIndexes(scan tsdb.Scanner, hformat *bool) error {
	unrepairable, repaired, err := scan.Indexes()
	if err != nil {
		return err
	}

	if len(repaired) > 0 {
		fmt.Println("Corrupted indexes that were repaired.")
		for _, stats := range repaired {
			fmt.Printf("path:%v stats:%+v  \n", stats.BlockDir, stats)
		}
	}

	for cause, bdirs := range unrepairable {
		fmt.Println("Blocks with unrepairable indexes! \n", cause)
		printFiles(bdirs, hformat)

		moveTo := filepath.Join(scan.Dir(), "blocksWithInvalidIndexes")
		confirmed, err := confirm("Confirm moving unrepairable indexes to: " + moveTo)
		if err != nil {
			return err
		}
		if confirmed {
			for _, file := range bdirs {
				fileutil.Replace(file, moveTo)
			}
		}
	}
	return nil
}

func scanTombstones(scan tsdb.Scanner, hformat *bool) error {
	invalid, err := scan.Tombstones()
	if err != nil {
		return errors.Wrap(err, "scannings Tombstones")
	}

	if len(invalid) > 0 {
		fmt.Println("Tombstones include data to be deleted so removing these will cancel deleting these timeseries.")
		for cause, files := range invalid {
			for _, p := range files {
				_, file := filepath.Split(p)
				if file != "tombstone" {
					return fmt.Errorf("path doesn't contain a valid tombstone filename: %v", p)
				}
			}
			fmt.Println("invalid tombstones:", cause)
			printFiles(files, hformat)
			moveTo := filepath.Join(scan.Dir(), "badTombstones")
			confirmed, err := confirm("Confirm moving unrepairable tombstones to: " + moveTo)
			if err != nil {
				return err
			}
			if confirmed {
				for _, file := range files {
					fileutil.Replace(file, moveTo)
				}
			}
		}
	}
	return nil
}

func scanTmps(scanPath string, hformat *bool) error {
	var files []string
	filepath.Walk(scanPath, func(path string, f os.FileInfo, _ error) error {
		if filepath.Ext(path) == ".tmp" {
			files = append(files, path)
		}
		return nil
	})
	if len(files) > 0 {
		fmt.Println(`
			These are usually caused by a crash or some incomplete operation and 
			are safe to delete as long as no other application is currently using this database.`)
		printFiles(files, hformat)
		confirmed, err := confirm("DELETE")
		if err != nil {
			return err
		}
		if confirmed {
			if err := delAll(files); err != nil {
				return errors.Wrap(err, "deleting temp files")
			}
		}
	}
	return nil
}

func delAll(paths []string) error {
	for _, p := range paths {
		if err := os.RemoveAll(p); err != nil {
			return errors.Wrapf(err, "error deleting:%v", p)
		}
	}
	return nil
}

func confirm(action string) (bool, error) {
	for x := 0; x < 3; x++ {
		fmt.Println(action, " (y/N)?")
		var s string
		_, err := fmt.Scanln(&s)
		if err != nil {
			return false, err
		}

		s = strings.TrimSpace(s)
		s = strings.ToLower(s)

		if s == "y" || s == "yes" {
			return true, nil
		}
		if s == "n" || s == "no" {
			return false, nil
		}
		fmt.Println(s, "is not a valid answer")
	}
	fmt.Printf("Bailing out, too many invalid answers! \n\n")
	return false, nil
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
		exitWithError(errors.Wrap(err, "bench: could not create cpu profile"))
	}
	if err := pprof.StartCPUProfile(b.cpuprof); err != nil {
		exitWithError(errors.Wrap(err, "bench: could not start CPU profile"))
	}
	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		exitWithError(errors.Wrap(err, "bench: could not create memory profile: %v"))
	}
	runtime.MemProfileRate = 64 * 1024

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		exitWithError(errors.Wrap(err, "bench: could not create block profile: %v"))
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

func printFiles(files []string, humanReadable *bool) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "PATH\tSIZE\tDATE\t")
	for _, path := range files {
		f, e := os.Stat(path)
		if e != nil {
			exitWithError(e)
		}
		fmt.Fprintf(tw,
			"%v\t%v\t%v\n",
			path, f.Size(), getFormatedTime(f.ModTime().Unix(), humanReadable),
		)
	}
}

func printBlocks(blocks []*tsdb.Block, humanReadable *bool) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES\tPATH")
	for _, b := range blocks {
		meta := b.Meta()

		fmt.Fprintf(tw,
			"%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			getFormatedTime(meta.MinTime, humanReadable),
			getFormatedTime(meta.MaxTime, humanReadable),
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
			b.Dir(),
		)
	}
}

func getFormatedTime(timestamp int64, humanReadable *bool) string {
	if *humanReadable {
		return time.Unix(timestamp/1000, 0).String()
	}
	return strconv.FormatInt(timestamp, 10)
}
