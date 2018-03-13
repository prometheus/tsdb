package tsdb

import (
	"encoding/json"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/tsdb/testutil"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
)

var (
	testMinBlockSize = 2 * time.Hour
	entropy          = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func createDummyBlock(t *testing.T, dir string, metas ...*BlockMeta) {
	time.Sleep(10 * time.Millisecond)
	uid := ulid.MustNew(ulid.Now(), entropy)

	testutil.Ok(t, os.Mkdir(path.Join(dir, uid.String()), os.ModePerm))

	var meta *BlockMeta
	if len(metas) == 1 {
		meta = metas[0]
		meta.ULID = uid
		meta.Version = 1
	} else {
		meta = compactBlockMetas(uid, metas...)
		meta.Version = 1
	}
	b, err := json.Marshal(meta)
	testutil.Ok(t, err)

	testutil.Ok(t, ioutil.WriteFile(path.Join(dir, uid.String(), metaFilename), b, os.ModePerm))
}

func getMmetas(t *testing.T, dir string) []*BlockMeta {
	dirs, err := blockDirs(dir)
	testutil.Ok(t, err)

	var metas []*BlockMeta
	for _, dir := range dirs {
		meta, err := readMetaFile(dir)
		testutil.Ok(t, err)
		metas = append(metas, meta)
	}
	return metas
}

func printBlocks(metas []*BlockMeta) {
	for i, meta := range metas {
		line := fmt.Sprintf("ulid: %s min: %v max: %v", meta.ULID.String(), meta.MinTime, meta.MaxTime)
		if i == len(metas)-1 {
			line += "< LAST >"
		}
		fmt.Println(line)
	}
	fmt.Println("")
}

func TestCompactionPlan_Issue3943(t *testing.T) {
	dir := "testdata/issue3943"
	testutil.Ok(t, os.Mkdir(dir, os.ModePerm))
	defer os.RemoveAll(dir)

	rngs := ExponentialBlockRanges(int64(time.Duration(testMinBlockSize).Seconds()*1000), 10, 3)

	comp, err := NewLeveledCompactor(nil, log.NewLogfmtLogger(os.Stderr), rngs, nil)
	testutil.Ok(t, err)

	// Arbitrary start time.
	currT := int64(1520532000000)

	// Trying to deduct initial state (4 blocks) before upgrade.
	createDummyBlock(t, dir, &BlockMeta{
		MinTime: currT - int64(time.Duration(36* time.Hour).Seconds()*1000),
		MaxTime: currT,
	})

	for _, tcase := range []struct {
		runMsg                     string
		expectedBlockIndexesInPlan [][]int
		expectedBlockRanges        []time.Duration
	}{
		{
			runMsg:              "0",
			expectedBlockRanges: []time.Duration{36 * time.Hour},
		},
		{
			runMsg:              "1",
			expectedBlockRanges: []time.Duration{36* time.Hour, 2 * time.Hour},
		},
		{
			runMsg:              "2",
			expectedBlockRanges: []time.Duration{36* time.Hour, 2 * time.Hour, 2 * time.Hour},
		},
		{
			runMsg:              "3",
			expectedBlockRanges: []time.Duration{36* time.Hour,2 * time.Hour, 2 * time.Hour, 2 * time.Hour},
		},
		{
			runMsg: "4",
			expectedBlockIndexesInPlan: [][]int{{1, 2, 3}},
			expectedBlockRanges:        []time.Duration{36* time.Hour, 2 * time.Hour, 6 * time.Hour},
		},
		{
			runMsg:              "5",
			expectedBlockRanges: []time.Duration{36* time.Hour, 2 * time.Hour, 6 * time.Hour, 2 * time.Hour},
		},
		{
			runMsg:              "6",
			expectedBlockRanges: []time.Duration{36* time.Hour, 2 * time.Hour, 6 * time.Hour, 2 * time.Hour, 2 * time.Hour},
		},
		{
			runMsg: "7",
			expectedBlockIndexesInPlan: [][]int{{1, 3, 4}},
			expectedBlockRanges:        []time.Duration{36* time.Hour, 6 * time.Hour, 2 * time.Hour, 6 * time.Hour},
		},
		{
			runMsg:              "8",
			expectedBlockRanges: []time.Duration{36* time.Hour, 6 * time.Hour, 2 * time.Hour, 6 * time.Hour, 2 * time.Hour},
		},
		{
			runMsg:              "9",
			expectedBlockRanges: []time.Duration{36* time.Hour, 6 * time.Hour, 2 * time.Hour, 6 * time.Hour, 2 * time.Hour, 2 * time.Hour},
		},
		{
			// This is tricky. There was some blocks already, because from logs we can see that
			// "level=info ts=2018-03-09T15:05:31.944625635Z caller=compact.go:394 component=tsdb msg="compact blocks" count=3 mint=1520402400000 maxt=1520596800000"
			// 54h block was created. So I deducted that 36h block was there.
			runMsg: "10",
			// There 3 compactions:
			// - the three 2h blocks into 6h - state after: []time.Duration{36* time.Hour, 6 * time.Hour, 6 * time.Hour, 2 * time.Hour, 6 * time.Hour}
			// - the two 6h blocks into 12h - state after: []time.Duration{36* time.Hour, 2 * time.Hour, 6 * time.Hour, 12 * time.Hour}
			// - 36h + 6h + 12h???
			expectedBlockIndexesInPlan: [][]int{{2, 4, 5}, {1, 2}, {0, 2}},
			expectedBlockRanges:        []time.Duration{2* time.Hour, 12 * time.Hour, 54 * time.Hour},
			// Why we ended up in this state?
			// Why 36h + 12h + 6h merged together 12h was the last one which should be ignored!
			// Why the used 12h is not deleted?
		},
	} {
		if !t.Run(tcase.runMsg, func(t *testing.T) {
			defer func() {
				metas := getMmetas(t, dir)
				fmt.Printf("---After compaction at %v ----\n", currT)
				printBlocks(metas)
				var ranges []int64
				for _, m := range metas {
					ranges = append(ranges, m.MaxTime-m.MinTime)
				}
				var expectedRanges []int64
				for _, e := range tcase.expectedBlockRanges {
					expectedRanges = append(expectedRanges, int64(e.Seconds()*1000))
				}
				testutil.Equals(t, expectedRanges, ranges)
			}()
			metas := getMmetas(t, dir)
			fmt.Printf("---Before compaction at %v ----\n", currT)
			printBlocks(metas)

			i := 0
			for {
				metas := getMmetas(t, dir)
				dirs, err := comp.Plan(dir)
				testutil.Ok(t, err)

				if len(dirs) == 0 {
					testutil.Assert(t, len(tcase.expectedBlockIndexesInPlan) == i, "Expected %d plans. Got only %d", len(tcase.expectedBlockIndexesInPlan), i)
					return
				}
				testutil.Assert(t, len(tcase.expectedBlockIndexesInPlan) >= i+1, "Expected %d plans. Got at least %d", len(tcase.expectedBlockIndexesInPlan), i+1)

				var expectedPlan []string
				for _, i := range tcase.expectedBlockIndexesInPlan[i] {
					expectedPlan = append(expectedPlan, path.Join(dir, metas[i].ULID.String()))
				}
				testutil.Equals(t, expectedPlan, dirs)

				var plannedMetas []*BlockMeta
				for _, dir := range dirs {
					meta, err := readMetaFile(dir)
					testutil.Ok(t, err)
					plannedMetas = append(plannedMetas, meta)
				}

				// Fake compaction - in same way compactor does it.
				createDummyBlock(t, dir, plannedMetas...)

				// We can kill getMmetas now.
				for _, d := range dirs {
					fmt.Println("Removing " + d)
					testutil.Ok(t, os.RemoveAll(d))
				}

				i++
			}
		}) {
			return
		}

		// Create another 2h block.
		nextT := currT + int64(time.Duration(testMinBlockSize).Seconds()*1000)
		createDummyBlock(t, dir, &BlockMeta{
			MinTime: currT,
			MaxTime: nextT,
		})
		currT = nextT
	}
}
