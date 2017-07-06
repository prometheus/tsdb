package tsdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompactionSelect(t *testing.T) {
	opts := &compactorOptions{
		blockRanges: []int64{
			20,
			60,
			240,
			720,
			2160,
		},
	}

	cases := []struct {
		blocks  []dirMeta
		planned [][]string
	}{
		{
			blocks: []dirMeta{
				{
					dir: "1",
					meta: &BlockMeta{
						MinTime: 0,
						MaxTime: 20,
					},
				},
			},
			planned: nil,
		},
		{
			blocks: []dirMeta{
				{
					dir: "1",
					meta: &BlockMeta{
						MinTime: 0,
						MaxTime: 20,
					},
				},
				{
					dir: "2",
					meta: &BlockMeta{
						MinTime: 20,
						MaxTime: 40,
					},
				},
				{
					dir: "3",
					meta: &BlockMeta{
						MinTime: 40,
						MaxTime: 60,
					},
				},
			},
			planned: [][]string{{"1", "2", "3"}},
		},
		{
			blocks: []dirMeta{
				{
					dir: "1",
					meta: &BlockMeta{
						MinTime: 0,
						MaxTime: 20,
					},
				},
				{
					dir: "2",
					meta: &BlockMeta{
						MinTime: 20,
						MaxTime: 40,
					},
				},
				{
					dir: "3",
					meta: &BlockMeta{
						MinTime: 40,
						MaxTime: 60,
					},
				},
				{
					dir: "4",
					meta: &BlockMeta{
						MinTime: 60,
						MaxTime: 120,
					},
				},
				{
					dir: "5",
					meta: &BlockMeta{
						MinTime: 120,
						MaxTime: 180,
					},
				},
			},
			planned: [][]string{{"1", "2", "3"}}, // We still need 0-60 to compact 0-240
		},
		{
			blocks: []dirMeta{
				{
					dir: "1",
					meta: &BlockMeta{
						MinTime: 0,
						MaxTime: 20,
					},
				},
				{
					dir: "2",
					meta: &BlockMeta{
						MinTime: 20,
						MaxTime: 40,
					},
				},
				{
					dir: "3",
					meta: &BlockMeta{
						MinTime: 40,
						MaxTime: 60,
					},
				},
				{
					dir: "4",
					meta: &BlockMeta{
						MinTime: 60,
						MaxTime: 120,
					},
				},
				{
					dir: "5",
					meta: &BlockMeta{
						MinTime: 120,
						MaxTime: 180,
					},
				},
				{
					dir: "6",
					meta: &BlockMeta{
						MinTime: 720,
						MaxTime: 960,
					},
				},
				{
					dir: "7",
					meta: &BlockMeta{
						MinTime: 1200,
						MaxTime: 1440,
					},
				},
			},
			planned: [][]string{{"6", "7"}},
		},
	}

	c := &compactor{
		opts: opts,
	}
	sliceDirs := func(dms []dirMeta) [][]string {
		if len(dms) == 0 {
			return nil
		}
		var res []string
		for _, dm := range dms {
			res = append(res, dm.dir)
		}
		return [][]string{res}
	}

	for _, tc := range cases {
		require.Equal(t, tc.planned, sliceDirs(c.selectDirs(tc.blocks)))
	}
}
