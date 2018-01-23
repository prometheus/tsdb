package chunkenc

import "testing"

func TestBitRange(t *testing.T) {
	fixtures := []struct {
		x      int64
		nbits  int
		result bool
	}{
		{-8191, 13, false},
		{-8192, 13, false},
		{-8191, 13, false},

		{8191, 13, false},
		{8192, 13, false},
		{8191, 13, false},

		{-8191, 14, true},
		{-8192, 14, false},
		{-8193, 14, false},

		{8191, 14, true},
		{8192, 14, true},
		{8193, 14, false},

		{-8191, 15, true},
		{-8192, 15, true},
		{-8193, 15, true},

		{8191, 15, true},
		{8192, 15, true},
		{8193, 15, true},
	}

	for i, f := range fixtures {
		if bitRange(f.x, f.nbits) != f.result {
			t.Fatalf("fixture %d returned unexpected result", i)
		}
	}
}

func BenchmarkBitRange(b *testing.B) {
	bitFixtures := []int64{-1, 1}
	for i := 0; i < b.N; i++ {
		_ = bitRange(bitFixtures[i%2], 14)
	}
}
