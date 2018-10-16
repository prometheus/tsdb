package tsdb

import (
	"context"
	"testing"
)

func TestSortSliceContext(t *testing.T) {
	s := []int{3, 2, 1}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := SortSliceContext(ctx, s, func(i, j int) bool {
		return s[i] < s[j]
	})
	if err == nil {
		t.Fatalf("Expected err")
	}
}
