package tsdb

import (
	"context"
	"sort"
)

func SortSliceContext(ctx context.Context, slice interface{}, less func(i, j int) bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			tmp, ok := r.(error)
			if ok {
				err = tmp
			}
		}
	}()

	sort.Slice(slice, func(i, j int) bool {
		select {
		case <-ctx.Done():
			// Use the panic to exit this without having to complete the sort
			panic(ctx.Err())
		default:
			return less(i, j)
		}
	})

	return nil
}
