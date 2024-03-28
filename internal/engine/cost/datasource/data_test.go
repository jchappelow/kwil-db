package datasource

import (
	"context"
	"fmt"
	"testing"
)

func TestStreamingAPI(t *testing.T) {
	data := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
	ctx := context.TODO()

	newData := Tap(ctx, data).
		Filter(ctx, func(x int) bool { return x%2 == 0 }).
		Transform(ctx, func(x int) int { return x * 2 }).
		Collect(ctx)

	fmt.Println(newData)

	fmt.Println(data)
	newData = Tap2[int, float64](ctx, data).
		Transform().
		Filter(ctx, func(x int) bool { return x%2 == 0 }).
		Collect(ctx)

	fmt.Println(newData)
}
