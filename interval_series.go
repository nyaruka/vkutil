package vkutil

import (
	"context"
	_ "embed"
	"time"

	"github.com/valkey-io/valkey-go"
)

// IntervalSeries returns all values from interval based hashes.
type IntervalSeries struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalSeries creates a new empty series
func NewIntervalSeries(keyBase string, interval time.Duration, size int) *IntervalSeries {
	return &IntervalSeries{keyBase: keyBase, interval: interval, size: size}
}

// Record increments the value of field by value in the current interval
func (s *IntervalSeries) Record(ctx context.Context, vc valkey.Client, field string, value int64) error {
	currKey := s.keys()[0]

	vc.Do(ctx, vc.B().Multi().Build())

	vc.Do(ctx, vc.B().Hincrby().Key(currKey).Field(field).Increment(value).Build())
	vc.Do(ctx, vc.B().Expire().Key(currKey).Seconds(int64(s.size)*int64(s.interval/time.Second)).Build())
	return vc.Do(ctx, vc.B().Exec().Build()).Error()
}

//go:embed lua/iseries_get.lua
var iseriesGet string
var iseriesGetScript = valkey.NewLuaScript(iseriesGet)

// Get gets the values of field in all intervals
func (s *IntervalSeries) Get(ctx context.Context, vc valkey.Client, field string) ([]int64, error) {
	keys := s.keys()
	res := iseriesGetScript.Exec(ctx, vc, keys, []string{field})
	return res.AsIntSlice()
}

// Total gets the total value of field across all intervals
func (s *IntervalSeries) Total(ctx context.Context, vc valkey.Client, field string) (int64, error) {
	vals, err := s.Get(ctx, vc, field)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, v := range vals {
		total += v
	}
	return total, nil
}

func (s *IntervalSeries) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
