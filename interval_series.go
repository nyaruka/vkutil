package vkutil

import (
	"context"
	_ "embed"
	"time"

	valkey "github.com/gomodule/redigo/redis"
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
func (s *IntervalSeries) Record(ctx context.Context, vc valkey.Conn, field string, value int64) error {
	currKey := s.keys()[0]

	vc.Send("MULTI")
	vc.Send("HINCRBY", currKey, field, value)
	vc.Send("EXPIRE", currKey, s.size*int(s.interval/time.Second))
	_, err := valkey.DoContext(vc, ctx, "EXEC")
	return err
}

//go:embed lua/iseries_get.lua
var iseriesGet string
var iseriesGetScript = valkey.NewScript(-1, iseriesGet)

// Get gets the values of field in all intervals
func (s *IntervalSeries) Get(ctx context.Context, vc valkey.Conn, field string) ([]int64, error) {
	keys := s.keys()
	args := valkey.Args{}.Add(len(keys)).AddFlat(keys).Add(field)

	return valkey.Int64s(iseriesGetScript.DoContext(ctx, vc, args...))
}

// Total gets the total value of field across all intervals
func (s *IntervalSeries) Total(ctx context.Context, vc valkey.Conn, field string) (int64, error) {
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
