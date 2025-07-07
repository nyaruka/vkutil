package vkutil

import (
	"context"
	_ "embed"
	"time"

	"github.com/gomodule/redigo/redis"
)

// IntervalSeries returns all values from interval based hashes.
type IntervalSeries struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
	hashTags bool          // whether to use hash tags in keys
}

// NewIntervalSeries creates a new empty series
func NewIntervalSeries(keyBase string, interval time.Duration, size int, hashTags bool) *IntervalSeries {
	return &IntervalSeries{keyBase: keyBase, interval: interval, size: size, hashTags: hashTags}
}

// Record increments the value of field by value in the current interval
func (s *IntervalSeries) Record(ctx context.Context, rc redis.Conn, field string, value int64) error {
	currKey := s.keys()[0]

	rc.Send("MULTI")
	rc.Send("HINCRBY", currKey, field, value)
	rc.Send("EXPIRE", currKey, s.size*int(s.interval/time.Second))
	_, err := redis.DoContext(rc, ctx, "EXEC")
	return err
}

//go:embed lua/iseries_get.lua
var iseriesGet string
var iseriesGetScript = redis.NewScript(-1, iseriesGet)

// Get gets the values of field in all intervals
func (s *IntervalSeries) Get(ctx context.Context, rc redis.Conn, field string) ([]int64, error) {
	keys := s.keys()
	args := redis.Args{}.Add(len(keys)).AddFlat(keys).Add(field)

	return redis.Int64s(iseriesGetScript.DoContext(ctx, rc, args...))
}

// Total gets the total value of field across all intervals
func (s *IntervalSeries) Total(ctx context.Context, rc redis.Conn, field string) (int64, error) {
	vals, err := s.Get(ctx, rc, field)
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
	return intervalKeys(s.keyBase, s.interval, s.size, s.hashTags)
}
