package vkutil

import (
	"context"
	_ "embed"
	"time"

	"github.com/gomodule/redigo/redis"
)

// IntervalSet operates like a set but with expiring intervals
type IntervalSet struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalSet creates a new empty interval set
func NewIntervalSet(keyBase string, interval time.Duration, size int) *IntervalSet {
	return &IntervalSet{keyBase: keyBase, interval: interval, size: size}
}

//go:embed lua/iset_ismember.lua
var isetIsMember string
var isetIsMemberScript = redis.NewScript(-1, isetIsMember)

// IsMember returns whether we contain the given value
func (s *IntervalSet) IsMember(ctx context.Context, vc redis.Conn, member string) (bool, error) {
	keys := s.keys()

	return redis.Bool(isetIsMemberScript.DoContext(ctx, vc, redis.Args{}.Add(len(keys)).AddFlat(keys).Add(member)...))
}

// Add adds the given value
func (s *IntervalSet) Add(ctx context.Context, vc redis.Conn, member string) error {
	key := s.keys()[0]

	vc.Send("MULTI")
	vc.Send("SADD", key, member)
	vc.Send("EXPIRE", key, s.size*int(s.interval/time.Second))
	_, err := redis.DoContext(vc, ctx, "EXEC")
	return err
}

// Rem removes the given values
func (s *IntervalSet) Rem(ctx context.Context, vc redis.Conn, members ...string) error {
	vc.Send("MULTI")
	for _, k := range s.keys() {
		vc.Send("SREM", redis.Args{}.Add(k).AddFlat(members)...)
	}
	_, err := redis.DoContext(vc, ctx, "EXEC")
	return err
}

// Clear removes all values
func (s *IntervalSet) Clear(ctx context.Context, vc redis.Conn) error {
	vc.Send("MULTI")
	for _, k := range s.keys() {
		vc.Send("DEL", k)
	}
	_, err := redis.DoContext(vc, ctx, "EXEC")
	return err
}

func (s *IntervalSet) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
