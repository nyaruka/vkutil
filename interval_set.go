package vkutil

import (
	"context"
	_ "embed"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// IntervalSet operates like a set but with expiring intervals
type IntervalSet struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
	hashTags bool          // whether to use hash tags in keys
}

// NewIntervalSet creates a new empty interval set
func NewIntervalSet(keyBase string, interval time.Duration, size int, hashTags bool) *IntervalSet {
	return &IntervalSet{keyBase: keyBase, interval: interval, size: size, hashTags: hashTags}
}

//go:embed lua/iset_ismember.lua
var isetIsMember string
var isetIsMemberScript = valkey.NewScript(-1, isetIsMember)

// IsMember returns whether we contain the given value
func (s *IntervalSet) IsMember(ctx context.Context, vc valkey.Conn, member string) (bool, error) {
	keys := s.keys()

	return valkey.Bool(isetIsMemberScript.DoContext(ctx, vc, valkey.Args{}.Add(len(keys)).AddFlat(keys).Add(member)...))
}

// Add adds the given value
func (s *IntervalSet) Add(ctx context.Context, vc valkey.Conn, member string) error {
	key := s.keys()[0]

	vc.Send("MULTI")
	vc.Send("SADD", key, member)
	vc.Send("EXPIRE", key, s.size*int(s.interval/time.Second))
	_, err := valkey.DoContext(vc, ctx, "EXEC")
	return err
}

// Rem removes the given values
func (s *IntervalSet) Rem(ctx context.Context, vc valkey.Conn, members ...string) error {
	vc.Send("MULTI")
	for _, k := range s.keys() {
		vc.Send("SREM", valkey.Args{}.Add(k).AddFlat(members)...)
	}
	_, err := valkey.DoContext(vc, ctx, "EXEC")
	return err
}

// Clear removes all values
func (s *IntervalSet) Clear(ctx context.Context, vc valkey.Conn) error {
	vc.Send("MULTI")
	for _, k := range s.keys() {
		vc.Send("DEL", k)
	}
	_, err := valkey.DoContext(vc, ctx, "EXEC")
	return err
}

func (s *IntervalSet) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size, s.hashTags)
}
