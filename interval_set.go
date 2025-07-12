package vkutil

import (
	"context"
	_ "embed"
	"time"

	"github.com/valkey-io/valkey-go"
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
var isetIsMemberScript = valkey.NewLuaScript(isetIsMember)

// IsMember returns whether we contain the given value
func (s *IntervalSet) IsMember(ctx context.Context, vc valkey.Client, member string) (bool, error) {
	keys := s.keys()
	return isetIsMemberScript.Exec(ctx, vc, keys, []string{member}).AsBool()
}

// Add adds the given value
func (s *IntervalSet) Add(ctx context.Context, vc valkey.Client, member string) error {
	key := s.keys()[0]

	vc.Do(ctx, vc.B().Multi().Build())
	vc.Do(ctx, vc.B().Sadd().Key(key).Member(member).Build())
	vc.Do(ctx, vc.B().Expire().Key(key).Seconds(int64(s.size)*int64(s.interval/time.Second)).Build())
	return vc.Do(ctx, vc.B().Exec().Build()).Error()
}

// Rem removes the given values
func (s *IntervalSet) Rem(ctx context.Context, vc valkey.Client, members ...string) error {

	vc.Do(ctx, vc.B().Multi().Build())
	for _, k := range s.keys() {
		vc.Do(ctx, vc.B().Srem().Key(k).Member(members...).Build())
	}
	return vc.Do(ctx, vc.B().Exec().Build()).Error()
}

// Clear removes all values
func (s *IntervalSet) Clear(ctx context.Context, vc valkey.Client) error {

	vc.Do(ctx, vc.B().Multi().Build())
	for _, k := range s.keys() {
		vc.Do(ctx, vc.B().Del().Key(k).Build())
	}
	return vc.Do(ctx, vc.B().Exec().Build()).Error()
}

func (s *IntervalSet) keys() []string {
	return intervalKeys(s.keyBase, s.interval, s.size)
}
