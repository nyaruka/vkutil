package vkutil

import (
	"context"
	_ "embed"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// CappedZSet is a sorted set but enforces a cap on size
type CappedZSet struct {
	key    string
	cap    int
	expire time.Duration
}

// NewCappedZSet creates a new capped sorted set. It panics if cap or expire aren't positive, as
// neither can be honoured: a non-positive cap empties the set on every add, and a non-positive
// expire deletes it.
func NewCappedZSet(key string, cap int, expire time.Duration) *CappedZSet {
	if cap <= 0 {
		panic("cap must be greater than zero")
	}
	if expire <= 0 {
		panic("expire must be greater than zero")
	}

	return &CappedZSet{key: key, cap: cap, expire: expire}
}

//go:embed lua/czset_add.lua
var czsetAdd string
var czsetAddScript = valkey.NewScript(1, czsetAdd)

// Add adds an element to the set, if its score puts in the top `cap` members
func (z *CappedZSet) Add(ctx context.Context, vc valkey.Conn, member string, score float64) error {
	_, err := czsetAddScript.DoContext(ctx, vc, z.key, score, member, z.cap, z.expire.Milliseconds())
	return err
}

// Card returns the cardinality of the set
func (z *CappedZSet) Card(ctx context.Context, vc valkey.Conn) (int, error) {
	return valkey.Int(valkey.DoContext(vc, ctx, "ZCARD", z.key))
}

// Members returns all members of the set, ordered by ascending rank
func (z *CappedZSet) Members(ctx context.Context, vc valkey.Conn) ([]string, []float64, error) {
	return StringsWithScores(valkey.DoContext(vc, ctx, "ZRANGE", z.key, 0, -1, "WITHSCORES"))
}
