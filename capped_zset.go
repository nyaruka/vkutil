package vkutil

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
)

// CappedZSet is a sorted set but enforces a cap on size
type CappedZSet struct {
	key    string
	cap    int
	expire time.Duration
}

// NewCappedZSet creates a new capped sorted set
func NewCappedZSet(key string, cap int, expire time.Duration) *CappedZSet {
	return &CappedZSet{key: key, cap: cap, expire: expire}
}

//go:embed lua/czset_add.lua
var czsetAdd string
var czsetAddScript = valkey.NewLuaScript(czsetAdd)

// Add adds an element to the set, if its score puts in the top `cap` members
func (z *CappedZSet) Add(ctx context.Context, vc valkey.Client, member string, score float64) error {
	err := czsetAddScript.Exec(ctx, vc, []string{z.key}, []string{fmt.Sprintf("%f", score), member, strconv.Itoa(z.cap), strconv.Itoa(int(z.expire / time.Second))}).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		return fmt.Errorf("error adding to capped zset %s: %w", z.key, err)
	}
	return nil
}

// Card returns the cardinality of the set
func (z *CappedZSet) Card(ctx context.Context, vc valkey.Client) (int, error) {
	result, err := vc.Do(ctx, vc.B().Zcard().Key(z.key).Build()).AsInt64()
	if err != nil && !valkey.IsValkeyNil(err) {

		return 0, fmt.Errorf("error getting cardinality of capped zset %s: %w", z.key, err)
	}
	return int(result), nil
}

// Members returns all members of the set, ordered by ascending rank
func (z *CappedZSet) Members(ctx context.Context, vc valkey.Client) ([]string, []float64, error) {
	members, err := vc.Do(ctx, vc.B().Zrange().Key(z.key).Min("0").Max("-1").Withscores().Build()).AsZScores()
	if err != nil && !valkey.IsValkeyNil(err) {
		return nil, nil, fmt.Errorf("error getting members of capped zset %s: %w", z.key, err)
	}

	var keys []string
	var scores []float64
	for _, zs := range members {
		keys = append(keys, zs.Member)
		scores = append(scores, zs.Score)
	}

	return keys, scores, nil
}
