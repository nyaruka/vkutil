package vkutil

import (
	"context"
	_ "embed"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// RateLimiter limits how often something can happen per key, over a fixed window starting at a key's first use
type RateLimiter struct {
	limit  int
	window time.Duration
}

// NewRateLimiter creates a new rate limiter which allows limit uses of a key per window. It panics if limit or window
// aren't positive, as neither can be honoured.
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	if limit <= 0 {
		panic("limit must be greater than zero")
	}
	if window <= 0 {
		panic("window must be greater than zero")
	}

	return &RateLimiter{limit: limit, window: window}
}

//go:embed lua/rlimit_incr.lua
var rlimitIncr string
var rlimitIncrScript = valkey.NewScript(1, rlimitIncr)

// Allow counts a use of the given key and returns whether it's within the limit. Only the first use in a window sets
// the key's expiry, so steady use can't keep extending the window.
func (r *RateLimiter) Allow(ctx context.Context, vc valkey.Conn, key string) (bool, error) {
	count, err := valkey.Int(rlimitIncrScript.DoContext(ctx, vc, key, r.window.Milliseconds()))
	if err != nil {
		return false, err
	}
	return count <= r.limit, nil
}
