package vkutil

import (
	"context"
	_ "embed"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// RateLimiter limits how often something can happen per id, over a fixed window starting at an id's first use
type RateLimiter struct {
	keyBase string
	limit   int
	window  time.Duration
}

// NewRateLimiter creates a new rate limiter which allows limit uses of an id per window. It panics if limit or window
// aren't positive, as neither can be honoured.
func NewRateLimiter(keyBase string, limit int, window time.Duration) *RateLimiter {
	if limit <= 0 {
		panic("limit must be greater than zero")
	}
	if window <= 0 {
		panic("window must be greater than zero")
	}

	return &RateLimiter{keyBase: keyBase, limit: limit, window: window}
}

//go:embed lua/rlimit_incr.lua
var rlimitIncr string
var rlimitIncrScript = valkey.NewScript(1, rlimitIncr)

// Allow counts a use of the given id and returns whether it's within the limit. Only the first use in a window sets
// the id's expiry, so steady use can't keep extending the window.
func (r *RateLimiter) Allow(ctx context.Context, vc valkey.Conn, id string) (bool, error) {
	count, err := valkey.Int(rlimitIncrScript.DoContext(ctx, vc, r.keyBase+":"+id, r.window.Milliseconds()))
	if err != nil {
		return false, err
	}
	return count <= r.limit, nil
}
