package vkutil_test

import (
	"context"
	"testing"
	"time"

	valkey "github.com/gomodule/redigo/redis"

	"github.com/nyaruka/vkutil"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter(t *testing.T) {
	ctx := context.Background()
	vc := assertvk.ClaimDB(t).Pool().Get()
	defer vc.Close()

	assertAllow := func(r *vkutil.RateLimiter, id string, expected bool) {
		t.Helper()
		allowed, err := r.Allow(ctx, vc, id)
		assert.NoError(t, err)
		assert.Equal(t, expected, allowed)
	}

	limiter := vkutil.NewRateLimiter("typing", 3, time.Minute)

	assertAllow(limiter, "foo", true)
	assertAllow(limiter, "foo", true)
	assertAllow(limiter, "foo", true)
	assertAllow(limiter, "foo", false)
	assertAllow(limiter, "foo", false)

	// ids are limited independently
	assertAllow(limiter, "bar", true)

	assertvk.Get(t, vc, "typing:foo", "5")
	assertvk.Get(t, vc, "typing:bar", "1")

	// limiters with different key bases don't share counts
	assertAllow(vkutil.NewRateLimiter("other", 3, time.Minute), "foo", true)
	assertvk.Get(t, vc, "other:foo", "1")

	// the window starts at the first use and isn't extended by later uses
	_, err := vc.Do("PEXPIRE", "typing:foo", 5000)
	require.NoError(t, err)
	assertAllow(limiter, "foo", false)

	ttl, err := valkey.Int(vc.Do("PTTL", "typing:foo"))
	require.NoError(t, err)
	assert.LessOrEqual(t, ttl, 5000)

	// once the window ends, uses are allowed again
	_, err = vc.Do("DEL", "typing:foo")
	require.NoError(t, err)
	assertAllow(limiter, "foo", true)

	ttl, err = valkey.Int(vc.Do("PTTL", "typing:foo"))
	require.NoError(t, err)
	assert.Greater(t, ttl, 59000)

	assert.Panics(t, func() { vkutil.NewRateLimiter("typing", 0, time.Minute) })
	assert.Panics(t, func() { vkutil.NewRateLimiter("typing", 3, 0) })
}
