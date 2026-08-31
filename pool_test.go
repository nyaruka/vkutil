package vkutil_test

import (
	"testing"
	"time"

	vkutil "github.com/nyaruka/vkutil"
	"github.com/stretchr/testify/assert"
)

func TestNewPool(t *testing.T) {
	// check defaults
	vp, err := vkutil.NewPool("valkey://valkey8:6379/15")
	assert.NoError(t, err)
	assert.Equal(t, 32, vp.MaxActive)
	assert.Equal(t, 4, vp.MaxIdle)
	assert.Equal(t, 180*time.Second, vp.IdleTimeout)

	vp, err = vkutil.NewPool("valkey://valkey8:6379/15", vkutil.WithMaxActive(10), vkutil.WithMaxIdle(3), vkutil.WithIdleTimeout(time.Minute))
	assert.NoError(t, err)
	assert.Equal(t, 10, vp.MaxActive)
	assert.Equal(t, 3, vp.MaxIdle)
	assert.Equal(t, time.Minute, vp.IdleTimeout)
}

func TestNewPoolURLs(t *testing.T) {
	// scheme determines whether we use TLS and anything else is rejected
	for _, scheme := range []string{"valkey", "redis", "valkeys", "rediss"} {
		_, err := vkutil.NewPool(scheme + "://valkey8:6379/15")
		assert.NoError(t, err, "unexpected error for scheme %s", scheme)
	}

	_, err := vkutil.NewPool("http://valkey8:6379/15")
	assert.EqualError(t, err, "unsupported scheme in valkey URL: http")

	// database defaults to 0 if not given
	_, err = vkutil.NewPool("valkey://valkey8:6379")
	assert.NoError(t, err)

	_, err = vkutil.NewPool("valkey://valkey8:6379/notanumber")
	assert.EqualError(t, err, "invalid database in valkey URL: notanumber")
}
