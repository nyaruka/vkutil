package vkutil_test

import (
	"testing"
	"time"

	vkutil "github.com/nyaruka/vkutil"
	"github.com/stretchr/testify/assert"
)

func TestNewPool(t *testing.T) {
	// check defaults
	rp, err := vkutil.NewPool("valkey://valkey8:6379/15")
	assert.NoError(t, err)
	assert.Equal(t, 32, rp.MaxActive)
	assert.Equal(t, 4, rp.MaxIdle)
	assert.Equal(t, 180*time.Second, rp.IdleTimeout)

	rp, err = vkutil.NewPool("valkey://valkey8:6379/15", vkutil.WithMaxActive(10), vkutil.WithMaxIdle(3), vkutil.WithIdleTimeout(time.Minute))
	assert.NoError(t, err)
	assert.Equal(t, 10, rp.MaxActive)
	assert.Equal(t, 3, rp.MaxIdle)
	assert.Equal(t, time.Minute, rp.IdleTimeout)
}
