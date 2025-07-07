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
