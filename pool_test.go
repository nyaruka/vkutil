package vkutil_test

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	vkutil "github.com/nyaruka/vkutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestNewPoolTLS(t *testing.T) {
	// a listener which records the first byte each client sends and then hangs up
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	firstBytes := make(chan byte, 2)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			buf := make([]byte, 1)
			if _, err := io.ReadFull(conn, buf); err == nil {
				firstBytes <- buf[0]
			}
			conn.Close()
		}
	}()

	firstByte := func(url string) byte {
		vp, err := vkutil.NewPool(url)
		require.NoError(t, err)

		vc := vp.Get()
		vc.Do("PING") // expected to fail, we only care what went over the wire
		vc.Close()

		select {
		case b := <-firstBytes:
			return b
		case <-time.After(time.Second * 30):
			t.Fatalf("timed out waiting for a connection from %s", url)
			return 0
		}
	}

	// a plaintext scheme speaks RESP directly, so the first byte is an array marker
	assert.Equal(t, byte('*'), firstByte("valkey://"+ln.Addr().String()+"/0"))

	// whereas a TLS scheme must begin a handshake, so the first byte is a TLS record type
	assert.Equal(t, byte(0x16), firstByte("valkeys://"+ln.Addr().String()+"/0"))
}

func TestNewPoolTimeouts(t *testing.T) {
	// a listener which accepts connections and reads from them but never replies, like a half-open connection
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, conn)
		}
	}()

	vp, err := vkutil.NewPool("valkey://"+ln.Addr().String()+"/0", vkutil.WithReadTimeout(100*time.Millisecond))
	require.NoError(t, err)

	start := time.Now()
	vc := vp.Get()
	_, err = vc.Do("PING")
	vc.Close()

	var netErr net.Error
	if assert.True(t, errors.As(err, &netErr), "expected a network error, got %v", err) {
		assert.True(t, netErr.Timeout())
	}
	assert.Less(t, time.Since(start), 5*time.Second)
}
