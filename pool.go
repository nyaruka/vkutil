package vkutil

import (
	"net/url"
	"strings"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// WithMaxActive configures maximum number of concurrent connections to allow
func WithMaxActive(v int) func(*valkey.Pool) {
	return func(vp *valkey.Pool) { vp.MaxActive = v }
}

// WithMaxIdle configures the maximum number of idle connections to keep
func WithMaxIdle(v int) func(*valkey.Pool) {
	return func(vp *valkey.Pool) { vp.MaxIdle = v }
}

// WithIdleTimeout configures how long to wait before reaping a connection
func WithIdleTimeout(v time.Duration) func(*valkey.Pool) {
	return func(vp *valkey.Pool) { vp.IdleTimeout = v }
}

// NewPool creates a new pool with the given options
func NewPool(redisURL string, options ...func(*valkey.Pool)) (*valkey.Pool, error) {
	parsedURL, err := url.Parse(redisURL)
	if err != nil {
		return nil, err
	}

	dial := func() (valkey.Conn, error) {
		conn, err := valkey.Dial("tcp", parsedURL.Host)
		if err != nil {
			return nil, err
		}

		// send auth if required
		if parsedURL.User != nil {
			pass, authRequired := parsedURL.User.Password()
			if authRequired {
				if _, err := conn.Do("AUTH", pass); err != nil {
					conn.Close()
					return nil, err
				}
			}
		}

		// switch to the right DB
		_, err = conn.Do("SELECT", strings.TrimLeft(parsedURL.Path, "/"))
		return conn, err
	}

	vp := &valkey.Pool{
		MaxActive:   32,
		MaxIdle:     4,
		IdleTimeout: 180 * time.Second,
		Wait:        true, // makes callers wait for a connection
		Dial:        dial,
	}

	for _, o := range options {
		o(vp)
	}

	return vp, nil
}
