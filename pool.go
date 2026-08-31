package vkutil

import (
	"fmt"
	"net/url"
	"strconv"
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

// NewPool creates a new pool with the given options. The URL takes the form
// scheme://[[username][:password]@]host:port[/db] where the scheme selects whether the connection
// is encrypted - valkey:// and redis:// are plaintext, valkeys:// and rediss:// use TLS. If a
// username is given then credentials are sent as an ACL style AUTH, otherwise as a password only
// AUTH. The database defaults to 0.
func NewPool(redisURL string, options ...func(*valkey.Pool)) (*valkey.Pool, error) {
	parsedURL, err := url.Parse(redisURL)
	if err != nil {
		return nil, err
	}

	var useTLS bool
	switch parsedURL.Scheme {
	case "valkey", "redis":
	case "valkeys", "rediss":
		useTLS = true
	default:
		return nil, fmt.Errorf("unsupported scheme in valkey URL: %s", parsedURL.Scheme)
	}

	db := strings.TrimLeft(parsedURL.Path, "/")
	if db == "" {
		db = "0"
	}
	if _, err := strconv.Atoi(db); err != nil {
		return nil, fmt.Errorf("invalid database in valkey URL: %s", db)
	}

	dial := func() (valkey.Conn, error) {
		conn, err := valkey.Dial("tcp", parsedURL.Host, valkey.DialUseTLS(useTLS))
		if err != nil {
			return nil, err
		}

		// from here on we must close the connection ourselves if anything fails - the pool discards
		// a connection returned alongside an error without closing it
		if err := initConn(conn, parsedURL.User, db); err != nil {
			conn.Close()
			return nil, err
		}

		return conn, nil
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

// initConn authenticates a newly dialed connection if the URL carried credentials, and switches it
// to the right database.
func initConn(conn valkey.Conn, user *url.Userinfo, db string) error {
	if user != nil {
		username := user.Username()
		password, _ := user.Password()

		if username != "" {
			// ACL style auth, i.e. valkey://username:password@host
			if _, err := conn.Do("AUTH", username, password); err != nil {
				return err
			}
		} else if password != "" {
			// legacy password only auth, i.e. valkey://:password@host
			if _, err := conn.Do("AUTH", password); err != nil {
				return err
			}
		}
	}

	_, err := conn.Do("SELECT", db)
	return err
}
