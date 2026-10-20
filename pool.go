package vkutil

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// PoolOption configures a pool created by NewPool
type PoolOption func(*poolOptions)

type poolOptions struct {
	maxActive      int
	maxIdle        int
	idleTimeout    time.Duration
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

// WithMaxActive configures maximum number of concurrent connections to allow
func WithMaxActive(v int) PoolOption {
	return func(o *poolOptions) { o.maxActive = v }
}

// WithMaxIdle configures the maximum number of idle connections to keep
func WithMaxIdle(v int) PoolOption {
	return func(o *poolOptions) { o.maxIdle = v }
}

// WithIdleTimeout configures how long to wait before reaping a connection
func WithIdleTimeout(v time.Duration) PoolOption {
	return func(o *poolOptions) { o.idleTimeout = v }
}

// WithConnectTimeout configures how long to wait when dialing a new connection, zero meaning no limit
func WithConnectTimeout(v time.Duration) PoolOption {
	return func(o *poolOptions) { o.connectTimeout = v }
}

// WithReadTimeout configures how long to wait for a reply to a command, zero meaning no limit. Callers which use
// blocking commands (e.g. BLPOP) must set this higher than the longest time they block for.
func WithReadTimeout(v time.Duration) PoolOption {
	return func(o *poolOptions) { o.readTimeout = v }
}

// WithWriteTimeout configures how long to wait when writing a command, zero meaning no limit
func WithWriteTimeout(v time.Duration) PoolOption {
	return func(o *poolOptions) { o.writeTimeout = v }
}

// NewPool creates a new pool with the given options. The URL takes the form
// scheme://[[username][:password]@]host:port[/db] where the scheme selects whether the connection
// is encrypted - valkey:// and redis:// are plaintext, valkeys:// and rediss:// use TLS. If a
// username is given then credentials are sent as an ACL style AUTH, otherwise as a password only
// AUTH. The database defaults to 0.
func NewPool(redisURL string, options ...PoolOption) (*valkey.Pool, error) {
	opts := &poolOptions{
		maxActive:   32,
		maxIdle:     4,
		idleTimeout: 180 * time.Second,

		// without these a connection which goes half-open, e.g. after a network partition or server failover, blocks
		// its caller indefinitely, holding its slot in the pool
		connectTimeout: 5 * time.Second,
		readTimeout:    10 * time.Second,
		writeTimeout:   10 * time.Second,
	}
	for _, o := range options {
		o(opts)
	}

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
		conn, err := valkey.Dial("tcp", parsedURL.Host,
			valkey.DialUseTLS(useTLS),
			valkey.DialConnectTimeout(opts.connectTimeout),
			valkey.DialReadTimeout(opts.readTimeout),
			valkey.DialWriteTimeout(opts.writeTimeout),
		)
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

	return &valkey.Pool{
		MaxActive:   opts.maxActive,
		MaxIdle:     opts.maxIdle,
		IdleTimeout: opts.idleTimeout,
		Wait:        true, // makes callers wait for a connection
		Dial:        dial,
	}, nil
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
