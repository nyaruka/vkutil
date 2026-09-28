package assertvk

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// Each test binary claims its own logical database so that concurrent test runs - other packages, other
// worktrees, other repos, other languages - sharing a valkey instance can't interfere with each other. A
// claim expires unless its owner keeps renewing it, so it evaporates shortly after the run that owns it ends,
// and whatever a dead run left behind is flushed on the next claim.
//
// By default each claim is a key in the claimed database itself, with databases tried from the top of the
// keyspace down - fine for a throwaway instance, but a test that flushes its database also drops its claim.
//
// Instances shared more widely can instead dedicate a coordination database to claims on a pool of databases
// (see Coordinate). Its testdbs:claims sorted set holds each claimed database scored by when its claim expires,
// and its testdbs:owners hash holds each claimed database's owner. Claiming and renewing are the Lua scripts
// below, which any client sharing the pool must use as-is. Claims live outside the claimed databases, so tests
// can flush their own database freely.

const (
	claimTTL = 30 * time.Second

	claimsKey = "testdbs:claims"
	ownersKey = "testdbs:owners"

	claimKey      = "__assertvk_claim__" // an uncoordinated claim
	claimBandSize = 16
)

// a coordination database and the pool of databases it coordinates claims on
type coordination struct {
	db, first, last int
}

var (
	coordMu      sync.Mutex
	coord        *coordination
	claimStarted bool
)

// Coordinate makes this binary claim its test database from the pool first..last through the coordination
// database db, falling back to uncoordinated claims on an instance without that database. Every binary sharing
// the pool must use the same values, and it must be called before the test database is first used, e.g. from
// the init of a package every test imports.
func Coordinate(db, first, last int) {
	if db < 0 || first < 0 || first > last || (db >= first && db <= last) {
		panic(fmt.Sprintf("invalid test database coordination: db %d, pool %d-%d", db, first, last))
	}

	coordMu.Lock()
	defer coordMu.Unlock()

	if claimStarted {
		panic("assertvk.Coordinate called after the test database was claimed")
	}
	coord = &coordination{db, first, last}
}

// claims the first database in the pool (ARGV[1]..ARGV[2]) which isn't claimed or whose claim has expired, for
// owner ARGV[3] with a TTL of ARGV[4] milliseconds - returning the database or -1 if none are free
var claimScript = valkey.NewScript(2, `
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
for _, db in ipairs(redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", now)) do
	redis.call("ZREM", KEYS[1], db)
	redis.call("HDEL", KEYS[2], db)
end
for db = tonumber(ARGV[1]), tonumber(ARGV[2]) do
	if not redis.call("ZSCORE", KEYS[1], db) then
		redis.call("ZADD", KEYS[1], now + tonumber(ARGV[4]), db)
		redis.call("HSET", KEYS[2], db, ARGV[3])
		return db
	end
end
return -1
`)

// renews the claim on database ARGV[1] by owner ARGV[2] with a TTL of ARGV[3] milliseconds - returning 0 if
// that owner no longer holds it
var renewScript = valkey.NewScript(2, `
if redis.call("HGET", KEYS[2], ARGV[1]) ~= ARGV[2] then
	return 0
end
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call("ZADD", KEYS[1], "XX", now + tonumber(ARGV[3]), ARGV[1])
return 1
`)

// this binary's claimed database - claimed on first use
var claimedDB = sync.OnceValues(func() (int, error) {
	hostname, _ := os.Hostname()

	owner := fmt.Sprintf("%s:%d", hostname, os.Getpid())

	coordMu.Lock()
	claimStarted = true
	c := coord
	coordMu.Unlock()

	db, used, err := claim(getHostAddress(), owner, c, time.Now().Add(3*time.Minute))
	if err == nil {
		go keepClaim(db, owner, used)
	}
	return db, err
})

// claim finds and claims an unclaimed database, clearing anything a previous owner left behind, and returns
// the coordination used, if any. If every database is claimed it keeps trying until the deadline - claims
// from dead runs expire.
func claim(addr, owner string, c *coordination, deadline time.Time) (int, *coordination, error) {
	ctx := context.Background()

	conn, err := valkey.Dial("tcp", addr)
	if err != nil {
		return 0, nil, fmt.Errorf("error connecting to valkey: %w", err)
	}
	defer conn.Close()

	cfg, err := valkey.Strings(valkey.DoContext(conn, ctx, "CONFIG", "GET", "databases"))
	if err != nil || len(cfg) != 2 {
		return 0, nil, fmt.Errorf("error reading valkey database count: %w", err)
	}
	numDBs, err := strconv.Atoi(cfg[1])
	if err != nil {
		return 0, nil, fmt.Errorf("error parsing valkey database count: %w", err)
	}

	if c != nil && numDBs > max(c.db, c.first) {
		c = &coordination{c.db, c.first, min(c.last, numDBs-1)} // as much of the pool as exists
		db, err := claimFromPool(conn, owner, c, deadline)
		return db, c, err
	}
	db, err := claimFromTop(conn, owner, numDBs, deadline)
	return db, nil, err
}

// claimFromPool claims a database from a coordinated pool
func claimFromPool(conn valkey.Conn, owner string, c *coordination, deadline time.Time) (int, error) {
	ctx := context.Background()

	if _, err := valkey.DoContext(conn, ctx, "SELECT", c.db); err != nil {
		return 0, fmt.Errorf("error selecting coordination database: %w", err)
	}

	for {
		db, err := valkey.Int(claimScript.DoContext(ctx, conn, claimsKey, ownersKey, c.first, c.last, owner, claimTTL.Milliseconds()))
		if err != nil {
			return 0, fmt.Errorf("error claiming database: %w", err)
		}
		if db >= 0 {
			if _, err := valkey.DoContext(conn, ctx, "SELECT", db); err != nil {
				return 0, fmt.Errorf("error selecting database %d: %w", db, err)
			}
			if _, err := valkey.DoContext(conn, ctx, "FLUSHDB"); err != nil {
				return 0, fmt.Errorf("error flushing database %d: %w", db, err)
			}
			return db, nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("timed out waiting for an unclaimed test database (tried %d-%d)", c.first, c.last)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// claimFromTop claims a database by trying them from the top of the keyspace down
func claimFromTop(conn valkey.Conn, owner string, numDBs int, deadline time.Time) (int, error) {
	ctx := context.Background()

	for {
		for db := numDBs - 1; db >= numDBs-claimBandSize && db >= 0; db-- {
			if _, err := valkey.DoContext(conn, ctx, "SELECT", db); err != nil {
				return 0, fmt.Errorf("error selecting database %d: %w", db, err)
			}
			set, err := valkey.String(valkey.DoContext(conn, ctx, "SET", claimKey, owner, "NX", "EX", int(claimTTL.Seconds())))
			if err != nil && err != valkey.ErrNil {
				return 0, fmt.Errorf("error claiming database %d: %w", db, err)
			}
			if set == "OK" {
				return db, clear(conn)
			}
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("timed out waiting for an unclaimed test database (tried %d-%d)", max(numDBs-claimBandSize, 0), numDBs-1)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// keepClaim renews this binary's claim until the binary exits
func keepClaim(db int, owner string, c *coordination) {
	for {
		time.Sleep(claimTTL / 3)

		held, err := renew(getHostAddress(), db, owner, c)
		if err == nil && !held {
			// another binary may now be using our database, so nothing this binary asserts can be trusted
			panic(fmt.Sprintf("lost claim on test database %d", db))
		}
	}
}

// renew renews a claim, returning whether the owner still held it
func renew(addr string, db int, owner string, c *coordination) (bool, error) {
	ctx := context.Background()

	conn, err := valkey.Dial("tcp", addr)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	if c == nil {
		valkey.DoContext(conn, ctx, "SELECT", db)
		_, err := valkey.DoContext(conn, ctx, "EXPIRE", claimKey, int(claimTTL.Seconds()))
		return true, err
	}

	if _, err := valkey.DoContext(conn, ctx, "SELECT", c.db); err != nil {
		return false, err
	}
	held, err := valkey.Int(renewScript.DoContext(ctx, conn, claimsKey, ownersKey, db, owner, claimTTL.Milliseconds()))
	return held == 1, err
}

// clear deletes everything in the connection's currently selected database except the claim on it
func clear(conn valkey.Conn) error {
	ctx := context.Background()

	keys, err := valkey.Strings(valkey.DoContext(conn, ctx, "KEYS", "*"))
	if err != nil {
		return fmt.Errorf("error listing keys: %w", err)
	}
	keys = slices.DeleteFunc(keys, func(k string) bool { return k == claimKey })
	if len(keys) > 0 {
		args := make([]any, len(keys))
		for i, k := range keys {
			args[i] = k
		}
		if _, err := valkey.DoContext(conn, ctx, "DEL", args...); err != nil {
			return fmt.Errorf("error deleting keys: %w", err)
		}
	}
	return nil
}

// TestDB returns a valkey pool to this test binary's claimed database
func TestDB() *valkey.Pool {
	return &valkey.Pool{
		Dial: func() (valkey.Conn, error) {
			db, err := claimedDB()
			if err != nil {
				return nil, err
			}
			conn, err := valkey.Dial("tcp", getHostAddress())
			if err != nil {
				return nil, err
			}
			_, err = valkey.DoContext(conn, context.Background(), "SELECT", db)
			return conn, err
		},
	}
}

// TestDSN returns the DSN of this test binary's claimed database, for code that takes a valkey URL
func TestDSN() string {
	db, err := claimedDB()
	if err != nil {
		panic(fmt.Sprintf("error claiming test database: %s", err.Error()))
	}
	return fmt.Sprintf("valkey://%s/%d", getHostAddress(), db)
}

// FlushDB flushes the test database (preserving this binary's claim on it)
func FlushDB() {
	db, err := claimedDB()
	if err != nil {
		panic(fmt.Sprintf("error claiming test database: %s", err.Error()))
	}

	conn, err := valkey.Dial("tcp", getHostAddress())
	if err != nil {
		panic(fmt.Sprintf("error connecting to valkey db: %s", err.Error()))
	}
	defer conn.Close()

	if _, err := valkey.DoContext(conn, context.Background(), "SELECT", db); err != nil {
		panic(fmt.Sprintf("error selecting valkey db: %s", err.Error()))
	}
	if err := clear(conn); err != nil {
		panic(fmt.Sprintf("error flushing valkey db: %s", err.Error()))
	}
}

func getHostAddress() string {
	host := os.Getenv("VALKEY_HOST")
	if host == "" {
		host = "valkey"
	}
	return host + ":6379"
}
