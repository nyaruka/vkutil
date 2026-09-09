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
// worktrees, other repos - sharing a valkey instance can't interfere with each other. Databases are tried
// from the top of the instance's keyspace downward: 15..0 on a default 16 database instance (e.g. a
// throwaway CI service), 127..112 on a shared instance configured with 128 databases and that band reserved
// for test claims. A claim is a key with a TTL kept alive for the binary's lifetime, so it evaporates
// shortly after the run that owns it ends, and whatever a dead run left behind is cleared on the next claim.

const (
	claimKey      = "__assertvk_claim__"
	claimTTL      = 30 * time.Second
	claimBandSize = 16
)

// this binary's claimed database - claimed on first use
var claimedDB = sync.OnceValues(func() (int, error) {
	hostname, _ := os.Hostname()

	db, err := claim(getHostAddress(), fmt.Sprintf("%s:%d", hostname, os.Getpid()), time.Now().Add(3*time.Minute))
	if err == nil {
		go keepClaim(db)
	}
	return db, err
})

// claim finds and claims an unclaimed database, clearing anything a previous owner left behind. If every
// database in the band is claimed it keeps trying until the deadline - claims from dead runs expire.
func claim(addr, owner string, deadline time.Time) (int, error) {
	ctx := context.Background()

	conn, err := valkey.Dial("tcp", addr)
	if err != nil {
		return 0, fmt.Errorf("error connecting to valkey: %w", err)
	}
	defer conn.Close()

	cfg, err := valkey.Strings(valkey.DoContext(conn, ctx, "CONFIG", "GET", "databases"))
	if err != nil || len(cfg) != 2 {
		return 0, fmt.Errorf("error reading valkey database count: %w", err)
	}
	numDBs, err := strconv.Atoi(cfg[1])
	if err != nil {
		return 0, fmt.Errorf("error parsing valkey database count: %w", err)
	}

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

// keepClaim refreshes this binary's claim until the binary exits
func keepClaim(db int) {
	ctx := context.Background()

	for {
		time.Sleep(claimTTL / 3)

		conn, err := valkey.Dial("tcp", getHostAddress())
		if err != nil {
			continue
		}
		valkey.DoContext(conn, ctx, "SELECT", db)
		valkey.DoContext(conn, ctx, "EXPIRE", claimKey, int(claimTTL.Seconds()))
		conn.Close()
	}
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
