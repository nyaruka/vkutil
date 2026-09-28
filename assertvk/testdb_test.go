package assertvk

import (
	"context"
	"strconv"
	"testing"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoordinate(t *testing.T) {
	assert.PanicsWithValue(t, "invalid test database coordination: db -1, pool 17-63", func() { Coordinate(-1, 17, 63) })
	assert.PanicsWithValue(t, "invalid test database coordination: db 16, pool 63-17", func() { Coordinate(16, 63, 17) })
	assert.PanicsWithValue(t, "invalid test database coordination: db 20, pool 17-63", func() { Coordinate(20, 17, 63) })
}

func TestClaimFromPool(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	numDBs := numDatabases(t)

	if numDBs <= 17 {
		t.Skip("valkey instance is too small to coordinate claims")
	}
	coord := &coordination{16, 17, min(63, numDBs-1)}

	// claiming leaves the connection on the claimed database, so each claim gets its own
	claimFromPool := func(owner string) (int, error) {
		c := sel(t, coord.db)
		defer c.Close()
		return claimFromPool(c, owner, coord, deadline)
	}

	conn := sel(t, coord.db)
	defer conn.Close()

	db1, err := claimFromPool("owner1")
	require.NoError(t, err)

	// a second claimant is given a different database
	db2, err := claimFromPool("owner2")
	require.NoError(t, err)
	assert.NotEqual(t, db1, db2)

	// owners can renew their own claims but not each other's
	held, err := renew(getHostAddress(), db1, "owner1", coord)
	assert.NoError(t, err)
	assert.True(t, held)
	held, err = renew(getHostAddress(), db1, "owner2", coord)
	assert.NoError(t, err)
	assert.False(t, held)

	// leave junk behind in db2 and expire its claim, as if the run that owned it had died
	c2 := sel(t, db2)
	_, err = valkey.DoContext(c2, ctx, "SET", "junk", "1")
	require.NoError(t, err)
	c2.Close()
	_, err = valkey.DoContext(conn, ctx, "ZADD", claimsKey, 0, db2)
	require.NoError(t, err)

	// the next claimant gets it back, flushed
	db3, err := claimFromPool("owner3")
	require.NoError(t, err)
	assert.Equal(t, db2, db3)

	c3 := sel(t, db3)
	exists, err := valkey.Bool(valkey.DoContext(c3, ctx, "EXISTS", "junk"))
	require.NoError(t, err)
	assert.False(t, exists)
	c3.Close()

	// and the previous owner can no longer renew it
	held, err = renew(getHostAddress(), db2, "owner2", coord)
	assert.NoError(t, err)
	assert.False(t, held)

	owner, err := valkey.String(valkey.DoContext(conn, ctx, "HGET", ownersKey, db3))
	require.NoError(t, err)
	assert.Equal(t, "owner3", owner)

	// flushing a claimed database doesn't lose its claim
	c3 = sel(t, db3)
	_, err = valkey.DoContext(c3, ctx, "FLUSHDB")
	require.NoError(t, err)
	c3.Close()
	held, err = renew(getHostAddress(), db3, "owner3", coord)
	assert.NoError(t, err)
	assert.True(t, held)

	// owners can release their own claims but not each other's
	released, err := release(getHostAddress(), db3, "owner1", coord)
	assert.NoError(t, err)
	assert.False(t, released)
	released, err = release(getHostAddress(), db3, "owner3", coord)
	assert.NoError(t, err)
	assert.True(t, released)

	isClaimed, err := valkey.Bool(valkey.DoContext(conn, ctx, "HEXISTS", ownersKey, db3))
	require.NoError(t, err)
	assert.False(t, isClaimed)
	_, err = valkey.Float64(valkey.DoContext(conn, ctx, "ZSCORE", claimsKey, db3))
	assert.Equal(t, valkey.ErrNil, err)

	// a released claim can't be renewed or released again, and its database is claimable straight away
	held, err = renew(getHostAddress(), db3, "owner3", coord)
	assert.NoError(t, err)
	assert.False(t, held)
	released, err = release(getHostAddress(), db3, "owner3", coord)
	assert.NoError(t, err)
	assert.False(t, released)

	db4, err := claimFromPool("owner4")
	require.NoError(t, err)
	assert.Equal(t, db3, db4)

	// release this test's claims
	for db, owner := range map[int]string{db1: "owner1", db4: "owner4"} {
		released, err := release(getHostAddress(), db, owner, coord)
		require.NoError(t, err)
		assert.True(t, released)
	}
}

func TestClaimFromTop(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	numDBs := numDatabases(t)

	conn := sel(t, 0)
	defer conn.Close()

	db1, err := claimFromTop(conn, "owner1", numDBs, deadline)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, db1, numDBs-claimBandSize)

	// a second claimant is given a different database
	db2, err := claimFromTop(conn, "owner2", numDBs, deadline)
	require.NoError(t, err)
	assert.NotEqual(t, db1, db2)

	// leave junk behind in db2 and drop its claim, as if the run that owned it had died
	c2 := sel(t, db2)
	_, err = valkey.DoContext(c2, ctx, "SET", "junk", "1")
	require.NoError(t, err)
	_, err = valkey.DoContext(c2, ctx, "DEL", claimKey)
	require.NoError(t, err)
	c2.Close()

	// the next claimant gets it back, cleared
	db3, err := claimFromTop(conn, "owner3", numDBs, deadline)
	require.NoError(t, err)
	assert.Equal(t, db2, db3)

	c3 := sel(t, db3)
	exists, err := valkey.Bool(valkey.DoContext(c3, ctx, "EXISTS", "junk"))
	require.NoError(t, err)
	assert.False(t, exists)
	owner, err := valkey.String(valkey.DoContext(c3, ctx, "GET", claimKey))
	require.NoError(t, err)
	assert.Equal(t, "owner3", owner)

	// owners can release their own claims but not each other's
	released, err := release(getHostAddress(), db3, "owner1", nil)
	assert.NoError(t, err)
	assert.False(t, released)
	released, err = release(getHostAddress(), db3, "owner3", nil)
	assert.NoError(t, err)
	assert.True(t, released)

	isClaimed, err := valkey.Bool(valkey.DoContext(c3, ctx, "EXISTS", claimKey))
	require.NoError(t, err)
	assert.False(t, isClaimed)
	c3.Close()

	// a released claim can't be released again, and its database is claimable straight away
	released, err = release(getHostAddress(), db3, "owner3", nil)
	assert.NoError(t, err)
	assert.False(t, released)

	db4, err := claimFromTop(conn, "owner4", numDBs, deadline)
	require.NoError(t, err)
	assert.Equal(t, db3, db4)

	// release this test's claims
	for db, owner := range map[int]string{db1: "owner1", db4: "owner4"} {
		released, err := release(getHostAddress(), db, owner, nil)
		require.NoError(t, err)
		assert.True(t, released)
	}
}

func TestHeldClaimRelease(t *testing.T) {
	ctx := context.Background()
	numDBs := numDatabases(t)

	conn := sel(t, 0)
	defer conn.Close()

	db, err := claimFromTop(conn, "owner1", numDBs, time.Now().Add(5*time.Second))
	require.NoError(t, err)

	h := &heldClaim{db: db, owner: "owner1", stop: make(chan struct{}), stopped: make(chan struct{})}
	go h.keep()

	c := sel(t, db)
	defer c.Close()
	_, err = valkey.DoContext(c, ctx, "SET", "data", "1")
	require.NoError(t, err)

	// releasing stops renewals and drops the claim, but leaves the data for inspection
	h.release()

	select {
	case <-h.stopped:
	default:
		assert.Fail(t, "claim still being renewed")
	}
	assertExists := func(key string, expected bool) {
		exists, err := valkey.Bool(valkey.DoContext(c, ctx, "EXISTS", key))
		require.NoError(t, err)
		assert.Equal(t, expected, exists, "exists %s", key)
	}
	assertExists(claimKey, false)
	assertExists("data", true)

	_, err = valkey.DoContext(c, ctx, "DEL", "data")
	require.NoError(t, err)
}

// sel returns a new connection to the given database
func sel(t *testing.T, db int) valkey.Conn {
	conn, err := valkey.Dial("tcp", getHostAddress())
	require.NoError(t, err)
	_, err = valkey.DoContext(conn, context.Background(), "SELECT", db)
	require.NoError(t, err)
	return conn
}

func numDatabases(t *testing.T) int {
	conn := sel(t, 0)
	defer conn.Close()

	cfg, err := valkey.Strings(valkey.DoContext(conn, context.Background(), "CONFIG", "GET", "databases"))
	require.NoError(t, err)
	n, err := strconv.Atoi(cfg[1])
	require.NoError(t, err)
	return n
}

func TestClaimFallsBackWithoutCoordinationDB(t *testing.T) {
	ctx := context.Background()
	numDBs := numDatabases(t)

	// coordination through a database the instance doesn't have falls back to uncoordinated claims
	db, used, err := claim(getHostAddress(), "owner1", &coordination{numDBs, numDBs + 1, numDBs + 10}, time.Now().Add(5*time.Second))
	require.NoError(t, err)
	assert.Nil(t, used)

	c := sel(t, db)
	defer c.Close()
	owner, err := valkey.String(valkey.DoContext(c, ctx, "GET", claimKey))
	require.NoError(t, err)
	assert.Equal(t, "owner1", owner)

	_, err = valkey.DoContext(c, ctx, "DEL", claimKey)
	require.NoError(t, err)
}
