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

func TestClaimFromPool(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	numDBs := numDatabases(t)

	if numDBs <= poolFirst {
		t.Skip("valkey instance has no coordination database")
	}

	// claiming leaves the connection on the claimed database, so each claim gets its own
	claimFromPool := func(owner string) (int, error) {
		c := sel(t, coordDB)
		defer c.Close()
		return claimFromPool(c, owner, min(poolLast, numDBs-1), deadline)
	}

	conn := sel(t, coordDB)
	defer conn.Close()

	db1, err := claimFromPool("owner1")
	require.NoError(t, err)

	// a second claimant is given a different database
	db2, err := claimFromPool("owner2")
	require.NoError(t, err)
	assert.NotEqual(t, db1, db2)

	// owners can renew their own claims but not each other's
	held, err := renew(getHostAddress(), db1, "owner1", true)
	assert.NoError(t, err)
	assert.True(t, held)
	held, err = renew(getHostAddress(), db1, "owner2", true)
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
	held, err = renew(getHostAddress(), db2, "owner2", true)
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
	held, err = renew(getHostAddress(), db3, "owner3", true)
	assert.NoError(t, err)
	assert.True(t, held)

	// release this test's claims
	for _, db := range []int{db1, db3} {
		_, err = valkey.DoContext(conn, ctx, "ZREM", claimsKey, db)
		require.NoError(t, err)
		_, err = valkey.DoContext(conn, ctx, "HDEL", ownersKey, db)
		require.NoError(t, err)
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
	c3.Close()

	// release this test's claims
	for _, db := range []int{db1, db3} {
		c := sel(t, db)
		_, err = valkey.DoContext(c, ctx, "DEL", claimKey)
		require.NoError(t, err)
		c.Close()
	}
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
