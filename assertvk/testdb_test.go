package assertvk

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	Coordinate(16, 17, 63) // CI's valkeys have 64 databases

	os.Exit(m.Run())
}

func TestCoordinate(t *testing.T) {
	assert.PanicsWithValue(t, "invalid test database coordination: db -1, pool 17-63", func() { Coordinate(-1, 17, 63) })
	assert.PanicsWithValue(t, "invalid test database coordination: db 16, pool 63-17", func() { Coordinate(16, 63, 17) })
	assert.PanicsWithValue(t, "invalid test database coordination: db 20, pool 17-63", func() { Coordinate(20, 17, 63) })
}

func TestClaimFromPool(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)

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

func TestClaimDB(t *testing.T) {
	ctx := context.Background()

	var db1, db2 int

	t.Run("claiming", func(t *testing.T) {
		d1, d2 := ClaimDB(t), ClaimDB(t)
		db1, db2 = d1.Num, d2.Num

		// each claim is given its own database
		assert.NotEqual(t, db1, db2)
		assert.Equal(t, fmt.Sprintf("valkey://%s/%d", getHostAddress(), db1), d1.URL)

		// whose pool connects to it
		vc := d1.Pool().Get()
		defer vc.Close()
		info, err := valkey.String(valkey.DoContext(vc, ctx, "CLIENT", "INFO"))
		require.NoError(t, err)
		assert.Contains(t, info, fmt.Sprintf(" db=%d ", db1))

		_, err = valkey.DoContext(vc, ctx, "SET", "data", "1")
		require.NoError(t, err)
	})

	// once the test completes its databases are flushed and released
	for _, db := range []int{db1, db2} {
		c := sel(t, db)
		n, err := valkey.Int(valkey.DoContext(c, ctx, "DBSIZE"))
		require.NoError(t, err)
		assert.Equal(t, 0, n, "keys in db %d", db)
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

func TestClaimRequiresCoordination(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	numDBs := numDatabases(t)

	_, err := claim(getHostAddress(), "owner1", nil, deadline)
	assert.EqualError(t, err, "test database claims aren't coordinated, call assertvk.Coordinate first")

	// the instance must have the coordination database and the whole pool
	_, err = claim(getHostAddress(), "owner1", &coordination{numDBs, 1, 2}, deadline)
	assert.EqualError(t, err, fmt.Sprintf("valkey at %s has %d databases, too few for coordination database %d and pool 1-2", getHostAddress(), numDBs, numDBs))
	_, err = claim(getHostAddress(), "owner1", &coordination{0, 1, numDBs}, deadline)
	assert.EqualError(t, err, fmt.Sprintf("valkey at %s has %d databases, too few for coordination database 0 and pool 1-%d", getHostAddress(), numDBs, numDBs))
}
