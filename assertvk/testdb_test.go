package assertvk

import (
	"context"
	"testing"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaim(t *testing.T) {
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)

	sel := func(db int) valkey.Conn {
		conn, err := valkey.Dial("tcp", getHostAddress())
		require.NoError(t, err)
		_, err = valkey.DoContext(conn, ctx, "SELECT", db)
		require.NoError(t, err)
		return conn
	}

	db1, err := claim(getHostAddress(), "owner1", deadline)
	require.NoError(t, err)

	// a second claimant is given a different database
	db2, err := claim(getHostAddress(), "owner2", deadline)
	require.NoError(t, err)
	assert.NotEqual(t, db1, db2)

	// leave junk behind in db2 and drop its claim, as if the run that owned it had died
	conn := sel(db2)
	_, err = valkey.DoContext(conn, ctx, "SET", "junk", "1")
	require.NoError(t, err)
	_, err = valkey.DoContext(conn, ctx, "DEL", claimKey)
	require.NoError(t, err)
	conn.Close()

	// the next claimant gets it back, cleared
	db3, err := claim(getHostAddress(), "owner3", deadline)
	require.NoError(t, err)
	assert.Equal(t, db2, db3)

	conn = sel(db3)
	exists, err := valkey.Bool(valkey.DoContext(conn, ctx, "EXISTS", "junk"))
	require.NoError(t, err)
	assert.False(t, exists)
	owner, err := valkey.String(valkey.DoContext(conn, ctx, "GET", claimKey))
	require.NoError(t, err)
	assert.Equal(t, "owner3", owner)
	conn.Close()

	// release this test's claims
	for _, db := range []int{db1, db3} {
		conn = sel(db)
		_, err = valkey.DoContext(conn, ctx, "DEL", claimKey)
		require.NoError(t, err)
		conn.Close()
	}
}
