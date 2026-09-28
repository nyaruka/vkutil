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

func TestClaimBand(t *testing.T) {
	tcs := []struct {
		band        string
		numDBs      int
		first, last int
		err         string
	}{
		{"", 16, 0, 15, ""},
		{"", 128, 112, 127, ""},
		{"", 8, 0, 7, ""},
		{"80-95", 128, 80, 95, ""},
		{"5-5", 16, 5, 5, ""},
		{"80-95", 16, 0, 0, `invalid VALKEY_TEST_DBS "80-95" for an instance with 16 databases`},
		{"95-80", 128, 0, 0, `invalid VALKEY_TEST_DBS "95-80" for an instance with 128 databases`},
		{"80", 128, 0, 0, `invalid VALKEY_TEST_DBS "80" for an instance with 128 databases`},
		{"x-y", 128, 0, 0, `invalid VALKEY_TEST_DBS "x-y" for an instance with 128 databases`},
	}
	for _, tc := range tcs {
		first, last, err := claimBand(tc.band, tc.numDBs)
		if tc.err != "" {
			assert.EqualError(t, err, tc.err, "error mismatch for %q/%d", tc.band, tc.numDBs)
		} else {
			assert.NoError(t, err, "unexpected error for %q/%d", tc.band, tc.numDBs)
			assert.Equal(t, tc.first, first, "first mismatch for %q/%d", tc.band, tc.numDBs)
			assert.Equal(t, tc.last, last, "last mismatch for %q/%d", tc.band, tc.numDBs)
		}
	}
}
