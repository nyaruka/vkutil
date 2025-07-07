package vkutil_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	vkutil "github.com/nyaruka/vkutil"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalHash(t *testing.T) {
	ctx := context.Background()
	vp := assertvk.TestDB()
	vc := vp.Get()
	defer vc.Close()

	defer assertvk.FlushDB()

	defer dates.SetNowFunc(time.Now)
	setNow := func(d time.Time) { dates.SetNowFunc(dates.NewFixedNow(d)) }

	setNow(time.Date(2021, 11, 18, 12, 7, 3, 234567, time.UTC))

	assertGet := func(h *vkutil.IntervalHash, k, expected string) {
		actual, err := h.Get(ctx, vc, k)
		assert.NoError(t, err, "unexpected error getting key %s", k)
		assert.Equal(t, expected, actual, "expected hash key %s to contain %s", k, expected)
	}
	assertMGet := func(h *vkutil.IntervalHash, ks []string, expected []string) {
		actual, err := h.MGet(ctx, vc, ks...)
		assert.NoError(t, err, "unexpected error getting keys %s", strings.Join(ks, ","))
		assert.Equal(t, expected, actual, "expected hash keys %s to contain %s", strings.Join(ks, ","), strings.Join(expected, ","))
	}

	// create a 24-hour x 2 based hash
	hash1 := vkutil.NewIntervalHash("foos", time.Hour*24, 2, false)
	assert.NoError(t, hash1.Set(ctx, vc, "A", "1"))
	assert.NoError(t, hash1.Set(ctx, vc, "B", "2"))
	assert.NoError(t, hash1.Set(ctx, vc, "C", "3"))

	assertvk.HGetAll(t, vc, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertvk.HGetAll(t, vc, "foos:2021-11-17", map[string]string{})

	assertGet(hash1, "A", "1")
	assertGet(hash1, "B", "2")
	assertGet(hash1, "C", "3")
	assertGet(hash1, "D", "")
	assertMGet(hash1, []string{"A", "C", "D"}, []string{"1", "3", ""})
	assertMGet(hash1, []string{"D", "A"}, []string{"", "1"})

	_, err := hash1.MGet(ctx, vc) // zero fields is an error
	assert.EqualError(t, err, "wrong number of arguments for command")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 7, 3, 234567, time.UTC))

	hash1.Set(ctx, vc, "A", "5")
	hash1.Set(ctx, vc, "B", "6")

	assertvk.HGetAll(t, vc, "foos:2021-11-19", map[string]string{"A": "5", "B": "6"})
	assertvk.HGetAll(t, vc, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertvk.HGetAll(t, vc, "foos:2021-11-17", map[string]string{})

	assertGet(hash1, "A", "5")
	assertGet(hash1, "B", "6")
	assertGet(hash1, "C", "3")
	assertGet(hash1, "D", "")
	assertMGet(hash1, []string{"A", "C", "D"}, []string{"5", "3", ""})
	assertMGet(hash1, []string{"B"}, []string{"6"})

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 7, 3, 234567, time.UTC))

	hash1.Set(ctx, vc, "A", "7")
	hash1.Set(ctx, vc, "Z", "9")

	assertvk.HGetAll(t, vc, "foos:2021-11-20", map[string]string{"A": "7", "Z": "9"})
	assertvk.HGetAll(t, vc, "foos:2021-11-19", map[string]string{"A": "5", "B": "6"})
	assertvk.HGetAll(t, vc, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertvk.HGetAll(t, vc, "foos:2021-11-17", map[string]string{})

	assertGet(hash1, "A", "7")
	assertGet(hash1, "Z", "9")
	assertGet(hash1, "B", "6")
	assertGet(hash1, "C", "") // too old
	assertGet(hash1, "D", "")
	assertMGet(hash1, []string{"B", "A", "D"}, []string{"6", "7", ""})

	err = hash1.Del(ctx, vc, "A") // from today and yesterday
	require.NoError(t, err)
	err = hash1.Del(ctx, vc, "B") // from yesterday
	require.NoError(t, err)

	assertvk.HGetAll(t, vc, "foos:2021-11-20", map[string]string{"Z": "9"})
	assertvk.HGetAll(t, vc, "foos:2021-11-19", map[string]string{})
	assertvk.HGetAll(t, vc, "foos:2021-11-18", map[string]string{"A": "1", "B": "2", "C": "3"})
	assertvk.HGetAll(t, vc, "foos:2021-11-17", map[string]string{})

	assertGet(hash1, "A", "")
	assertGet(hash1, "Z", "9")
	assertGet(hash1, "B", "")
	assertGet(hash1, "C", "")
	assertGet(hash1, "D", "")

	err = hash1.Clear(ctx, vc)
	require.NoError(t, err)

	assertvk.HGetAll(t, vc, "foos:2021-11-20", map[string]string{})
	assertvk.HGetAll(t, vc, "foos:2021-11-19", map[string]string{})

	assertGet(hash1, "A", "")
	assertGet(hash1, "Z", "")
	assertGet(hash1, "B", "")
	assertGet(hash1, "C", "")
	assertGet(hash1, "D", "")

	// create a 5 minute x 3 based hash
	hash2 := vkutil.NewIntervalHash("foos", time.Minute*5, 3, true)
	hash2.Set(ctx, vc, "A", "1")
	hash2.Set(ctx, vc, "B", "2")

	assertvk.HGetAll(t, vc, "{foos}:2021-11-20T12:05", map[string]string{"A": "1", "B": "2"})
	assertvk.HGetAll(t, vc, "{foos}:2021-11-20T12:00", map[string]string{})

	assertGet(hash2, "A", "1")
	assertGet(hash2, "B", "2")
	assertGet(hash2, "C", "")

	// create a 5 second x 2 based set
	hash3 := vkutil.NewIntervalHash("foos", time.Second*5, 2, true)
	hash3.Set(ctx, vc, "A", "1")
	hash3.Set(ctx, vc, "B", "2")

	assertvk.HGetAll(t, vc, "{foos}:2021-11-20T12:07:00", map[string]string{"A": "1", "B": "2"})
	assertvk.HGetAll(t, vc, "{foos}:2021-11-20T12:06:55", map[string]string{})

	assertGet(hash3, "A", "1")
	assertGet(hash3, "B", "2")
	assertGet(hash3, "C", "")
}
