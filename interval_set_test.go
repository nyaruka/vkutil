package vkutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/vkutil"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntervalSet(t *testing.T) {
	ctx := context.Background()
	vp := assertvk.TestDB()
	vc := vp.Get()
	defer vc.Close()

	defer assertvk.FlushDB()

	defer dates.SetNowFunc(time.Now)
	setNow := func(d time.Time) { dates.SetNowFunc(dates.NewFixedNow(d)) }

	setNow(time.Date(2021, 11, 18, 12, 0, 3, 234567, time.UTC))

	// create a 24-hour x 2 based set
	set1 := vkutil.NewIntervalSet("foos", time.Hour*24, 2)
	assert.NoError(t, set1.Add(ctx, vc, "A"))
	assert.NoError(t, set1.Add(ctx, vc, "B"))
	assert.NoError(t, set1.Add(ctx, vc, "C"))

	assertvk.SMembers(t, vc, "{foos}:2021-11-18", []string{"A", "B", "C"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-17", []string{})

	assertIsMember := func(s *vkutil.IntervalSet, v string) {
		contains, err := s.IsMember(ctx, vc, v)
		assert.NoError(t, err)
		assert.True(t, contains, "expected marker to contain %s", v)
	}
	assertNotIsMember := func(s *vkutil.IntervalSet, v string) {
		contains, err := s.IsMember(ctx, vc, v)
		assert.NoError(t, err)
		assert.False(t, contains, "expected marker to not contain %s", v)
	}

	assertIsMember(set1, "A")
	assertIsMember(set1, "B")
	assertIsMember(set1, "C")
	assertNotIsMember(set1, "D")

	// move forward a day..
	setNow(time.Date(2021, 11, 19, 12, 0, 3, 234567, time.UTC))

	set1.Add(ctx, vc, "D")
	set1.Add(ctx, vc, "E")

	assertvk.SMembers(t, vc, "{foos}:2021-11-19", []string{"D", "E"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-18", []string{"A", "B", "C"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-17", []string{})

	assertIsMember(set1, "A")
	assertIsMember(set1, "B")
	assertIsMember(set1, "C")
	assertIsMember(set1, "D")
	assertIsMember(set1, "E")
	assertNotIsMember(set1, "F")

	// move forward again..
	setNow(time.Date(2021, 11, 20, 12, 7, 3, 234567, time.UTC))

	set1.Add(ctx, vc, "F")
	set1.Add(ctx, vc, "G")

	assertvk.SMembers(t, vc, "{foos}:2021-11-20", []string{"F", "G"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-19", []string{"D", "E"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-18", []string{"A", "B", "C"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-17", []string{})

	assertNotIsMember(set1, "A") // too old
	assertNotIsMember(set1, "B") // too old
	assertNotIsMember(set1, "C") // too old
	assertIsMember(set1, "D")
	assertIsMember(set1, "E")
	assertIsMember(set1, "F")
	assertIsMember(set1, "G")

	err := set1.Rem(ctx, vc, "F") // from today
	require.NoError(t, err)
	err = set1.Rem(ctx, vc, "E") // from yesterday
	require.NoError(t, err)

	assertvk.SMembers(t, vc, "{foos}:2021-11-20", []string{"G"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-19", []string{"D"})

	assertIsMember(set1, "D")
	assertNotIsMember(set1, "E")
	assertNotIsMember(set1, "F")
	assertIsMember(set1, "G")

	err = set1.Clear(ctx, vc)
	require.NoError(t, err)

	assertvk.SMembers(t, vc, "{foos}:2021-11-20", []string{})
	assertvk.SMembers(t, vc, "{foos}:2021-11-19", []string{})

	assertNotIsMember(set1, "D")
	assertNotIsMember(set1, "E")
	assertNotIsMember(set1, "F")
	assertNotIsMember(set1, "G")

	// create a 5 minute x 3 based set
	set2 := vkutil.NewIntervalSet("foos", time.Minute*5, 3)
	set2.Add(ctx, vc, "A")
	set2.Add(ctx, vc, "B")

	assertvk.SMembers(t, vc, "{foos}:2021-11-20T12:05", []string{"A", "B"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-20T12:00", []string{})

	assertIsMember(set2, "A")
	assertIsMember(set2, "B")
	assertNotIsMember(set2, "C")

	// create a 5 second x 2 based set
	set3 := vkutil.NewIntervalSet("foos", time.Second*5, 2)
	set3.Add(ctx, vc, "A")
	set3.Add(ctx, vc, "B")

	assertvk.SMembers(t, vc, "{foos}:2021-11-20T12:07:00", []string{"A", "B"})
	assertvk.SMembers(t, vc, "{foos}:2021-11-20T12:06:55", []string{})

	assertIsMember(set3, "A")
	assertIsMember(set3, "B")
	assertNotIsMember(set3, "C")
}
