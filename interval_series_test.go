package vkutil_test

import (
	"context"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dates"
	vkutil "github.com/nyaruka/vkutil"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
)

func TestIntervalSeries(t *testing.T) {
	ctx := context.Background()
	rp := assertvk.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertvk.FlushDB()

	defer dates.SetNowFunc(time.Now)
	setNow := func(d time.Time) { dates.SetNowFunc(dates.NewFixedNow(d)) }

	assertGet := func(s *vkutil.IntervalSeries, f string, expected []int64) {
		actual, err := s.Get(ctx, rc, f)
		assert.NoError(t, err, "unexpected error getting field %s", f)
		assert.Equal(t, expected, actual, "expected series field %s to contain %v", f, expected)
	}
	assertTotal := func(s *vkutil.IntervalSeries, f string, expected int64) {
		actual, err := s.Total(ctx, rc, f)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}

	setNow(time.Date(2021, 11, 18, 12, 7, 3, 234567, time.UTC))

	// create a 5 minute x 5 based series
	series1 := vkutil.NewIntervalSeries("foos", time.Minute*5, 5, false)
	series1.Record(ctx, rc, "A", 2)

	setNow(time.Date(2021, 11, 18, 12, 9, 3, 234567, time.UTC)) // move time forward but within same interval

	series1.Record(ctx, rc, "A", 7)
	series1.Record(ctx, rc, "B", 4)

	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:05", map[string]string{"A": "9", "B": "4"})

	assertGet(series1, "A", []int64{9, 0, 0, 0, 0})
	assertGet(series1, "B", []int64{4, 0, 0, 0, 0})
	assertGet(series1, "C", []int64{0, 0, 0, 0, 0})
	assertTotal(series1, "A", 9)
	assertTotal(series1, "B", 4)
	assertTotal(series1, "C", 0)

	setNow(time.Date(2021, 11, 18, 12, 11, 3, 234567, time.UTC)) // move time forward to next interval

	series1.Record(ctx, rc, "A", 3)
	series1.Record(ctx, rc, "B", 2)

	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:10", map[string]string{"A": "3", "B": "2"})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:05", map[string]string{"A": "9", "B": "4"})

	assertGet(series1, "A", []int64{3, 9, 0, 0, 0})
	assertGet(series1, "B", []int64{2, 4, 0, 0, 0})
	assertGet(series1, "C", []int64{0, 0, 0, 0, 0})
	assertTotal(series1, "A", 12)
	assertTotal(series1, "B", 6)
	assertTotal(series1, "C", 0)

	setNow(time.Date(2021, 11, 18, 12, 26, 3, 234567, time.UTC)) // move time forward 3 intervals

	series1.Record(ctx, rc, "A", 10)
	series1.Record(ctx, rc, "B", 1)

	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:25", map[string]string{"A": "10", "B": "1"})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:20", map[string]string{})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:15", map[string]string{})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:10", map[string]string{"A": "3", "B": "2"})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:05", map[string]string{"A": "9", "B": "4"})

	assertGet(series1, "A", []int64{10, 0, 0, 3, 9})
	assertGet(series1, "B", []int64{1, 0, 0, 2, 4})
	assertGet(series1, "C", []int64{0, 0, 0, 0, 0})
	assertTotal(series1, "A", 22)
	assertTotal(series1, "B", 7)
	assertTotal(series1, "C", 0)

	setNow(time.Date(2021, 11, 18, 12, 30, 3, 234567, time.UTC)) // move time forward to next interval

	series1.Record(ctx, rc, "A", 1)

	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:30", map[string]string{"A": "1"})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:25", map[string]string{"A": "10", "B": "1"})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:20", map[string]string{})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:15", map[string]string{})
	assertvk.HGetAll(t, rc, "foos:2021-11-18T12:10", map[string]string{"A": "3", "B": "2"})

	assertGet(series1, "A", []int64{1, 10, 0, 0, 3})
	assertGet(series1, "B", []int64{0, 1, 0, 0, 2})
	assertGet(series1, "C", []int64{0, 0, 0, 0, 0})
	assertTotal(series1, "A", 14)
	assertTotal(series1, "B", 3)
	assertTotal(series1, "C", 0)
}
