package vkutil_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nyaruka/vkutil"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memoryTallyStore is a tally store which keeps totals in memory and can be made to fail.
type memoryTallyStore struct {
	totals       map[string]int64
	lastBatch    int64
	batches      []int // sizes of the batches written
	lastBatchErr error
	writeErr     error
	onWrite      func()
}

func newMemoryTallyStore() *memoryTallyStore {
	return &memoryTallyStore{totals: map[string]int64{}}
}

func (s *memoryTallyStore) LastBatch(ctx context.Context) (int64, error) {
	return s.lastBatch, s.lastBatchErr
}

func (s *memoryTallyStore) Write(ctx context.Context, id int64, amounts map[string]int64) (bool, error) {
	if s.writeErr != nil {
		return false, s.writeErr
	}
	if s.onWrite != nil {
		s.onWrite()
	}
	if id <= s.lastBatch {
		return false, nil
	}
	for field, count := range amounts {
		s.totals[field] += count
	}
	s.lastBatch = id
	s.batches = append(s.batches, len(amounts))
	return true, nil
}

func TestTalliesIncrement(t *testing.T) {
	ctx := t.Context()
	vc := assertvk.ClaimDB(t).Pool().Get()
	defer vc.Close()

	tl := vkutil.NewTallies("tallies")
	assert.Equal(t, "tallies", tl.KeyBase())

	require.NoError(t, tl.Increment(ctx, vc, map[string]int64{}))
	assertvk.NotExists(t, vc, "{tallies}:live")

	require.NoError(t, tl.Increment(ctx, vc, map[string]int64{"foo": 2, "1|bar@2025-05-04": 1}))
	require.NoError(t, tl.Increment(ctx, vc, map[string]int64{"foo": 1}))
	require.NoError(t, tl.Increment(ctx, vc, map[string]int64{"foo": 5, "a|b": 1}))

	assertvk.HGetAll(t, vc, "{tallies}:live", map[string]string{"foo": "8", "1|bar@2025-05-04": "1", "a|b": "1"})

	// a failing command inside the transaction is reported even though the transaction itself succeeds
	vc.Do("HSET", "{tallies}:live", "foo", "bar")

	err := tl.Increment(ctx, vc, map[string]int64{"foo": 1})
	assert.EqualError(t, err, "ERR hash value is not an integer")
}

func TestTalliesFlush(t *testing.T) {
	ctx := t.Context()
	vc := assertvk.ClaimDB(t).Pool().Get()
	defer vc.Close()

	tl := vkutil.NewTallies("tallies")
	store := newMemoryTallyStore()

	assert.PanicsWithValue(t, "flush batch size must be greater than zero", func() { tl.Flush(ctx, vc, store, 0) })

	increment := func(amounts map[string]int64) {
		require.NoError(t, tl.Increment(ctx, vc, amounts))
	}
	flush := func(batchSize, expected int) {
		flushed, err := tl.Flush(ctx, vc, store, batchSize)
		require.NoError(t, err)
		assert.Equal(t, expected, flushed)
	}
	assertCleared := func() {
		assertvk.NotExists(t, vc, "{tallies}:live")
		assertvk.NotExists(t, vc, "{tallies}:flushing")
		assertvk.NotExists(t, vc, "{tallies}:batch")
	}

	// nothing to flush
	flush(10, 0)
	assert.Equal(t, int64(0), store.lastBatch)

	increment(map[string]int64{"foo": 2, "bar": 1})
	increment(map[string]int64{"foo": 1})
	increment(map[string]int64{"baz": 5, "qux": 1})

	flush(10, 4)

	assert.Equal(t, map[string]int64{"foo": 3, "bar": 1, "baz": 5, "qux": 1}, store.totals)
	assert.Equal(t, int64(1), store.lastBatch)
	assert.Equal(t, []int{4}, store.batches)
	assertvk.Get(t, vc, "{tallies}:batch_id", "1")
	assertCleared()

	// a flush drains everything buffered when it started, a batch at a time
	increment(map[string]int64{"foo": 1, "a": 1, "b": 1, "c": 1, "d": 1})

	flush(2, 5)

	assert.Equal(t, int64(4), store.lastBatch)
	assert.Equal(t, []int{4, 2, 2, 1}, store.batches)
	assert.Equal(t, map[string]int64{"foo": 4, "bar": 1, "baz": 5, "qux": 1, "a": 1, "b": 1, "c": 1, "d": 1}, store.totals)
	assertCleared()

	// but not increments made during it, which wait for the next one
	store.onWrite = func() { increment(map[string]int64{"foo": 10}) }
	increment(map[string]int64{"foo": 1})

	flush(10, 1)

	assert.Equal(t, int64(5), store.totals["foo"])
	assertvk.HGetAll(t, vc, "{tallies}:live", map[string]string{"foo": "10"})

	store.onWrite = nil
	flush(10, 1)

	assert.Equal(t, int64(15), store.totals["foo"])
	assert.Equal(t, int64(6), store.lastBatch)
	assertCleared()

	// failing to get the last batch leaves everything as it was
	increment(map[string]int64{"foo": 1})
	store.lastBatchErr = errors.New("boom")

	_, err := tl.Flush(ctx, vc, store, 10)
	assert.EqualError(t, err, "error getting last batch: boom")
	assertvk.HGetAll(t, vc, "{tallies}:live", map[string]string{"foo": "1"})

	store.lastBatchErr = nil

	// a failed write leaves its batch, and the rest of the snapshot, for the next flush to write
	increment(map[string]int64{"bar": 1, "baz": 1})
	store.writeErr = errors.New("boom")

	flushed, err := tl.Flush(ctx, vc, store, 2)
	assert.EqualError(t, err, "error writing batch 7: boom")
	assert.Equal(t, 0, flushed)

	assertvk.HLen(t, vc, "{tallies}:batch", 2)
	assertvk.HLen(t, vc, "{tallies}:flushing", 1)
	assertvk.Get(t, vc, "{tallies}:batch_id", "7")

	// and does so before taking a new snapshot, which includes increments made in the meantime
	store.writeErr = nil
	increment(map[string]int64{"bar": 1})

	flush(2, 4)

	assert.Equal(t, int64(16), store.totals["foo"])
	assert.Equal(t, int64(3), store.totals["bar"])
	assert.Equal(t, int64(6), store.totals["baz"])
	assert.Equal(t, int64(9), store.lastBatch)
	assertCleared()

	// a batch which was written but not deleted is deleted without being written again
	vc.Do("HSET", "{tallies}:batch", "foo", 100)
	vc.Do("SET", "{tallies}:batch_id", 9)
	increment(map[string]int64{"foo": 1})

	flush(10, 1)

	assert.Equal(t, int64(17), store.totals["foo"])
	assert.Equal(t, int64(10), store.lastBatch)
	assertCleared()

	// a batch without an id can't be written safely, so stops flushing until it's dealt with
	vc.Do("HSET", "{tallies}:batch", "foo", 100)
	vc.Do("DEL", "{tallies}:batch_id")

	_, err = tl.Flush(ctx, vc, store, 10)
	assert.EqualError(t, err, "error reading id of leftover batch from {tallies}:batch_id: redigo: nil returned")

	vc.Do("DEL", "{tallies}:batch")
	vc.Do("SET", "{tallies}:batch_id", 10)

	// batch ids carry on from the last one used if the store is behind, e.g. because it was reset
	store.lastBatch = 3
	increment(map[string]int64{"foo": 1})

	flush(10, 1)

	assert.Equal(t, int64(11), store.lastBatch)
	assertvk.Get(t, vc, "{tallies}:batch_id", "11")

	// or from the store's if Valkey is behind, e.g. because it lost data
	vc.Do("DEL", "{tallies}:batch_id")
	increment(map[string]int64{"foo": 1})

	flush(10, 1)

	assert.Equal(t, int64(12), store.lastBatch)
	assertvk.Get(t, vc, "{tallies}:batch_id", "12")
	assert.Equal(t, int64(19), store.totals["foo"])
}

func TestTalliesFlushConcurrently(t *testing.T) {
	ctx := t.Context()
	db := assertvk.ClaimDB(t)
	vc, vc2 := db.Pool().Get(), db.Pool().Get()
	defer vc.Close()
	defer vc2.Close()

	tl := vkutil.NewTallies("tallies")
	store := newMemoryTallyStore()

	require.NoError(t, tl.Increment(ctx, vc, map[string]int64{"a": 1, "b": 1, "c": 1}))

	// another flush which runs while this one is writing its first batch writes that batch itself and then the rest,
	// so this one's write of it does nothing and it finds nothing left
	var otherFlushed int
	var otherErr error
	store.onWrite = func() {
		store.onWrite = nil
		otherFlushed, otherErr = tl.Flush(ctx, vc2, store, 1)
	}

	flushed, err := tl.Flush(ctx, vc, store, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, flushed)
	assert.NoError(t, otherErr)
	assert.Equal(t, 3, otherFlushed)

	assert.Equal(t, map[string]int64{"a": 1, "b": 1, "c": 1}, store.totals)
	assert.Equal(t, int64(3), store.lastBatch)
	assertvk.NotExists(t, vc, "{tallies}:flushing")
	assertvk.NotExists(t, vc, "{tallies}:batch")
}

func TestTalliesFlushLargeBatches(t *testing.T) {
	ctx := t.Context()
	vc := assertvk.ClaimDB(t).Pool().Get()
	defer vc.Close()

	tl := vkutil.NewTallies("tallies")
	store := newMemoryTallyStore()

	amounts := make(map[string]int64, 2500)
	for i := range 2500 {
		amounts[fmt.Sprintf("field%d", i)] = 1
	}
	require.NoError(t, tl.Increment(ctx, vc, amounts))

	flushed, err := tl.Flush(ctx, vc, store, 2000)
	require.NoError(t, err)
	assert.Equal(t, 2500, flushed)
	assert.Equal(t, []int{2000, 500}, store.batches)
	assert.Len(t, store.totals, 2500)
}

func TestNewTalliesValidation(t *testing.T) {
	// a key base which can't be used as a hash tag is a programming error, not a runtime one
	for _, keyBase := range []string{"", "{foo}", "foo}"} {
		assert.PanicsWithValue(t, fmt.Sprintf("invalid tallies key base %q", keyBase), func() { vkutil.NewTallies(keyBase) })
	}
}

func TestTalliesKeyBases(t *testing.T) {
	ctx := t.Context()
	vc := assertvk.ClaimDB(t).Pool().Get()
	defer vc.Close()

	foo, bar := vkutil.NewTallies("foo"), vkutil.NewTallies("bar")
	store := newMemoryTallyStore()

	require.NoError(t, foo.Increment(ctx, vc, map[string]int64{"x": 1}))
	require.NoError(t, bar.Increment(ctx, vc, map[string]int64{"x": 2}))

	assertvk.HGetAll(t, vc, "{foo}:live", map[string]string{"x": "1"})
	assertvk.HGetAll(t, vc, "{bar}:live", map[string]string{"x": "2"})

	// each only flushes its own tallies
	flushed, err := foo.Flush(ctx, vc, store, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, flushed)
	assert.Equal(t, map[string]int64{"x": 1}, store.totals)
	assertvk.NotExists(t, vc, "{foo}:live")
	assertvk.HGetAll(t, vc, "{bar}:live", map[string]string{"x": "2"})
}
