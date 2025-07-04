package queues_test

import (
	"context"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/nyaruka/vkutil/queues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFair(t *testing.T) {
	ctx := context.Background()
	rp := assertvk.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertvk.FlushDB()

	q := queues.NewFair("test", 3)

	assertSize := func(owner string, expected int) {
		size, err := q.Size(ctx, rc, owner)
		assert.NoError(t, err)
		assert.Equal(t, expected, size)
	}

	assertQueued := func(expected []string) {
		actual, err := q.Queued(ctx, rc)
		assert.NoError(t, err)
		assert.ElementsMatch(t, expected, actual)
	}

	assertSize("owner1", 0)
	assertSize("owner2", 0)

	q.Push(ctx, rc, "owner1", false, []byte(`task1`))
	q.Push(ctx, rc, "owner1", true, []byte(`task2`))
	q.Push(ctx, rc, "owner2", false, []byte(`task3`))
	q.Push(ctx, rc, "owner1", false, []byte(`task4`))
	q.Push(ctx, rc, "owner2", true, []byte(`task5`))

	// nobody processing any tasks so no workers assigned in active set
	assertvk.ZGetAll(t, rc, "test:queued", map[string]float64{"owner1": 3, "owner2": 2})
	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{})
	assertvk.LGetAll(t, rc, "test:q:owner1/0", []string{`task1`, `task4`})
	assertvk.LGetAll(t, rc, "test:q:owner1/1", []string{`task2`})
	assertvk.LGetAll(t, rc, "test:q:owner2/0", []string{`task3`})
	assertvk.LGetAll(t, rc, "test:q:owner2/1", []string{`task5`})

	assertSize("owner1", 3)
	assertSize("owner2", 2)

	assertPop(t, q, rc, "owner1", "task2") // because it's highest priority for owner 1
	assertvk.ZGetAll(t, rc, "test:queued", map[string]float64{"owner1": 2, "owner2": 2})
	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{"owner1": 1})

	assertPop(t, q, rc, "owner2", "task5") // because it's highest priority for owner 2
	assertvk.ZGetAll(t, rc, "test:queued", map[string]float64{"owner1": 2, "owner2": 1})
	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{"owner1": 1, "owner2": 1})

	assertPop(t, q, rc, "owner1", "task1")
	assertvk.ZGetAll(t, rc, "test:queued", map[string]float64{"owner1": 1, "owner2": 1})
	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{"owner1": 2, "owner2": 1})

	assertQueued([]string{"owner1", "owner2"})
	assertSize("owner1", 1)
	assertSize("owner2", 1)

	// mark task2 and task1 (owner1) as complete
	q.Done(ctx, rc, "owner1")
	q.Done(ctx, rc, "owner1")

	assertvk.ZGetAll(t, rc, "test:queued", map[string]float64{"owner1": 1, "owner2": 1})
	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{"owner2": 1})

	assertPop(t, q, rc, "owner1", "task4")
	assertPop(t, q, rc, "owner2", "task3")
	assertPop(t, q, rc, "", "") // no more tasks

	assertSize("owner1", 0)
	assertSize("owner2", 0)

	assertvk.ZGetAll(t, rc, "test:queued", map[string]float64{})
	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{})

	q.Push(ctx, rc, "owner1", false, []byte(`task6`))
	q.Push(ctx, rc, "owner1", false, []byte(`task7`))
	q.Push(ctx, rc, "owner2", false, []byte(`task8`))
	q.Push(ctx, rc, "owner2", false, []byte(`task9`))

	assertPop(t, q, rc, "owner1", "task6")

	q.Pause(ctx, rc, "owner1")
	q.Pause(ctx, rc, "owner1") // no-op if already paused

	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{"owner1": 1})
	paused, err := q.Paused(ctx, rc)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"owner1"}, paused)
	assertQueued([]string{"owner1", "owner2"})

	assertPop(t, q, rc, "owner2", "task8")
	assertPop(t, q, rc, "owner2", "task9")
	assertPop(t, q, rc, "", "") // no more tasks

	q.Resume(ctx, rc, "owner1")
	q.Resume(ctx, rc, "owner1") // no-op if already active

	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{"owner1": 1})
	paused, err = q.Paused(ctx, rc)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, paused)
	assertQueued([]string{"owner1"})

	assertPop(t, q, rc, "owner1", "task7")

	q.Done(ctx, rc, "owner1")
	q.Done(ctx, rc, "owner1")
	q.Done(ctx, rc, "owner2")
	q.Done(ctx, rc, "owner2")

	// if we somehow get into a state where an owner is in the active set but doesn't have queued tasks, pop will retry
	q.Push(ctx, rc, "owner1", false, []byte("task6"))
	q.Push(ctx, rc, "owner2", false, []byte("task7"))

	assertvk.LLen(t, rc, "test:q:owner1/0", 1)
	_, err = rc.Do("DEL", "test:q:owner1/0")
	assert.NoError(t, err)

	assertPop(t, q, rc, "owner2", "task7")
	assertPop(t, q, rc, "", "")

	// if we somehow call done too many times, we never get negative workers
	q.Push(ctx, rc, "owner1", false, []byte("task8"))
	q.Done(ctx, rc, "owner1")
	q.Done(ctx, rc, "owner1")

	assertvk.ZGetAll(t, rc, "test:active", map[string]float64{})
}

func TestFairMaxActivePerOwner(t *testing.T) {
	ctx := context.Background()
	rp := assertvk.TestDB()
	rc := rp.Get()
	defer rc.Close()

	defer assertvk.FlushDB()

	q := queues.NewFair("test", 2)

	q.Push(ctx, rc, "owner1", false, []byte(`task1`))
	q.Push(ctx, rc, "owner1", true, []byte(`task2`))
	q.Push(ctx, rc, "owner1", false, []byte(`task3`))

	assertPop(t, q, rc, "owner1", "task2")
	assertPop(t, q, rc, "owner1", "task1")
	assertPop(t, q, rc, "", "") // owner1 has reached max active tasks

	q.Done(ctx, rc, "owner1")

	assertPop(t, q, rc, "owner1", "task3") // now we can pop task3
}

// assertPop is a helper function that asserts the result of a Pop operation
func assertPop(t *testing.T, q *queues.Fair, rc redis.Conn, expectedOwner, expectedTask string) {
	owner, task, err := q.Pop(context.Background(), rc)
	require.NoError(t, err)
	if expectedTask != "" {
		assert.Equal(t, expectedOwner, owner)
		assert.Equal(t, expectedTask, string(task))
	} else {
		assert.Nil(t, task)
	}
}
