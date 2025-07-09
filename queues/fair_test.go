package queues_test

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/random"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/nyaruka/vkutil/queues"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFair(t *testing.T) {
	ctx := context.Background()
	vp := assertvk.TestDB()
	vc := vp.Get()
	defer vc.Close()

	defer assertvk.FlushDB()

	q := queues.NewFair("test", 3)

	assertQueued := func(expected map[string]int) {
		actualStrings, err := valkey.StringMap(valkey.DoContext(vc, context.Background(), "ZRANGE", "{test}:queued", 0, -1, "WITHSCORES"))
		require.NoError(t, err)

		actual := make(map[string]int, len(actualStrings))
		for k, v := range actualStrings {
			actual[k], err = strconv.Atoi(v)
			require.NoError(t, err)
		}

		assert.Equal(t, expected, actual)

		// checked the .Queued method as well
		actualOwners, err := q.Queued(ctx, vc)
		assert.NoError(t, err)
		assert.ElementsMatch(t, slices.Collect(maps.Keys(expected)), actualOwners)
	}

	assertActive := func(expected map[string]int) {
		actualStrings, err := valkey.StringMap(valkey.DoContext(vc, context.Background(), "ZRANGE", "{test}:active", 0, -1, "WITHSCORES"))
		require.NoError(t, err)

		actual := make(map[string]int, len(actualStrings))
		for k, v := range actualStrings {
			actual[k], err = strconv.Atoi(v)
			require.NoError(t, err)
		}

		assert.Equal(t, expected, actual)
	}

	assertTasks := func(owner string, expected0, expected1 []string) {
		actual0, err := valkey.Strings(valkey.DoContext(vc, context.Background(), "LRANGE", "{test:"+owner+"}/0", 0, -1))
		require.NoError(t, err)
		actual1, err := valkey.Strings(valkey.DoContext(vc, context.Background(), "LRANGE", "{test:"+owner+"}/1", 0, -1))
		require.NoError(t, err)

		assert.Equal(t, expected0, actual0, "priority 0 tasks mismatch")
		assert.Equal(t, expected1, actual1, "priority 1 tasks mismatch")

		// checked .Size() method as well
		size, err := q.Size(ctx, vc, owner)
		assert.NoError(t, err)
		assert.Equal(t, len(expected0)+len(expected1), size)
	}

	assertQueued(map[string]int{})
	assertActive(map[string]int{})
	assertTasks("owner1", []string{}, []string{})
	assertTasks("owner2", []string{}, []string{})

	q.Push(ctx, vc, "owner1", false, []byte(`task1`))
	q.Push(ctx, vc, "owner1", true, []byte(`task2`))
	q.Push(ctx, vc, "owner2", false, []byte(`task3`))
	q.Push(ctx, vc, "owner1", false, []byte(`task4`))
	q.Push(ctx, vc, "owner2", true, []byte(`task5`))

	// nobody processing any tasks so no workers assigned in active set
	assertQueued(map[string]int{"owner1": 3, "owner2": 2})
	assertActive(map[string]int{})
	assertTasks("owner1", []string{`task1`, `task4`}, []string{`task2`})
	assertTasks("owner2", []string{`task3`}, []string{`task5`})

	assertPop(t, q, vc, "owner1", "task2") // because it's highest priority for owner 1
	assertQueued(map[string]int{"owner1": 2, "owner2": 2})
	assertActive(map[string]int{"owner1": 1})

	assertPop(t, q, vc, "owner2", "task5") // because it's highest priority for owner 2
	assertQueued(map[string]int{"owner1": 2, "owner2": 1})
	assertActive(map[string]int{"owner1": 1, "owner2": 1})

	assertPop(t, q, vc, "owner1", "task1")
	assertQueued(map[string]int{"owner1": 1, "owner2": 1})
	assertActive(map[string]int{"owner1": 2, "owner2": 1})
	assertTasks("owner1", []string{`task4`}, []string{})
	assertTasks("owner2", []string{`task3`}, []string{})

	// mark task2 and task1 (owner1) as complete
	q.Done(ctx, vc, "owner1")
	q.Done(ctx, vc, "owner1")

	assertQueued(map[string]int{"owner1": 1, "owner2": 1})
	assertActive(map[string]int{"owner2": 1})

	assertPop(t, q, vc, "owner1", "task4")
	assertPop(t, q, vc, "owner2", "task3")
	assertTasks("owner1", []string{}, []string{})
	assertTasks("owner2", []string{}, []string{})

	assertQueued(map[string]int{})
	assertActive(map[string]int{"owner1": 1, "owner2": 2})

	assertPop(t, q, vc, "", "") // no more tasks
	assertTasks("owner1", []string{}, []string{})
	assertTasks("owner2", []string{}, []string{})

	assertQueued(map[string]int{})
	assertActive(map[string]int{"owner1": 1, "owner2": 2})

	// mark remaining tasks as complete
	q.Done(ctx, vc, "owner1")
	q.Done(ctx, vc, "owner2")
	q.Done(ctx, vc, "owner2")

	assertQueued(map[string]int{})
	assertActive(map[string]int{})

	q.Push(ctx, vc, "owner1", false, []byte(`task6`))
	q.Push(ctx, vc, "owner1", false, []byte(`task7`))
	q.Push(ctx, vc, "owner2", false, []byte(`task8`))
	q.Push(ctx, vc, "owner2", false, []byte(`task9`))

	assertPop(t, q, vc, "owner1", "task6")

	q.Pause(ctx, vc, "owner1")
	q.Pause(ctx, vc, "owner1") // no-op if already paused

	assertQueued(map[string]int{"owner1": 1, "owner2": 2})
	assertActive(map[string]int{"owner1": 1})

	paused, err := q.Paused(ctx, vc)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"owner1"}, paused)

	assertPop(t, q, vc, "owner2", "task8")
	assertPop(t, q, vc, "owner2", "task9")
	assertPop(t, q, vc, "", "") // no more tasks

	q.Resume(ctx, vc, "owner1")
	q.Resume(ctx, vc, "owner1") // no-op if already active

	assertQueued(map[string]int{"owner1": 1})
	assertActive(map[string]int{"owner1": 1, "owner2": 2})

	paused, err = q.Paused(ctx, vc)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{}, paused)

	assertPop(t, q, vc, "owner1", "task7")

	q.Done(ctx, vc, "owner1")
	q.Done(ctx, vc, "owner1")
	q.Done(ctx, vc, "owner2")
	q.Done(ctx, vc, "owner2")

	assertQueued(map[string]int{})
	assertActive(map[string]int{})

	// if we somehow get into a state where an owner is in the queued set but doesn't have queued tasks, pop will retry
	q.Push(ctx, vc, "owner1", false, []byte("task6"))
	q.Push(ctx, vc, "owner2", false, []byte("task7"))

	assertQueued(map[string]int{"owner1": 1, "owner2": 1})
	assertActive(map[string]int{})

	assertvk.LLen(t, vc, "{test:owner1}/0", 1)
	_, err = vc.Do("DEL", "{test:owner1}/0")
	assert.NoError(t, err)

	assertPop(t, q, vc, "owner2", "task7")
	assertPop(t, q, vc, "", "")

	assertQueued(map[string]int{})
	assertActive(map[string]int{"owner2": 1})

	// if we somehow call done too many times, we never get negative workers
	q.Done(ctx, vc, "owner2")
	q.Done(ctx, vc, "owner2")

	assertActive(map[string]int{})
}

func TestFairMaxActivePerOwner(t *testing.T) {
	ctx := context.Background()
	vp := assertvk.TestDB()
	vc := vp.Get()
	defer vc.Close()

	defer assertvk.FlushDB()

	q := queues.NewFair("test", 2)

	q.Push(ctx, vc, "owner1", false, []byte(`task1`))
	q.Push(ctx, vc, "owner1", true, []byte(`task2`))
	q.Push(ctx, vc, "owner1", false, []byte(`task3`))

	assertPop(t, q, vc, "owner1", "task2")
	assertPop(t, q, vc, "owner1", "task1")
	assertPop(t, q, vc, "", "") // owner1 has reached max active tasks

	q.Done(ctx, vc, "owner1")

	assertPop(t, q, vc, "owner1", "task3") // now we can pop task3
}

func TestFairConcurrency(t *testing.T) {
	ctx := context.Background()
	vp := assertvk.TestDB()
	vc := vp.Get()
	defer vc.Close()

	defer assertvk.FlushDB()

	q := queues.NewFair("test", 5) // one owner can only occupy 5 of the 10 consumers at a time

	type ownerAndTask struct {
		owner string
		task  string
	}

	numTasks := 10000
	pushedTasks := make([]*ownerAndTask, 0, numTasks)
	poppedTasks := make([]*ownerAndTask, 0, numTasks)

	var wg sync.WaitGroup
	var mutex sync.Mutex

	recordTaskPushed := func(owner, task string) {
		mutex.Lock()
		defer mutex.Unlock()

		pushedTasks = append(pushedTasks, &ownerAndTask{owner: owner, task: task})
	}

	recordTaskProcessed := func(owner, task string) {
		mutex.Lock()
		defer mutex.Unlock()

		poppedTasks = append(poppedTasks, &ownerAndTask{owner: owner, task: task})
	}

	random.IntN(5)

	// Start 5 producers to push tasks each concurrently
	for i := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			vc := vp.Get()
			defer vc.Close()

			for range numTasks / 5 {
				owner := fmt.Sprintf("owner%d", random.IntN(5)+1) // five possible owners (1...5)
				task := []byte(uuids.NewV7())
				err := q.Push(ctx, vc, owner, false, task)
				assert.NoError(t, err, "Producer %d failed to push task for owner %s", i, owner)

				recordTaskPushed(owner, string(task))

				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		}()
	}

	// Start 10 consumers to pop tasks concurrently
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			vc := vp.Get()
			defer vc.Close()

			for {
				owner, task, err := q.Pop(ctx, vc)
				assert.NoError(t, err, "Consumer %d failed to pop task", i)

				if task != nil {
					time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

					err = q.Done(ctx, vc, owner)
					assert.NoError(t, err, "Consumer %d failed to mark task done", i)

					recordTaskProcessed(owner, string(task))

					fmt.Printf("Consumer %d processed task %s for owner %s\n", i, string(task), owner)
				} else {
					fmt.Printf("Consumer %d got no task when popping\n", i)
				}
				// Check if all tasks have been processed
				mutex.Lock()
				allDone := len(poppedTasks) >= numTasks
				mutex.Unlock()

				if allDone {
					return
				} else {
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	wg.Wait() // Wait for all producers and consumers to complete

	// can't guarantee order of processed tasks, but we can check that all expected tasks were processed
	assert.ElementsMatch(t, pushedTasks, poppedTasks)

	assertvk.ZGetAll(t, vc, "{test}:queued", map[string]float64{})
	assertvk.ZGetAll(t, vc, "{test}:active", map[string]float64{})

	for i := range 5 {
		assertvk.LGetAll(t, vc, fmt.Sprintf("{test:owner%d}/0", i+1), []string{})
		assertvk.LGetAll(t, vc, fmt.Sprintf("{test:owner%d}/1", i+1), []string{})
	}
}

// assertPop is a helper function that asserts the result of a Pop operation
func assertPop(t *testing.T, q *queues.Fair, vc valkey.Conn, expectedOwner, expectedTask string) {
	owner, task, err := q.Pop(context.Background(), vc)
	require.NoError(t, err)
	if expectedTask != "" {
		assert.Equal(t, expectedOwner, owner)
		assert.Equal(t, expectedTask, string(task))
	} else {
		assert.Nil(t, task)
	}
}
