package queues

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

// Fair implements a fair queue where tasks are distributed evenly across owners.
//
// A queue with base key "foo" and owners "o1" and "o2" will have the following keys:
//   - {foo}:queued - set of owners scored by number of queued tasks
//   - {foo}:active - set of owners scored by number of active tasks
//   - {foo}:paused - set of paused owners
//   - {foo}:temp - used internally
//   - {foo:q:o1}/0 - e.g. list of tasks for o1 with priority 0 (low)
//   - {foo:q:o1}/1 - e.g. list of tasks for o1 with priority 1 (high)
//   - {foo:q:o2}/0 - e.g. list of tasks for o2 with priority 0 (low)
//   - {foo:q:o2}/1 - e.g. list of tasks for o2 with priority 1 (high)
type Fair struct {
	keyBase           string
	maxActivePerOwner int // max number of active tasks per owner
}

// NewFair creates a new fair queue with the given key base.
func NewFair(keyBase string, maxActivePerOwner int) *Fair {
	return &Fair{keyBase: keyBase, maxActivePerOwner: maxActivePerOwner}
}

//go:embed lua/fair_push.lua
var luaFairPush string
var scriptFairPush = redis.NewScript(4, luaFairPush)

// Push adds the passed in task to our queue for execution
func (q *Fair) Push(ctx context.Context, rc redis.Conn, owner string, priority bool, task []byte) error {
	queueKeys := q.queueKeys(owner)

	_, err := scriptFairPush.Do(rc, q.queuedKey(), q.activeKey(), queueKeys[0], queueKeys[1], owner, priority, task)
	return err
}

//go:embed lua/fair_pop_owner.lua
var luaFairPopOwner string
var scriptFairPopOwner = redis.NewScript(4, luaFairPopOwner)

//go:embed lua/fair_pop_task.lua
var luaFairPopTask string
var scriptFairPopTask = redis.NewScript(3, luaFairPopTask)

// Pop pops the next task off our queue
func (q *Fair) Pop(ctx context.Context, rc redis.Conn) (string, []byte, error) {
	for {
		// Step 1: Select an owner to process
		owner, err := q.popOwner(ctx, rc)
		if err != nil {
			return "", nil, err
		}

		if owner == "" {
			// No owner available
			return "", nil, nil
		}

		// Step 2: Pop a task for the selected owner
		task, err := q.popTask(ctx, rc, owner)
		if err != nil {
			return "", nil, err
		}

		if task != nil {
			// Successfully got a task
			return owner, task, nil
		}

		// No task found for this owner, the popTask script already cleaned up the active count.
		// Retry to select another owner.
		continue
	}
}

// selects the next owner to process tasks for and reserves a slot in the active set.
// This is the first step of the two-step pop process that avoids dynamic key usage.
// Returns the selected owner or empty string if no owner is available.
func (q *Fair) popOwner(ctx context.Context, rc redis.Conn) (string, error) {
	owner, err := redis.String(scriptFairPopOwner.DoContext(ctx, rc, q.queuedKey(), q.activeKey(), q.pausedKey(), q.tempKey(), q.maxActivePerOwner))
	if err != nil {
		return "", err
	}

	return owner, nil
}

// pops a task for the specified owner. This is the second step of the two-step pop process.
// If no task is found, it automatically decrements the active count to clean up the reservation.
// Returns the task data or nil if no task is available.
func (q *Fair) popTask(ctx context.Context, rc redis.Conn, owner string) ([]byte, error) {
	queueKeys := q.queueKeys(owner)

	result, err := redis.String(scriptFairPopTask.DoContext(ctx, rc, q.activeKey(), queueKeys[0], queueKeys[1], owner))
	if err != nil {
		return nil, err
	}

	if result == "" {
		return nil, nil
	} else {
		return []byte(result), nil
	}
}

//go:embed lua/fair_done.lua
var luaFairDone string
var scriptFairDone = redis.NewScript(1, luaFairDone)

// Done marks the passed in task as complete. Callers must call this in order
// to maintain fair workers across orgs
func (q *Fair) Done(ctx context.Context, rc redis.Conn, owner string) error {
	_, err := scriptFairDone.Do(rc, q.activeKey(), owner)
	return err
}

// Pause marks the given owner as paused, disabling processing of their tasks
func (q *Fair) Pause(ctx context.Context, rc redis.Conn, owner string) error {
	_, err := redis.DoContext(rc, ctx, "SADD", q.pausedKey(), owner)
	return err
}

// Resume unmarks the given owner as paused, re-enabling processing of their tasks
func (q *Fair) Resume(ctx context.Context, rc redis.Conn, owner string) error {
	_, err := redis.DoContext(rc, ctx, "SREM", q.pausedKey(), owner)
	return err
}

// Paused returns the list of owners marked as paused
func (q *Fair) Paused(ctx context.Context, rc redis.Conn) ([]string, error) {
	owners, err := redis.Strings(redis.DoContext(rc, ctx, "SMEMBERS", q.pausedKey()))
	if err != nil {
		return nil, err
	}

	return owners, nil
}

// Queued returns the list of owners with queued tasks
func (q *Fair) Queued(ctx context.Context, rc redis.Conn) ([]string, error) {
	owners, err := redis.Strings(redis.DoContext(rc, ctx, "ZRANGE", q.queuedKey(), 0, -1))
	if err != nil {
		return nil, err
	}

	return owners, nil
}

// Size returns the number of queued tasks for the given owner
func (q *Fair) Size(ctx context.Context, rc redis.Conn, owner string) (int, error) {
	queueKeys := q.queueKeys(owner)

	rc.Send("MULTI")
	rc.Send("LLEN", queueKeys[0])
	rc.Send("LLEN", queueKeys[1])
	r, err := redis.Values(redis.DoContext(rc, ctx, "EXEC"))
	if err != nil {
		return 0, err
	}

	counts, err := redis.Ints(r, nil)
	if err != nil {
		return 0, err
	}

	return counts[0] + counts[1], nil
}

func (q *Fair) queuedKey() string {
	return fmt.Sprintf("{%s}:queued", q.keyBase)
}

func (q *Fair) activeKey() string {
	return fmt.Sprintf("{%s}:active", q.keyBase)
}

func (q *Fair) pausedKey() string {
	return fmt.Sprintf("{%s}:paused", q.keyBase)
}

func (q *Fair) tempKey() string {
	return fmt.Sprintf("{%s}:temp", q.keyBase)
}

func (q *Fair) queueKeys(owner string) [2]string {
	return [2]string{
		fmt.Sprintf("{%s:q:%s}/0", q.keyBase, owner),
		fmt.Sprintf("{%s:q:%s}/1", q.keyBase, owner),
	}
}
