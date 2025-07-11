package queues

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

// Fair implements a fair queue where tasks are distributed evenly across owners.
//
// A queue with base key "foo" and owners "owner1" and "owner2" will have the following keys:
//   - {foo}:queued - set of owners scored by number of queued tasks
//   - {foo}:active - set of owners scored by number of active tasks
//   - {foo}:paused - set of paused owners
//   - {foo}:temp - used internally
//   - {foo}:o:owner1/0 - e.g. list of tasks for owner1 with priority 0 (low)
//   - {foo}:o:owner1/1 - e.g. list of tasks for owner1 with priority 1 (high)
//   - {foo}:o:owner2/0 - e.g. list of tasks for owner2 with priority 0 (low)
//   - {foo}:o:owner2/1 - e.g. list of tasks for owner2 with priority 1 (high)
//
// Note: it would be nice if owner queues could use distict hash tags and so live on different nodes in a cluster, but
// our push and pop scripts require atomic changes to the queued/active sets and the task lists.
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
func (q *Fair) Push(ctx context.Context, vc redis.Conn, owner string, priority bool, task []byte) error {
	queueKeys := q.queueKeys(owner)

	_, err := scriptFairPush.Do(vc, q.queuedKey(), q.activeKey(), queueKeys[0], queueKeys[1], owner, priority, task)
	if err != nil {
		return fmt.Errorf("error pushing task for owner %s: %w", owner, err)
	}
	return nil
}

//go:embed lua/fair_pop_owner.lua
var luaFairPopOwner string
var scriptFairPopOwner = redis.NewScript(4, luaFairPopOwner)

//go:embed lua/fair_pop_task.lua
var luaFairPopTask string
var scriptFairPopTask = redis.NewScript(3, luaFairPopTask)

// Pop pops the next task off our queue
func (q *Fair) Pop(ctx context.Context, vc redis.Conn) (string, []byte, error) {
	for {
		// Select an owner with queued tasks
		owner, err := redis.String(scriptFairPopOwner.DoContext(ctx, vc, q.queuedKey(), q.activeKey(), q.pausedKey(), q.tempKey(), q.maxActivePerOwner))
		if err != nil {
			return "", nil, fmt.Errorf("error selecting task owner: %w", err)
		}
		if owner == "" { // None found so no tasks to pop
			return "", nil, nil
		}

		// Pop a task for the owner
		queueKeys := q.queueKeys(owner)
		result, err := redis.String(scriptFairPopTask.DoContext(ctx, vc, q.activeKey(), queueKeys[0], queueKeys[1], owner))
		if err != nil {
			return "", nil, fmt.Errorf("error popping task for owner %s: %w", owner, err)
		}
		if result != "" {
			return owner, []byte(result), nil
		}

		// It's possible that we selected an owner with no tasks, so go back around again
	}
}

//go:embed lua/fair_done.lua
var luaFairDone string
var scriptFairDone = redis.NewScript(1, luaFairDone)

// Done marks the passed in task as complete. Callers must call this in order
// to maintain fair workers across orgs
func (q *Fair) Done(ctx context.Context, vc redis.Conn, owner string) error {
	_, err := scriptFairDone.Do(vc, q.activeKey(), owner)
	if err != nil {
		return fmt.Errorf("error marking task done for owner %s: %w", owner, err)
	}
	return nil
}

// Pause marks the given owner as paused, disabling processing of their tasks
func (q *Fair) Pause(ctx context.Context, vc redis.Conn, owner string) error {
	_, err := redis.DoContext(vc, ctx, "SADD", q.pausedKey(), owner)
	return err
}

// Resume unmarks the given owner as paused, re-enabling processing of their tasks
func (q *Fair) Resume(ctx context.Context, vc redis.Conn, owner string) error {
	_, err := redis.DoContext(vc, ctx, "SREM", q.pausedKey(), owner)
	return err
}

// Paused returns the list of owners marked as paused
func (q *Fair) Paused(ctx context.Context, vc redis.Conn) ([]string, error) {
	owners, err := redis.Strings(redis.DoContext(vc, ctx, "SMEMBERS", q.pausedKey()))
	if err != nil {
		return nil, err
	}

	return owners, nil
}

// Queued returns the list of owners with queued tasks
func (q *Fair) Queued(ctx context.Context, vc redis.Conn) ([]string, error) {
	owners, err := redis.Strings(redis.DoContext(vc, ctx, "ZRANGE", q.queuedKey(), 0, -1))
	if err != nil {
		return nil, err
	}

	return owners, nil
}

// Size returns the number of queued tasks for the given owner
func (q *Fair) Size(ctx context.Context, vc redis.Conn, owner string) (int, error) {
	queueKeys := q.queueKeys(owner)

	vc.Send("MULTI")
	vc.Send("LLEN", queueKeys[0])
	vc.Send("LLEN", queueKeys[1])
	counts, err := redis.Ints(redis.DoContext(vc, ctx, "EXEC"))
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
		fmt.Sprintf("{%s}:o:%s/0", q.keyBase, owner),
		fmt.Sprintf("{%s}:o:%s/1", q.keyBase, owner),
	}
}
