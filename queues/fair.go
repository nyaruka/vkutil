package queues

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

// Fair implements a fair queue where tasks are distributed evenly across owners.
//
// foo:active - set of owners, scored by number of active tasks
// foo:paused - set of paused owners
// foo:q:<OWNER>/0 - e.g. list of tasks for <OWNER> with priority 0 (low)
// foo:q:<OWNER>/1 - e.g. list of tasks for <OWNER> with priority 1 (high)
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
var scriptFairPush = redis.NewScript(3, luaFairPush)

// Push adds the passed in task to our queue for execution
func (q *Fair) Push(ctx context.Context, rc redis.Conn, owner string, priority bool, task []byte) error {
	queueKeys := q.queueKeys(owner)

	_, err := scriptFairPush.Do(rc, q.activeKey(), queueKeys[0], queueKeys[1], owner, priority, task)
	return err
}

//go:embed lua/fair_pop.lua
var luaFairPop string
var scriptFairPop = redis.NewScript(3, luaFairPop)

// Pop pops the next task off our queue
func (q *Fair) Pop(ctx context.Context, rc redis.Conn) (string, []byte, error) {
	for {
		values, err := redis.Strings(scriptFairPop.DoContext(ctx, rc, q.activeKey(), q.pausedKey(), q.tempKey(), q.keyBase, q.maxActivePerOwner))
		if err != nil {
			return "", nil, err
		}

		if values[0] == "empty" {
			return "", nil, nil
		} else if values[0] == "retry" {
			continue
		} else if values[0] == "ok" {
			return values[1], []byte(values[2]), err
		}

		panic("pop script returned unexpected value: " + values[0])
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

// Paused returns the list of paused owners
func (q *Fair) Paused(ctx context.Context, rc redis.Conn) ([]string, error) {
	owners, err := redis.Strings(redis.DoContext(rc, ctx, "SMEMBERS", q.pausedKey()))
	if err != nil {
		return nil, err
	}

	return owners, nil
}

func (q *Fair) Owners(ctx context.Context, rc redis.Conn) ([]string, error) {
	owners, err := redis.Strings(redis.DoContext(rc, ctx, "ZRANGE", q.activeKey(), 0, -1))
	if err != nil {
		return nil, err
	}

	return owners, nil
}

// Size returns the number of queued tasks for the given owner.
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

func (q *Fair) activeKey() string {
	return fmt.Sprintf("%s:active", q.keyBase)
}

func (q *Fair) pausedKey() string {
	return fmt.Sprintf("%s:paused", q.keyBase)
}

func (q *Fair) tempKey() string {
	return fmt.Sprintf("%s:temp", q.keyBase)
}

func (q *Fair) queueKeys(owner string) [2]string {
	return [2]string{
		fmt.Sprintf("%s:q:%s/0", q.keyBase, owner),
		fmt.Sprintf("%s:q:%s/1", q.keyBase, owner),
	}
}
