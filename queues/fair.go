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
// foo:<OWNER>/0 - e.g. list of tasks for <OWNER> with priority 0 (low)
// foo:<OWNER>/1 - e.g. list of tasks for <OWNER> with priority 1 (high)
type Fair struct {
	keyBase string
}

func NewFair(keyBase string) *Fair {
	return &Fair{keyBase: keyBase}
}

//go:embed lua/fair_push.lua
var luaFairPush string
var scriptFairPush = redis.NewScript(3, luaFairPush)

// Push adds the passed in task to our queue for execution
func (q *Fair) Push(ctx context.Context, rc redis.Conn, owner string, priority bool, task []byte) error {
	_, err := scriptFairPush.Do(rc, q.activeKey(), q.queueKey(owner, false), q.queueKey(owner, true), owner, priority, task)
	return err
}

//go:embed lua/fair_pop.lua
var luaFairPop string
var scriptFairPop = redis.NewScript(3, luaFairPop)

// Pop pops the next task off our queue
func (q *Fair) Pop(ctx context.Context, rc redis.Conn) (string, []byte, error) {
	for {
		values, err := redis.Strings(scriptFairPop.DoContext(ctx, rc, q.activeKey(), q.pausedKey(), q.tempKey(), q.keyBase))
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

func (q *Fair) Pause(ctx context.Context, rc redis.Conn, owner string) error {
	_, err := redis.DoContext(rc, ctx, "SADD", q.pausedKey(), owner)
	return err
}

func (q *Fair) Resume(ctx context.Context, rc redis.Conn, owner string) error {
	_, err := redis.DoContext(rc, ctx, "SREM", q.pausedKey(), owner)
	return err
}

//go:embed lua/fair_size.lua
var luaFairSize string
var scriptFairSize = redis.NewScript(1, luaFairSize)

// Size returns the total number of queued tasks across all owners
func (q *Fair) Size(ctx context.Context, rc redis.Conn) (int, error) {
	return redis.Int(scriptFairSize.DoContext(ctx, rc, q.activeKey(), q.keyBase))
}

func (q *Fair) Owners(ctx context.Context, rc redis.Conn) ([]string, error) {
	owners, err := redis.Strings(redis.DoContext(rc, ctx, "ZRANGE", q.activeKey(), 0, -1))
	if err != nil {
		return nil, err
	}

	return owners, nil
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

func (q *Fair) queueKey(owner string, priority bool) string {
	if priority {
		return fmt.Sprintf("%s:q:%s/1", q.keyBase, owner)
	}
	return fmt.Sprintf("%s:q:%s/0", q.keyBase, owner)
}
