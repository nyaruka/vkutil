package queues

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"

	"github.com/valkey-io/valkey-go"
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
var scriptFairPush = valkey.NewLuaScript(luaFairPush)

// Push adds the passed in task to our queue for execution
func (q *Fair) Push(ctx context.Context, vc valkey.Client, owner string, priority bool, task []byte) error {
	queueKeys := q.queueKeys(owner)

	priorityStr := "0"
	if priority {
		priorityStr = "1"
	}
	err := scriptFairPush.Exec(ctx, vc, []string{q.queuedKey(), q.activeKey(), queueKeys[0], queueKeys[1]}, []string{owner, priorityStr, string(task)}).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		return fmt.Errorf("error pushing task for owner %s: %w", owner, err)
	}

	return nil
}

//go:embed lua/fair_pop_owner.lua
var luaFairPopOwner string
var scriptFairPopOwner = valkey.NewLuaScript(luaFairPopOwner)

//go:embed lua/fair_pop_task.lua
var luaFairPopTask string
var scriptFairPopTask = valkey.NewLuaScript(luaFairPopTask)

// Pop pops the next task off our queue
func (q *Fair) Pop(ctx context.Context, vc valkey.Client) (string, []byte, error) {
	for {
		// Select an owner with queued tasks
		owner, err := scriptFairPopOwner.Exec(ctx, vc, []string{q.queuedKey(), q.activeKey(), q.pausedKey(), q.tempKey()}, []string{strconv.Itoa(q.maxActivePerOwner)}).ToString()
		if err != nil {
			return "", nil, fmt.Errorf("error selecting task owner: %w", err)
		}
		if owner == "" { // None found so no tasks to pop
			return "", nil, nil
		}

		// Pop a task for the owner
		queueKeys := q.queueKeys(owner)
		result, err := scriptFairPopTask.Exec(ctx, vc, []string{q.activeKey(), queueKeys[0], queueKeys[1]}, []string{owner}).ToString()
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
var scriptFairDone = valkey.NewLuaScript(luaFairDone)

// Done marks the passed in task as complete. Callers must call this in order
// to maintain fair workers across orgs
func (q *Fair) Done(ctx context.Context, vc valkey.Client, owner string) error {
	err := scriptFairDone.Exec(ctx, vc, []string{q.activeKey()}, []string{owner}).Error()
	if err != nil && !valkey.IsValkeyNil(err) {
		return fmt.Errorf("error marking task done for owner %s: %w", owner, err)
	}
	return nil
}

// Pause marks the given owner as paused, disabling processing of their tasks
func (q *Fair) Pause(ctx context.Context, vc valkey.Client, owner string) error {
	return vc.Do(ctx, vc.B().Sadd().Key(q.pausedKey()).Member(owner).Build()).Error()
}

// Resume unmarks the given owner as paused, re-enabling processing of their tasks
func (q *Fair) Resume(ctx context.Context, vc valkey.Client, owner string) error {
	return vc.Do(ctx, vc.B().Srem().Key(q.pausedKey()).Member(owner).Build()).Error()
}

// Paused returns the list of owners marked as paused
func (q *Fair) Paused(ctx context.Context, vc valkey.Client) ([]string, error) {
	return vc.Do(ctx, vc.B().Smembers().Key(q.pausedKey()).Build()).AsStrSlice()
}

// Queued returns the list of owners with queued tasks
func (q *Fair) Queued(ctx context.Context, vc valkey.Client) ([]string, error) {
	return vc.Do(ctx, vc.B().Zrange().Key(q.queuedKey()).Min("0").Max("-1").Build()).AsStrSlice()
}

// Size returns the number of queued tasks for the given owner
func (q *Fair) Size(ctx context.Context, vc valkey.Client, owner string) (int, error) {
	queueKeys := q.queueKeys(owner)

	results := vc.DoMulti(ctx, vc.B().Llen().Key(queueKeys[0]).Build(),
		vc.B().Llen().Key(queueKeys[1]).Build())

	if len(results) != 2 {
		return 0, fmt.Errorf("unexpected result count from DoMulti: %d", len(results))
	}

	len0, err := results[0].AsInt64()
	if err != nil {
		return 0, fmt.Errorf("error getting length of queue 0: %w", err)
	}
	len1, err := results[1].AsInt64()
	if err != nil {
		return 0, fmt.Errorf("error getting length of queue 1: %w", err)
	}

	return int(len0 + len1), nil
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
