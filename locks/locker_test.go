package locks_test

import (
	"context"
	"testing"
	"time"

	"github.com/nyaruka/vkutil/assertvk"
	"github.com/nyaruka/vkutil/locks"
	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	ctx := context.Background()
	vp := assertvk.TestDB()
	vc := vp.Get()
	defer vc.Close()

	defer assertvk.FlushDB()

	locker := locks.NewLocker("test", time.Second*5)

	isLocked, err := locker.IsLocked(ctx, vp)
	assert.NoError(t, err)
	assert.False(t, isLocked)

	// grab lock
	lock1, err := locker.Grab(ctx, vp, time.Second)
	assert.NoError(t, err)
	assert.NotZero(t, lock1)

	isLocked, err = locker.IsLocked(ctx, vp)
	assert.NoError(t, err)
	assert.True(t, isLocked)

	assertvk.Exists(t, vc, "test")

	// try to acquire the same lock, should fail
	lock2, err := locker.Grab(ctx, vp, time.Second)
	assert.NoError(t, err)
	assert.Zero(t, lock2)

	// should succeed if we wait longer
	lock3, err := locker.Grab(ctx, vp, time.Second*6)
	assert.NoError(t, err)
	assert.NotZero(t, lock3)
	assert.NotEqual(t, lock1, lock3)

	// extend the lock
	err = locker.Extend(ctx, vp, lock3, time.Second*10)
	assert.NoError(t, err)

	// trying to grab it should fail with a 5 second timeout
	lock4, err := locker.Grab(ctx, vp, time.Second*5)
	assert.NoError(t, err)
	assert.Zero(t, lock4)

	// try to release the lock with wrong value
	err = locker.Release(ctx, vp, "2352")
	assert.NoError(t, err)

	// no error but also dooesn't release the lock
	assertvk.Exists(t, vc, "test")

	// release the lock
	err = locker.Release(ctx, vp, lock3)
	assert.NoError(t, err)

	assertvk.NotExists(t, vc, "test")

	// new grab should work
	lock5, err := locker.Grab(ctx, vp, time.Second*5)
	assert.NoError(t, err)
	assert.NotZero(t, lock5)

	assertvk.Exists(t, vc, "test")
}

func TestLockerSubSecondExpiration(t *testing.T) {
	ctx := context.Background()
	vp := assertvk.TestDB()
	vc := vp.Get()
	defer vc.Close()

	defer assertvk.FlushDB()

	locker := locks.NewLocker("test", time.Minute)

	lock, err := locker.Grab(ctx, vp, time.Second)
	assert.NoError(t, err)
	assert.NotZero(t, lock)

	// extending by under a second must shorten the lock, not release it
	err = locker.Extend(ctx, vp, lock, time.Millisecond*900)
	assert.NoError(t, err)

	assertvk.Exists(t, vc, "test")

	// so nobody else can grab it
	other, err := locker.Grab(ctx, vp, 0)
	assert.NoError(t, err)
	assert.Zero(t, other)

	// an expiration which doesn't round to at least a millisecond is an error rather than a release
	err = locker.Extend(ctx, vp, lock, time.Microsecond*500)
	assert.EqualError(t, err, "lock expiration must be at least 1ms, got 500µs")

	assertvk.Exists(t, vc, "test")

	assert.NoError(t, locker.Release(ctx, vp, lock))

	// a locker with a sub-second expiration works and actually expires
	locker2 := locks.NewLocker("test2", time.Millisecond*300)
	lock2, err := locker2.Grab(ctx, vp, 0)
	assert.NoError(t, err)
	assert.NotZero(t, lock2)

	assertvk.Exists(t, vc, "test2")

	time.Sleep(time.Millisecond * 400)

	assertvk.NotExists(t, vc, "test2")

	// a locker whose expiration rounds to nothing errors rather than misbehaving
	_, err = locks.NewLocker("test3", time.Microsecond).Grab(ctx, vp, 0)
	assert.EqualError(t, err, "lock expiration must be at least 1ms, got 1µs")
}
