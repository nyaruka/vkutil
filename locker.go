package vkutil

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Locker is a lock implementation where grabbing returns a lock value and that value must be
// used to release or extend the lock.
type Locker struct {
	key        string
	expiration time.Duration
}

// NewLocker creates a new locker using the given key and expiration
func NewLocker(key string, expiration time.Duration) *Locker {
	return &Locker{key: key, expiration: expiration}
}

// Grab tries to grab this lock in an atomic operation. It returns the lock value if successful.
// It will retry every second until the retry period has ended, returning empty string if not
// acquired in that time.
func (l *Locker) Grab(ctx context.Context, rp *redis.Pool, retry time.Duration) (string, error) {
	value := RandomBase64(10)                  // generate our lock value
	expires := int(l.expiration / time.Second) // convert our expiration to seconds

	start := time.Now()
	for {
		rc := rp.Get()
		success, err := redis.DoContext(rc, ctx, "SET", l.key, value, "EX", expires, "NX")
		rc.Close()

		if err != nil {
			return "", fmt.Errorf("error trying to get lock: %w", err)
		}
		if success == "OK" {
			break
		}

		if time.Since(start) > retry {
			return "", nil
		}

		time.Sleep(time.Second)
	}

	return value, nil
}

//go:embed lua/locker_release.lua
var lockerRelease string
var lockerReleaseScript = redis.NewScript(1, lockerRelease)

// Release releases this lock if the given lock value is correct (i.e we own this lock). It is not an
// error to release a lock that is no longer present.
func (l *Locker) Release(ctx context.Context, rp *redis.Pool, value string) error {
	rc := rp.Get()
	defer rc.Close()

	// we use lua here because we only want to release the lock if we own it
	_, err := lockerReleaseScript.DoContext(ctx, rc, l.key, value)
	return err
}

//go:embed lua/locker_extend.lua
var lockerExtend string
var lockerExtendScript = redis.NewScript(1, lockerExtend)

// Extend extends our lock expiration by the passed in number of seconds provided the lock value is correct
func (l *Locker) Extend(ctx context.Context, rp *redis.Pool, value string, expiration time.Duration) error {
	rc := rp.Get()
	defer rc.Close()

	seconds := int(expiration / time.Second) // convert our expiration to seconds

	// we use lua here because we only want to set the expiration time if we own it
	_, err := lockerExtendScript.DoContext(ctx, rc, l.key, value, seconds)
	return err
}

// IsLocked returns whether this lock is currently held by any process.
func (l *Locker) IsLocked(ctx context.Context, rp *redis.Pool) (bool, error) {
	rc := rp.Get()
	defer rc.Close()

	exists, err := redis.Bool(redis.DoContext(rc, ctx, "EXISTS", l.key))
	if err != nil {
		return false, err
	}

	return exists, nil
}
