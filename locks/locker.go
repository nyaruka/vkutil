package locks

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/nyaruka/vkutil"
	"github.com/valkey-io/valkey-go"
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
func (l *Locker) Grab(ctx context.Context, vc valkey.Client, retry time.Duration) (string, error) {
	value := vkutil.RandomBase64(10)             // generate our lock value
	expires := int64(l.expiration / time.Second) // convert our expiration to seconds

	start := time.Now()
	for {
		success, err := vc.Do(ctx, vc.B().Set().Key(l.key).Value(value).Nx().ExSeconds(expires).Build()).ToString()
		if err != nil && !valkey.IsValkeyNil(err) {
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
var lockerReleaseScript = valkey.NewLuaScript(lockerRelease)

// Release releases this lock if the given lock value is correct (i.e we own this lock). It is not an
// error to release a lock that is no longer present.
func (l *Locker) Release(ctx context.Context, vc valkey.Client, value string) error {
	// we use lua here because we only want to release the lock if we own it
	return lockerReleaseScript.Exec(ctx, vc, []string{l.key}, []string{value}).Error()
}

//go:embed lua/locker_extend.lua
var lockerExtend string
var lockerExtendScript = valkey.NewLuaScript(lockerExtend)

// Extend extends our lock expiration by the passed in number of seconds provided the lock value is correct
func (l *Locker) Extend(ctx context.Context, vc valkey.Client, value string, expiration time.Duration) error {

	seconds := int(expiration / time.Second) // convert our expiration to seconds

	// we use lua here because we only want to set the expiration time if we own it
	return lockerExtendScript.Exec(ctx, vc, []string{l.key}, []string{value, fmt.Sprintf("%d", seconds)}).Error()
}

// IsLocked returns whether this lock is currently held by any process.
func (l *Locker) IsLocked(ctx context.Context, vc valkey.Client) (bool, error) {

	exists, err := vc.Do(ctx, vc.B().Exists().Key(l.key).Build()).AsBool()
	if err != nil {
		return false, err
	}

	return exists, nil
}
