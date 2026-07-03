package vkutil

import "time"

// SetNow overrides the time source used by interval based types in tests
func SetNow(fn func() time.Time) {
	timeNow = fn
}
