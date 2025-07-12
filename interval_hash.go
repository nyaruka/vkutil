package vkutil

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/valkey-io/valkey-go"
)

// IntervalHash operates like a hash map but with expiring intervals
type IntervalHash struct {
	keyBase  string
	interval time.Duration // e.g. 5 minutes
	size     int           // number of intervals
}

// NewIntervalHash creates a new empty interval hash
func NewIntervalHash(keyBase string, interval time.Duration, size int) *IntervalHash {
	return &IntervalHash{keyBase: keyBase, interval: interval, size: size}
}

//go:embed lua/ihash_get.lua
var ihashGet string
var ihashGetScript = valkey.NewLuaScript(ihashGet)

// Get returns the value of the given field
func (h *IntervalHash) Get(ctx context.Context, vc valkey.Client, field string) (string, error) {
	keys := h.keys()
	result, err := ihashGetScript.Exec(ctx, vc, keys, []string{field}).ToString()
	if err != nil && !valkey.IsValkeyNil(err) {
		return "", err
	}
	return result, nil
}

//go:embed lua/ihash_mget.lua
var ihashMGet string
var ihashMGetScript = valkey.NewLuaScript(ihashMGet)

// MGet returns the values of the given fields
func (h *IntervalHash) MGet(ctx context.Context, vc valkey.Client, fields ...string) ([]string, error) {
	keys := h.keys()

	// for consistency with HMGET, zero fields is an error
	if len(fields) == 0 {
		return nil, errors.New("wrong number of arguments for command")
	}

	return ihashMGetScript.Exec(ctx, vc, keys, fields).AsStrSlice()
}

// Set sets the value of the given field
func (h *IntervalHash) Set(ctx context.Context, vc valkey.Client, field, value string) error {
	key := h.keys()[0]

	vc.Do(ctx, vc.B().Multi().Build())
	vc.Do(ctx, vc.B().Hset().Key(key).FieldValue().FieldValue(field, value).Build())
	vc.Do(ctx, vc.B().Expire().Key(key).Seconds(int64(h.size)*int64(h.interval/time.Second)).Build())
	return vc.Do(ctx, vc.B().Exec().Build()).Error()
}

// Del removes the given fields
func (h *IntervalHash) Del(ctx context.Context, vc valkey.Client, fields ...string) error {
	vc.Do(ctx, vc.B().Multi().Build())

	for _, k := range h.keys() {
		vc.Do(ctx, vc.B().Hdel().Key(k).Field(fields...).Build())
	}
	return vc.Do(ctx, vc.B().Exec().Build()).Error()

}

// Clear removes all fields
func (h *IntervalHash) Clear(ctx context.Context, vc valkey.Client) error {
	vc.Do(ctx, vc.B().Multi().Build())

	for _, k := range h.keys() {
		vc.Do(ctx, vc.B().Del().Key(k).Build())
	}
	return vc.Do(ctx, vc.B().Exec().Build()).Error()

}

func (h *IntervalHash) keys() []string {
	return intervalKeys(h.keyBase, h.interval, h.size)
}
