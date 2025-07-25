package vkutil

import (
	"context"
	_ "embed"
	"errors"
	"time"

	valkey "github.com/gomodule/redigo/redis"
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
var ihashGetScript = valkey.NewScript(-1, ihashGet)

// Get returns the value of the given field
func (h *IntervalHash) Get(ctx context.Context, vc valkey.Conn, field string) (string, error) {
	keys := h.keys()

	value, err := valkey.String(ihashGetScript.DoContext(ctx, vc, valkey.Args{}.Add(len(keys)).AddFlat(keys).Add(field)...))
	if err != nil && err != valkey.ErrNil {
		return "", err
	}
	return value, nil
}

//go:embed lua/ihash_mget.lua
var ihashMGet string
var ihashMGetScript = valkey.NewScript(-1, ihashMGet)

// MGet returns the values of the given fields
func (h *IntervalHash) MGet(ctx context.Context, vc valkey.Conn, fields ...string) ([]string, error) {
	keys := h.keys()

	// for consistency with HMGET, zero fields is an error
	if len(fields) == 0 {
		return nil, errors.New("wrong number of arguments for command")
	}

	value, err := valkey.Strings(ihashMGetScript.DoContext(ctx, vc, valkey.Args{}.Add(len(keys)).AddFlat(keys).AddFlat(fields)...))
	if err != nil && err != valkey.ErrNil {
		return nil, err
	}
	return value, nil
}

// Set sets the value of the given field
func (h *IntervalHash) Set(ctx context.Context, vc valkey.Conn, field, value string) error {
	key := h.keys()[0]

	vc.Send("MULTI")
	vc.Send("HSET", key, field, value)
	vc.Send("EXPIRE", key, h.size*int(h.interval/time.Second))
	_, err := valkey.DoContext(vc, ctx, "EXEC")
	return err
}

// Del removes the given fields
func (h *IntervalHash) Del(ctx context.Context, vc valkey.Conn, fields ...string) error {
	vc.Send("MULTI")
	for _, k := range h.keys() {
		vc.Send("HDEL", valkey.Args{}.Add(k).AddFlat(fields)...)
	}
	_, err := valkey.DoContext(vc, ctx, "EXEC")
	return err
}

// Clear removes all fields
func (h *IntervalHash) Clear(ctx context.Context, vc valkey.Conn) error {
	vc.Send("MULTI")
	for _, k := range h.keys() {
		vc.Send("DEL", k)
	}
	_, err := valkey.DoContext(vc, ctx, "EXEC")
	return err
}

func (h *IntervalHash) keys() []string {
	return intervalKeys(h.keyBase, h.interval, h.size)
}
