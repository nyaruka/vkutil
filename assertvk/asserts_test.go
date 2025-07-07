package assertvk_test

import (
	"context"
	"testing"

	valkey "github.com/gomodule/redigo/redis"
	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
)

func TestAsserts(t *testing.T) {
	ctx := context.Background()
	vp := assertvk.TestDB()
	vc := vp.Get()
	defer vc.Close()

	defer assertvk.FlushDB()

	valkey.DoContext(vc, ctx, "SET", "mykey", "one")

	assert.True(t, assertvk.Exists(t, vc, "mykey"))
	assert.True(t, assertvk.NotExists(t, vc, "mykey2"))
	assert.True(t, assertvk.Get(t, vc, "mykey", "one"))

	valkey.DoContext(vc, ctx, "RPUSH", "mylist", "one")
	valkey.DoContext(vc, ctx, "RPUSH", "mylist", "two")
	valkey.DoContext(vc, ctx, "RPUSH", "mylist", "three")

	assert.True(t, assertvk.LLen(t, vc, "mylist", 3))
	assert.True(t, assertvk.LRange(t, vc, "mylist", 0, 1, []string{"one", "two"}))
	assert.True(t, assertvk.LGetAll(t, vc, "mylist", []string{"one", "two", "three"}))

	valkey.DoContext(vc, ctx, "SADD", "myset", "one")
	valkey.DoContext(vc, ctx, "SADD", "myset", "two")
	valkey.DoContext(vc, ctx, "SADD", "myset", "three")

	assert.True(t, assertvk.SCard(t, vc, "myset", 3))
	assert.True(t, assertvk.SIsMember(t, vc, "myset", "two"))
	assert.True(t, assertvk.SIsNotMember(t, vc, "myset", "four"))
	assert.True(t, assertvk.SMembers(t, vc, "myset", []string{"two", "one", "three"}))

	valkey.DoContext(vc, ctx, "HSET", "myhash", "a", "one")
	valkey.DoContext(vc, ctx, "HSET", "myhash", "b", "two")
	valkey.DoContext(vc, ctx, "HSET", "myhash", "c", "three")

	assert.True(t, assertvk.HLen(t, vc, "myhash", 3))
	assert.True(t, assertvk.HGet(t, vc, "myhash", "b", "two"))
	assert.True(t, assertvk.HGetAll(t, vc, "myhash", map[string]string{"a": "one", "b": "two", "c": "three"}))

	valkey.DoContext(vc, ctx, "ZADD", "myzset", 1, "one")
	valkey.DoContext(vc, ctx, "ZADD", "myzset", 2, "two")
	valkey.DoContext(vc, ctx, "ZADD", "myzset", 3, "three")

	assert.True(t, assertvk.ZCard(t, vc, "myzset", 3))
	assert.True(t, assertvk.ZScore(t, vc, "myzset", "one", 1))
	assert.True(t, assertvk.ZScore(t, vc, "myzset", "two", 2))
	assert.True(t, assertvk.ZScore(t, vc, "myzset", "three", 3))
	assert.True(t, assertvk.ZRange(t, vc, "myzset", 0, 1, []string{"one", "two"}))
	assert.True(t, assertvk.ZGetAll(t, vc, "myzset", map[string]float64{"one": 1, "two": 2, "three": 3}))
}
