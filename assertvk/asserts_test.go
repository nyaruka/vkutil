package assertvk_test

import (
	"context"
	"testing"

	"github.com/nyaruka/vkutil/assertvk"
	"github.com/stretchr/testify/assert"
)

func TestAsserts(t *testing.T) {
	ctx := context.Background()
	vc := assertvk.TestValkeyClient()
	defer vc.Close()

	defer assertvk.FlushDB()

	err := vc.Do(ctx, vc.B().Set().Key("mykey").Value("one").Build()).Error()
	assert.NoError(t, err)

	assert.True(t, assertvk.Exists(t, vc, "mykey"))
	assert.True(t, assertvk.NotExists(t, vc, "mykey2"))
	assert.True(t, assertvk.Get(t, vc, "mykey", "one"))

	err = vc.Do(ctx, vc.B().Rpush().Key("mylist").Element("one").Build()).Error()
	assert.NoError(t, err)
	err = vc.Do(ctx, vc.B().Rpush().Key("mylist").Element("two").Build()).Error()
	assert.NoError(t, err)
	err = vc.Do(ctx, vc.B().Rpush().Key("mylist").Element("three").Build()).Error()
	assert.NoError(t, err)

	assert.True(t, assertvk.LLen(t, vc, "mylist", 3))
	assert.True(t, assertvk.LRange(t, vc, "mylist", 0, 1, []string{"one", "two"}))
	assert.True(t, assertvk.LGetAll(t, vc, "mylist", []string{"one", "two", "three"}))

	err = vc.Do(ctx, vc.B().Sadd().Key("myset").Member("one").Build()).Error()
	assert.NoError(t, err)
	err = vc.Do(ctx, vc.B().Sadd().Key("myset").Member("two").Build()).Error()
	assert.NoError(t, err)
	err = vc.Do(ctx, vc.B().Sadd().Key("myset").Member("three").Build()).Error()
	assert.NoError(t, err)

	assert.True(t, assertvk.SCard(t, vc, "myset", 3))
	assert.True(t, assertvk.SIsMember(t, vc, "myset", "two"))
	assert.True(t, assertvk.SIsNotMember(t, vc, "myset", "four"))
	assert.True(t, assertvk.SMembers(t, vc, "myset", []string{"two", "one", "three"}))

	err = vc.Do(ctx, vc.B().Hset().Key("myhash").FieldValue().FieldValue("a", "one").Build()).Error()
	assert.NoError(t, err)
	err = vc.Do(ctx, vc.B().Hset().Key("myhash").FieldValue().FieldValue("b", "two").Build()).Error()
	assert.NoError(t, err)
	err = vc.Do(ctx, vc.B().Hset().Key("myhash").FieldValue().FieldValue("c", "three").Build()).Error()
	assert.NoError(t, err)

	assert.True(t, assertvk.HLen(t, vc, "myhash", 3))
	assert.True(t, assertvk.HGet(t, vc, "myhash", "b", "two"))
	assert.True(t, assertvk.HGetAll(t, vc, "myhash", map[string]string{"a": "one", "b": "two", "c": "three"}))

	err = vc.Do(ctx, vc.B().Zadd().Key("myzset").ScoreMember().ScoreMember(1, "one").Build()).Error()
	assert.NoError(t, err)
	err = vc.Do(ctx, vc.B().Zadd().Key("myzset").ScoreMember().ScoreMember(2, "two").Build()).Error()
	assert.NoError(t, err)
	err = vc.Do(ctx, vc.B().Zadd().Key("myzset").ScoreMember().ScoreMember(3, "three").Build()).Error()
	assert.NoError(t, err)

	assert.True(t, assertvk.ZCard(t, vc, "myzset", 3))
	assert.True(t, assertvk.ZScore(t, vc, "myzset", "one", 1))
	assert.True(t, assertvk.ZScore(t, vc, "myzset", "two", 2))
	assert.True(t, assertvk.ZScore(t, vc, "myzset", "three", 3))
	assert.True(t, assertvk.ZRange(t, vc, "myzset", 0, 1, []string{"one", "two"}))
	assert.True(t, assertvk.ZGetAll(t, vc, "myzset", map[string]float64{"one": 1, "two": 2, "three": 3}))
}
