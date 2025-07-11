package assertvk

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valkey-io/valkey-go"
)

// Keys asserts that only the given keys exist
func Keys(t *testing.T, vc valkey.Client, pattern string, expected []string, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Keys().Pattern(pattern).Build()).ToAny()
	assert.NoError(t, err)

	return assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// Exists asserts that the given key exists
func Exists(t *testing.T, vc valkey.Client, key string, msgAndArgs ...any) bool {
	exists, err := vc.Do(context.Background(), vc.B().Exists().Key(key).Build()).AsBool()
	assert.NoError(t, err)

	if !exists {
		assert.Fail(t, "Key should exist", msgAndArgs...)
	}

	return exists
}

// NotExists asserts that the given key does not exist
func NotExists(t *testing.T, vc valkey.Client, key string, msgAndArgs ...any) bool {
	exists, err := vc.Do(context.Background(), vc.B().Exists().Key(key).Build()).AsBool()
	assert.NoError(t, err)

	if exists {
		assert.Fail(t, "Key should not exist", msgAndArgs...)
	}

	return !exists
}

// Get asserts that the given key contains the given string value
func Get(t *testing.T, vc valkey.Client, key string, expected string, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Get().Key(key).Build()).ToString()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// SCard asserts the result of calling SCARD on the given key
func SCard(t *testing.T, vc valkey.Client, key string, expected int, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Scard().Key(key).Build()).ToInt64()
	assert.NoError(t, err)

	return assert.Equal(t, int64(expected), actual, msgAndArgs...)
}

// SIsMember asserts the result that calling SISMEMBER on the given key is true
func SIsMember(t *testing.T, vc valkey.Client, key, member string, msgAndArgs ...any) bool {
	exists, err := vc.Do(context.Background(), vc.B().Sismember().Key(key).Member(member).Build()).AsBool()
	assert.NoError(t, err)

	if !exists {
		assert.Fail(t, "Key should be member", msgAndArgs...)
	}

	return exists
}

// SIsNotMember asserts the result of calling SISMEMBER on the given key is false
func SIsNotMember(t *testing.T, vc valkey.Client, key, member string, msgAndArgs ...any) bool {
	exists, err := vc.Do(context.Background(), vc.B().Sismember().Key(key).Member(member).Build()).AsBool()
	assert.NoError(t, err)

	if exists {
		assert.Fail(t, "Key should not be member", msgAndArgs...)
	}

	return !exists
}

// SMembers asserts the result of calling SMEMBERS on the given key
func SMembers(t *testing.T, vc valkey.Client, key string, expected []string, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Smembers().Key(key).Build()).AsStrSlice()
	assert.NoError(t, err)

	return assert.ElementsMatch(t, expected, actual, msgAndArgs...)
}

// HGetAll asserts the result of calling HGET on the given key and field
func HGet(t *testing.T, vc valkey.Client, key, field string, expected string, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Hget().Key(key).Field(field).Build()).ToString()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// HGetAll asserts the result of calling HGETALL on the given key
func HGetAll(t *testing.T, vc valkey.Client, key string, expected map[string]string, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Hgetall().Key(key).Build()).AsStrMap()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// HLen asserts the result of calling HLEN on the given key
func HLen(t *testing.T, vc valkey.Client, key string, expected int, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Hlen().Key(key).Build()).ToInt64()
	assert.NoError(t, err)

	return assert.Equal(t, int64(expected), actual, msgAndArgs...)
}

// LLen asserts the result of calling LLEN on the given key
func LLen(t *testing.T, vc valkey.Client, key string, expected int, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Llen().Key(key).Build()).ToInt64()
	assert.NoError(t, err)

	return assert.Equal(t, int64(expected), actual, msgAndArgs...)
}

// LRange asserts the result of calling LRANGE on the given key
func LRange(t *testing.T, vc valkey.Client, key string, start, stop int, expected []string, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Lrange().Key(key).Start(int64(start)).Stop(int64(stop)).Build()).AsStrSlice()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// LGetAll asserts the result of calling LRANGE <?> 0 -1 on the given key
func LGetAll(t *testing.T, vc valkey.Client, key string, expected []string, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Lrange().Key(key).Start(0).Stop(-1).Build()).AsStrSlice()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZCard asserts the result of calling ZCARD on the given key
func ZCard(t *testing.T, vc valkey.Client, key string, expected int, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Zcard().Key(key).Build()).ToInt64()
	assert.NoError(t, err)

	return assert.Equal(t, int64(expected), actual, msgAndArgs...)
}

// ZRange asserts the result of calling ZRANGE on the given key
func ZRange(t *testing.T, vc valkey.Client, key string, start, stop int, expected []string, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Zrange().Key(key).Min(strconv.FormatInt(int64(start), 10)).Max(strconv.FormatInt(int64(stop), 10)).Build()).AsStrSlice()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZGetAll asserts the result of calling ZRANGE <?> 0 -1 WITHSCORES on the given key
func ZGetAll(t *testing.T, vc valkey.Client, key string, expected map[string]float64, msgAndArgs ...any) bool {
	actualStrings, err := vc.Do(context.Background(), vc.B().Zrange().Key(key).Min("0").Max("-1").Withscores().Build()).AsZScores()
	assert.NoError(t, err)

	actual := make(map[string]float64, len(actualStrings))
	for _, v := range actualStrings {
		actual[v.Member] = v.Score
		require.NoError(t, err)
	}

	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// ZRange asserts the result of calling ZSCORE on the given key
func ZScore(t *testing.T, vc valkey.Client, key, member string, expected float64, msgAndArgs ...any) bool {
	actual, err := vc.Do(context.Background(), vc.B().Zscore().Key(key).Member(member).Build()).ToFloat64()
	assert.NoError(t, err)

	return assert.Equal(t, expected, actual, msgAndArgs...)
}
