package vkutil

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	valkey "github.com/gomodule/redigo/redis"
)

// timeNow can be overridden in tests to use a fixed time source
var timeNow = time.Now

// StringsWithScores parses an array reply which is alternating pairs of strings and scores (floats)
func StringsWithScores(reply any, err error) ([]string, []float64, error) {
	pairs, err := valkey.Values(reply, err)
	if err != nil {
		return nil, nil, err
	}

	if len(pairs)%2 != 0 {
		return nil, nil, fmt.Errorf("expected an even number of values in reply, got %d", len(pairs))
	}

	strings := make([]string, len(pairs)/2)
	scores := make([]float64, len(pairs)/2)

	for i := range strings {
		rawString, ok := pairs[2*i].([]byte)
		if !ok {
			return nil, nil, fmt.Errorf("expected string at index %d in reply, got %T", 2*i, pairs[2*i])
		}
		rawScore, ok := pairs[2*i+1].([]byte)
		if !ok {
			return nil, nil, fmt.Errorf("expected string at index %d in reply, got %T", 2*i+1, pairs[2*i+1])
		}

		score, err := strconv.ParseFloat(string(rawScore), 64)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid score at index %d in reply: %w", 2*i+1, err)
		}

		strings[i] = string(rawString)
		scores[i] = score
	}

	return strings, scores, nil
}

// execTx executes the commands queued on the given connection as a transaction, returning the first
// error it finds. A command which fails during EXEC doesn't make EXEC itself fail - its error comes
// back as an element of EXEC's reply - so without checking those, a write which didn't happen is
// indistinguishable from one which did.
func execTx(ctx context.Context, vc valkey.Conn) error {
	replies, err := valkey.Values(valkey.DoContext(vc, ctx, "EXEC"))
	if err != nil {
		return err
	}

	for _, reply := range replies {
		if err, ok := reply.(valkey.Error); ok {
			return err
		}
	}

	return nil
}

func intervalTimestamp(t time.Time, interval time.Duration) string {
	t = t.UTC().Truncate(interval)

	if interval < time.Minute {
		return t.Format("2006-01-02T15:04:05")
	}
	if interval < time.Hour*24 {
		return t.Format("2006-01-02T15:04")
	}
	return t.Format("2006-01-02")
}

func intervalKeys(keyBase string, interval time.Duration, size int) []string {
	now := timeNow()
	keys := make([]string, size)
	for i := range keys {
		timestamp := intervalTimestamp(now.Add(-interval*time.Duration(i)), interval)
		keys[i] = fmt.Sprintf("{%s}:%s", keyBase, timestamp)
	}
	return keys
}

const base64Charset = `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/`

// RandomBase64 creates a random string of the length passed in
func RandomBase64(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = base64Charset[rand.IntN(64)]
	}
	return string(b)
}

// intervalExpire returns how long an interval key should live for, in milliseconds. Expirations are
// expressed in milliseconds rather than seconds because a sub-second duration truncated to zero
// seconds would cause the key to be deleted rather than expired.
func intervalExpire(interval time.Duration, size int) int64 {
	return (interval * time.Duration(size)).Milliseconds()
}

// checkIntervalParams panics if the given interval parameters can't be honoured. A non-positive
// interval collapses every bucket onto the same key, and size is used as a slice length.
func checkIntervalParams(interval time.Duration, size int) {
	if interval <= 0 {
		panic("interval must be greater than zero")
	}
	if size <= 0 {
		panic("size must be greater than zero")
	}
}
