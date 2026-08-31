package vkutil_test

import (
	"errors"
	"testing"

	"github.com/nyaruka/vkutil"
	"github.com/stretchr/testify/assert"
)

func TestStringsWithScores(t *testing.T) {
	strs, scores, err := vkutil.StringsWithScores([]any{[]byte("A"), []byte("1.5"), []byte("B"), []byte("2")}, nil)
	assert.NoError(t, err)
	assert.Equal(t, []string{"A", "B"}, strs)
	assert.Equal(t, []float64{1.5, 2}, scores)

	strs, scores, err = vkutil.StringsWithScores([]any{}, nil)
	assert.NoError(t, err)
	assert.Empty(t, strs)
	assert.Empty(t, scores)

	// an incoming error is passed through untouched
	_, _, err = vkutil.StringsWithScores(nil, errors.New("boom"))
	assert.EqualError(t, err, "boom")

	// an unexpected reply is an error rather than a panic
	_, _, err = vkutil.StringsWithScores([]any{int64(5), []byte("1")}, nil)
	assert.EqualError(t, err, "expected string at index 0 in reply, got int64")

	_, _, err = vkutil.StringsWithScores([]any{[]byte("A"), int64(5)}, nil)
	assert.EqualError(t, err, "expected string at index 1 in reply, got int64")

	_, _, err = vkutil.StringsWithScores([]any{[]byte("A"), nil}, nil)
	assert.EqualError(t, err, "expected string at index 1 in reply, got <nil>")

	// as is a score which isn't a number, or an odd number of values
	_, _, err = vkutil.StringsWithScores([]any{[]byte("A"), []byte("xx")}, nil)
	assert.ErrorContains(t, err, "invalid score at index 1 in reply")

	_, _, err = vkutil.StringsWithScores([]any{[]byte("A")}, nil)
	assert.EqualError(t, err, "expected an even number of values in reply, got 1")
}
