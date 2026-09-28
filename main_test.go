package vkutil_test

import (
	"os"
	"testing"

	"github.com/nyaruka/vkutil/assertvk"
)

func TestMain(m *testing.M) {
	assertvk.Coordinate(16, 17, 63) // CI's valkeys have 64 databases

	os.Exit(m.Run())
}
