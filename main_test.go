package vkutil_test

import (
	"os"
	"testing"

	"github.com/nyaruka/vkutil/assertvk"
)

func TestMain(m *testing.M) {
	code := m.Run()
	assertvk.Release()
	os.Exit(code)
}
