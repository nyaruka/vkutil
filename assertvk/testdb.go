package assertvk

import (
	"context"
	"fmt"
	"os"

	valkey "github.com/gomodule/redigo/redis"
)

const (
	// maybe don't run these tests where you store your production database
	testDBIndex = 0
)

// TestDB returns a valkey pool to our test database
func TestDB() *valkey.Pool {
	return &valkey.Pool{
		Dial: func() (valkey.Conn, error) {
			conn, err := valkey.Dial("tcp", getHostAddress())
			if err != nil {
				return nil, err
			}
			_, err = valkey.DoContext(conn, context.Background(), "SELECT", 0)
			return conn, err
		},
	}
}

// FlushDB flushes the test database
func FlushDB() {
	rc, err := valkey.Dial("tcp", getHostAddress())
	if err != nil {
		panic(fmt.Sprintf("error connecting to valkey db: %s", err.Error()))
	}
	valkey.DoContext(rc, context.Background(), "SELECT", testDBIndex)
	_, err = valkey.DoContext(rc, context.Background(), "FLUSHDB")
	if err != nil {
		panic(fmt.Sprintf("error flushing valkey db: %s", err.Error()))
	}
}

func getHostAddress() string {
	host := os.Getenv("VALKEY_HOST")
	if host == "" {
		host = "localhost"
	}
	return host + ":6379"
}
