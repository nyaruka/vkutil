package assertvk

import (
	"context"
	"fmt"
	"os"

	"github.com/gomodule/redigo/redis"
)

const (
	// maybe don't run these tests where you store your production database
	testDBIndex = 0
)

// TestDB returns a redis pool to our test database
func TestDB() *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", getHostAddress())
			if err != nil {
				return nil, err
			}
			_, err = redis.DoContext(conn, context.Background(), "SELECT", 0)
			return conn, err
		},
	}
}

// FlushDB flushes the test database
func FlushDB() {
	rp, err := redis.Dial("tcp", getHostAddress())
	if err != nil {
		panic(fmt.Sprintf("error connecting to redis db: %s", err.Error()))
	}

	redis.DoContext(rp, context.Background(), "SELECT", testDBIndex)

	_, err = redis.DoContext(rp, context.Background(), "FLUSHDB")
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
