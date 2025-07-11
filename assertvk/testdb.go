package assertvk

import (
	"context"
	"fmt"
	"os"

	"github.com/valkey-io/valkey-go"
)

const (
	// maybe don't run these tests where you store your production database
	testDBIndex = 0
)

// TestValkeyClient returns a valkey client to our test database
func TestValkeyClient() valkey.Client {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{getHostAddress()},
		SelectDB:    testDBIndex,
	})
	if err != nil {
		panic(fmt.Sprintf("error creating valkey client: %s", err.Error()))
	}

	return client
}

// FlushDB flushes the test database
func FlushDB() {
	client := TestValkeyClient()
	defer client.Close()

	result := client.Do(context.Background(), client.B().Flushdb().Build())

	if result.Error() != nil {
		panic(fmt.Sprintf("error flushing valkey db: %s", result.Error()))
	}
}

func getHostAddress() string {
	host := os.Getenv("VALKEY_HOST")
	if host == "" {
		host = "localhost"
	}
	return host + ":6379"
}
