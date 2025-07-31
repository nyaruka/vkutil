package queues

import (
	"bytes"
	"fmt"

	"github.com/nyaruka/gocommon/uuids"
)

type TaskID uuids.UUID
type OwnerID string

func parsePayload(raw []byte) (TaskID, []byte, error) {
	if len(raw) == 0 {
		return "", nil, fmt.Errorf("empty task payload")
	}

	// TODO workaround for tasks without UUIDs
	if raw[0] == '{' {
		return "", raw, nil
	}

	parts := bytes.SplitN(raw, []byte{'|'}, 2)
	if len(parts) != 2 || !uuids.Is(string(parts[0])) {
		return "", nil, fmt.Errorf("invalid task payload: %s", raw)
	}

	id := TaskID(parts[0])
	task := parts[1]

	return id, task, nil
}
