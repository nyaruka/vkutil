package queues

import (
	"bytes"
	"fmt"

	"github.com/nyaruka/gocommon/uuids"
)

// TaskID is the unique identifier for a task in the queue.
type TaskID string

// OwnerID is the identifier for an owner of tasks in the queue.
type OwnerID string

func newTaskID() TaskID {
	return TaskID(uuids.NewV7())
}

func parsePayload(raw []byte) (TaskID, []byte, error) {
	if len(raw) == 0 {
		return "", nil, fmt.Errorf("empty task payload")
	}

	parts := bytes.SplitN(raw, []byte{'|'}, 2)
	if len(parts) != 2 || !uuids.Is(string(parts[0])) {
		return "", nil, fmt.Errorf("invalid task payload: %s", raw)
	}

	id := TaskID(parts[0])
	task := parts[1]

	return id, task, nil
}
