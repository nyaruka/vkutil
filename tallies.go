package vkutil

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"

	valkey "github.com/gomodule/redigo/redis"
)

// Tallies buffers totals of things which only go up and flushes them to a store in batches, so that frequent
// increments cost one store write per field per flush rather than a write each. What a field identifies is up to the
// caller, e.g. 123|msgs/in@2025-05-04 for the messages received by owner 123 on a day.
//
// Keys, for a key base of foo:
//   - {foo}:live - hash of field to amount, incremented by writers
//   - {foo}:flushing - snapshot of live being flushed
//   - {foo}:batch - fields moved out of flushing which are being written to the store
//   - {foo}:batch_id - id of the most recently created batch
//
// Everything which increments or flushes the same tallies must use the same key base.
//
// Flushing needs Valkey or Redis 6.2 or later.
//
// A flush renames live to flushing and then writes it to the store a batch at a time. Batch ids only go up, and the
// store records each one in the same transaction as its amounts, so a batch left behind by a failed flush is written
// again only if the store doesn't have it yet. Concurrent flushes of the same tallies can't double count or lose
// amounts, but only one makes progress at a time, so they should be prevented with a lock.
//
// A reader gets consistent totals of some fields by:
//  1. Reading them from live, flushing and batch, whether batch exists, and batch_id, in one transaction
//  2. Reading their totals and the last batch from the store, from the same snapshot
//  3. Including the amounts read from batch only if it exists and the last batch is less than batch_id (0 if missing)
//  4. Reading again if the last batch is greater than batch_id, as a batch was written between the two reads
type Tallies struct {
	keyBase string
}

// TallyStore is where flushed tallies are written, a batch at a time.
type TallyStore interface {
	// LastBatch returns the id of the last batch written, or zero if there is none.
	LastBatch(ctx context.Context) (int64, error)

	// Write must add the given amounts by field to their totals and record id as the last batch in one transaction,
	// returning whether it did. It must do nothing if the last batch is already greater than or equal to id, checking
	// that atomically with recording id (e.g. with a conditional update of the last batch) as concurrent flushes can
	// write the same batch at the same time.
	Write(ctx context.Context, id int64, amounts map[string]int64) (bool, error)
}

// NewTallies creates new tallies with the given key base. Panics if the key base is empty or contains { or }, as then
// it can't be used as the hash tag which keeps all its keys in the same slot.
func NewTallies(keyBase string) *Tallies {
	if keyBase == "" || strings.ContainsAny(keyBase, "{}") {
		panic(fmt.Sprintf("invalid tallies key base %q", keyBase))
	}
	return &Tallies{keyBase: keyBase}
}

// KeyBase returns the key base, which a store can use to keep the last batch of each set of tallies separately.
func (t *Tallies) KeyBase() string { return t.keyBase }

func (t *Tallies) liveKey() string     { return fmt.Sprintf("{%s}:live", t.keyBase) }
func (t *Tallies) flushingKey() string { return fmt.Sprintf("{%s}:flushing", t.keyBase) }
func (t *Tallies) batchKey() string    { return fmt.Sprintf("{%s}:batch", t.keyBase) }
func (t *Tallies) batchIDKey() string  { return fmt.Sprintf("{%s}:batch_id", t.keyBase) }

// Increment adds the given amounts by field.
func (t *Tallies) Increment(ctx context.Context, vc valkey.Conn, amounts map[string]int64) error {
	if len(amounts) == 0 {
		return nil
	}

	vc.Send("MULTI")
	for field, amount := range amounts {
		vc.Send("HINCRBY", t.liveKey(), field, amount)
	}

	return execTx(ctx, vc)
}

//go:embed lua/tally_snapshot.lua
var tallySnapshot string
var tallySnapshotScript = valkey.NewScript(2, tallySnapshot)

//go:embed lua/tally_batch.lua
var tallyBatch string
var tallyBatchScript = valkey.NewScript(3, tallyBatch)

// Flush writes everything buffered when it starts to the given store in batches of up to batchSize fields, along with
// anything left by a failed flush, returning the number of fields written. Increments made during a flush are left for
// the next one.
func (t *Tallies) Flush(ctx context.Context, vc valkey.Conn, store TallyStore, batchSize int) (int, error) {
	if batchSize <= 0 {
		panic("flush batch size must be greater than zero")
	}

	lastBatch, err := store.LastBatch(ctx)
	if err != nil {
		return 0, fmt.Errorf("error getting last batch: %w", err)
	}

	flushed, err := t.finishBatch(ctx, vc, store, &lastBatch)
	if err != nil {
		return flushed, err
	}

	// drain any snapshot left by a failed flush before taking a new one
	n, err := t.drain(ctx, vc, store, batchSize, &lastBatch)
	flushed += n
	if err != nil {
		return flushed, err
	}

	if _, err := tallySnapshotScript.DoContext(ctx, vc, t.liveKey(), t.flushingKey()); err != nil {
		return flushed, fmt.Errorf("error taking snapshot: %w", err)
	}

	n, err = t.drain(ctx, vc, store, batchSize, &lastBatch)
	flushed += n
	return flushed, err
}

// finishBatch writes a batch left by a failed flush, unless the store already has it, and deletes it
func (t *Tallies) finishBatch(ctx context.Context, vc valkey.Conn, store TallyStore, lastBatch *int64) (int, error) {
	vc.Send("MULTI")
	vc.Send("HGETALL", t.batchKey())
	vc.Send("GET", t.batchIDKey())
	replies, err := valkey.Values(valkey.DoContext(vc, ctx, "EXEC"))
	if err != nil {
		return 0, fmt.Errorf("error reading leftover batch: %w", err)
	}

	kvs, err := valkey.Strings(replies[0], nil)
	if err != nil {
		return 0, fmt.Errorf("error reading leftover batch: %w", err)
	}
	if len(kvs) == 0 {
		return 0, nil
	}
	id, err := valkey.Int64(replies[1], nil)
	if err != nil {
		return 0, fmt.Errorf("error reading id of leftover batch from %s: %w", t.batchIDKey(), err)
	}

	written := 0
	if id > *lastBatch {
		if written, err = t.writeBatch(ctx, store, id, kvs); err != nil {
			return 0, err
		}
		*lastBatch = id
	}

	return written, t.deleteBatch(ctx, vc, id)
}

// drain writes the flushing snapshot to the store a batch at a time until it's empty
func (t *Tallies) drain(
	ctx context.Context, vc valkey.Conn, store TallyStore, batchSize int, lastBatch *int64,
) (int, error) {
	flushed := 0
	for {
		reply, err := valkey.Values(tallyBatchScript.DoContext(
			ctx, vc, t.flushingKey(), t.batchKey(), t.batchIDKey(), *lastBatch, batchSize,
		))
		if err != nil {
			return flushed, fmt.Errorf("error creating batch: %w", err)
		}
		id, _ := valkey.Int64(reply[0], nil)
		kvs, err := valkey.Strings(reply[1], nil)
		if err != nil {
			return flushed, fmt.Errorf("error reading batch %d: %w", id, err)
		}
		if len(kvs) == 0 {
			return flushed, nil
		}

		n, err := t.writeBatch(ctx, store, id, kvs)
		if err != nil {
			return flushed, err
		}
		flushed += n
		*lastBatch = id

		if err := t.deleteBatch(ctx, vc, id); err != nil {
			return flushed, err
		}
	}
}

func (t *Tallies) writeBatch(ctx context.Context, store TallyStore, id int64, kvs []string) (int, error) {
	amounts := make(map[string]int64, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		count, err := strconv.ParseInt(kvs[i+1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid amount %q for %q in batch %d", kvs[i+1], kvs[i], id)
		}
		amounts[kvs[i]] = count
	}

	written, err := store.Write(ctx, id, amounts)
	if err != nil {
		return 0, fmt.Errorf("error writing batch %d: %w", id, err)
	}
	if !written {
		return 0, nil
	}
	return len(amounts), nil
}

//go:embed lua/tally_done.lua
var tallyDone string
var tallyDoneScript = valkey.NewScript(2, tallyDone)

func (t *Tallies) deleteBatch(ctx context.Context, vc valkey.Conn, id int64) error {
	if _, err := tallyDoneScript.DoContext(ctx, vc, t.batchKey(), t.batchIDKey(), id); err != nil {
		return fmt.Errorf("error deleting batch %d: %w", id, err)
	}
	return nil
}
