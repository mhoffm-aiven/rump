// Package redis allows reading/writing from/to a Redis DB.
package redis

import (
	"context"
	"fmt"

	"github.com/mediocregopher/radix/v3"

	"github.com/stickermule/rump/pkg/message"
)

// Redis holds references to a DB pool and a shared message bus.
// Silent disables verbose mode.
// TTL enables TTL sync.
type Redis struct {
	PoolSource *radix.Pool
	PoolTarget *radix.Pool
	Bus        message.Bus
	Silent     bool
	TTL        bool
}

// New creates the Redis struct, used to read/write.
func New(source, target *radix.Pool, bus message.Bus, silent, ttl bool) *Redis {
	return &Redis{
		PoolSource: source,
		PoolTarget: target,
		Bus:        bus,
		Silent:     silent,
		TTL:        ttl,
	}
}

// maybeLog may log, depending on the Silent flag
func (r *Redis) maybeLog(s string) {
	if r.Silent {
		return
	}
	fmt.Print(s)
}

// maybeTTL may sync the TTL, depending on the TTL flag
func (r *Redis) maybeTTL(key string) (string, error) {
	// noop if TTL is disabled, speeds up sync process
	if !r.TTL {
		return "0", nil
	}

	var ttl string

	// Try getting key TTL.
	err := r.PoolSource.Do(radix.Cmd(&ttl, "PTTL", key))
	if err != nil {
		return ttl, err
	}

	// When key has no expire PTTL returns "-1".
	// We set it to 0, default for no expiration time.
	if ttl == "-1" {
		ttl = "0"
	}

	return ttl, nil
}

// Read gently scans an entire Redis DB for keys, then dumps
// the key/value pair (Payload) on the message Bus channel.
// It leverages implicit pipelining to speedup large DB reads.
// To be used in an ErrGroup.
func (r *Redis) Read(ctx context.Context) error {
	defer close(r.Bus)

	scanner := radix.NewScanner(r.PoolSource, radix.ScanAllKeys)

	var key string
	var value string

	// Scan and push to bus until no keys are left.
	// If context Done, exit early.
	for scanner.Next(&key) {
		err := r.PoolSource.Do(radix.Cmd(&value, "DUMP", key))
		if err != nil {
			return err
		}

		// TTL is handled when writing so that we do not starve writers
		select {
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("redis read: exit")
			return ctx.Err()
		case r.Bus <- message.Payload{Key: key, Value: value, TTL: "0"}:
			r.maybeLog("r")
		}
	}

	return scanner.Close()
}

// Write restores keys on the db as they come on the message bus.
func (r *Redis) Write(ctx context.Context) error {
	// Loop until channel is open
	for r.Bus != nil {
		select {
		// Exit early if context done.
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("redis write: exit")
			return ctx.Err()
		// Get Messages from Bus
		case p, ok := <-r.Bus:
			// if channel closed, set to nil, break loop
			if !ok {
				r.Bus = nil
				continue
			}
			ttl, err := r.maybeTTL(p.Key)
			if err != nil {
				return err
			}
			if err = r.PoolTarget.Do(radix.Cmd(nil, "RESTORE", p.Key, ttl, p.Value, "REPLACE")); err != nil {
				return err
			}
			r.maybeLog("w")
		}
	}

	return nil
}
