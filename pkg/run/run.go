// Package run manages Read, Write and Signal goroutines.
package run

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/mediocregopher/radix/v3"
	"golang.org/x/sync/errgroup"

	"github.com/stickermule/rump/pkg/config"
	"github.com/stickermule/rump/pkg/message"
	"github.com/stickermule/rump/pkg/redis"
	"github.com/stickermule/rump/pkg/signal"
)

// Exit helper
func exit(e error) {
	fmt.Println(e)
	os.Exit(1)
}

// Run orchestrate the Reader, Writer and Signal handler.
func Run(cfg config.Config) {
	// create ErrGroup to manage goroutines
	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)

	// Start signal handling goroutine
	g.Go(func() error {
		return signal.Run(gctx, cancel)
	})

	concurrency := runtime.NumCPU()

	// Create shared message bus
	ch := make(message.Bus, 100)

	dbSource, err := radix.NewPool("tcp", cfg.Source.URI, 1)
	if err != nil {
		exit(err)
	}
	dbTarget, err := radix.NewPool("tcp", cfg.Target.URI, concurrency)
	if err != nil {
		exit(err)
	}
	redis := redis.New(dbSource, dbTarget, ch, cfg.Silent, cfg.TTL)
	g.Go(func() error {
		return redis.Read(gctx)
	})

	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			defer cancel()
			return redis.Write(gctx)
		})
	}

	// Block and wait for goroutines
	err = g.Wait()
	if err != nil && err != context.Canceled {
		exit(err)
	} else {
		fmt.Println("done")
	}
}
