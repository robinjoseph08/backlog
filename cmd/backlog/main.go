package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/robinjoseph08/backlog/internal/cli"
)

func main() {
	ctx, stopTermination := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	interrupts := make(chan os.Signal, 16)
	signal.Notify(interrupts, os.Interrupt)
	exitCode := cli.MainWithSignals(ctx, os.Args[1:], os.Stdout, os.Stderr, interrupts)
	signal.Stop(interrupts)
	stopTermination()
	os.Exit(exitCode)
}
