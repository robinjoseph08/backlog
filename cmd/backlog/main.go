package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/robinjoseph08/backlog/internal/cli"
)

func main() {
	ctx := context.Background()
	shutdownSignals := make(chan os.Signal, 16)
	signal.Notify(shutdownSignals, os.Interrupt, syscall.SIGTERM)
	exitCode := cli.MainWithTerminal(ctx, os.Args[1:], cli.TerminalDependencies{
		Input: os.Stdin, Output: os.Stdout, ErrorOutput: os.Stderr, Signals: shutdownSignals,
	})
	signal.Stop(shutdownSignals)
	os.Exit(exitCode)
}
