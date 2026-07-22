package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/robinjoseph08/backlog/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(cli.Main(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
