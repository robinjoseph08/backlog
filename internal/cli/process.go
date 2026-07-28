package cli

import "github.com/robinjoseph08/backlog/internal/processidentity"

func signalZero(pid int) (bool, error) { return processidentity.Alive(pid) }

func pidStartIdentity(pid int) (string, error) { return processidentity.Start(pid) }
