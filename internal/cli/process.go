package cli

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
)

func signalZero(pid int) (bool, error) {
	err := syscall.Kill(pid, syscall.Signal(0))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	case errors.Is(err, syscall.EPERM):
		return false, errors.New("permission denied; liveness is unknown")
	default:
		return false, err
	}
}

func pidStartIdentity(pid int) (string, error) {
	command := exec.Command("ps", "-p", fmt.Sprint(pid), "-o", "lstart=") // #nosec G204 -- validated numeric PID
	output, err := command.CombinedOutput()
	if err != nil {
		return "", err
	}
	started := strings.TrimSpace(string(output))
	if started == "" {
		return "", errors.New("empty process start identity")
	}
	return fmt.Sprintf("%d:%s", pid, started), nil
}
