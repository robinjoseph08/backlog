// Package processidentity owns persisted process identity and liveness rules.
package processidentity

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// Alive reports whether a PID or process group exists. Permission failures are
// reported as unknown rather than absence.
func Alive(pid int) (bool, error) {
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

// Start returns the persisted identity for a live process.
func Start(pid int) (string, error) {
	command := exec.Command("ps", "-p", fmt.Sprint(pid), "-o", "lstart=") // #nosec G204 -- callers supply a validated numeric PID
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

// PID extracts the recorded PID from a persisted process identity.
func PID(identity string) (int, error) {
	value, started, found := strings.Cut(identity, ":")
	pid, err := strconv.Atoi(value)
	if !found || err != nil || pid <= 0 || strings.TrimSpace(started) == "" {
		return 0, errors.New("invalid process identity")
	}
	return pid, nil
}
