package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const runnerSupervisionFile = "backlog.runner.json"

type runnerProcessIdentity struct {
	PID             int    `json:"pid"`
	ProcessIdentity string `json:"processIdentity"`
}

type runnerSupervisionMarker struct {
	path     string
	identity runnerProcessIdentity
}

func establishRunnerSupervision(commonDirectory string) (*runnerSupervisionMarker, error) {
	processIdentity, err := pidStartIdentity(os.Getpid())
	if err != nil {
		return nil, fmt.Errorf("inspect Runner process-start identity: %w", err)
	}
	identity := runnerProcessIdentity{PID: os.Getpid(), ProcessIdentity: processIdentity}
	path := filepath.Join(commonDirectory, runnerSupervisionFile)
	if err := writeRunnerProcessIdentity(path, identity); err != nil {
		return nil, err
	}
	return &runnerSupervisionMarker{path: path, identity: identity}, nil
}

func writeRunnerProcessIdentity(path string, identity runnerProcessIdentity) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".backlog-runner-*.json")
	if err != nil {
		return fmt.Errorf("create Runner supervision marker: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect Runner supervision marker: %w", err)
	}
	if err := json.NewEncoder(temporary).Encode(identity); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("encode Runner supervision marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync Runner supervision marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Runner supervision marker: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish Runner supervision marker: %w", err)
	}
	return nil
}

func (m *runnerSupervisionMarker) Release() error {
	identity, err := readRunnerProcessIdentity(m.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if identity != m.identity {
		return nil
	}
	if err := os.Remove(m.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove Runner supervision marker: %w", err)
	}
	return nil
}

func runnerSupervised(commonDirectory string) (bool, error) {
	identity, err := readRunnerProcessIdentity(filepath.Join(commonDirectory, runnerSupervisionFile))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if identity.PID <= 0 || identity.ProcessIdentity == "" {
		return false, errors.New("Runner supervision marker has incomplete process identity")
	}
	alive, err := signalZero(identity.PID)
	if err != nil {
		return false, fmt.Errorf("verify Runner PID %d: %w", identity.PID, err)
	}
	if !alive {
		return false, nil
	}
	observedIdentity, err := pidStartIdentity(identity.PID)
	if err != nil {
		return false, fmt.Errorf("verify Runner process-start identity: %w", err)
	}
	return observedIdentity == identity.ProcessIdentity, nil
}

func readRunnerProcessIdentity(path string) (runnerProcessIdentity, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return runnerProcessIdentity{}, err
	}
	var identity runnerProcessIdentity
	if err := json.Unmarshal(content, &identity); err != nil {
		return runnerProcessIdentity{}, fmt.Errorf("read Runner supervision marker: %w", err)
	}
	return identity, nil
}
