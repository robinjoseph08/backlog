package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/robinjoseph08/backlog/internal/scheduler"
)

const CurrentVersion = 1

type State struct {
	Version             int             `json:"version"`
	Repo                string          `json:"repo"`
	DefaultBranch       string          `json:"defaultBranch"`
	MaxConcurrentIssues int             `json:"maxConcurrentIssues"`
	Paused              bool            `json:"paused"`
	Runs                []scheduler.Run `json:"runs"`
}

type FileStore struct {
	Path string
}

func (s FileStore) Load() (State, error) {
	file, err := os.Open(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return State{Version: CurrentVersion}, nil
	}
	if err != nil {
		return State{}, fmt.Errorf("open state: %w", err)
	}
	defer file.Close()

	var value State
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&value); err != nil {
		return State{}, fmt.Errorf("decode state: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return State{}, err
	}
	if err := validate(value); err != nil {
		return State{}, err
	}
	return value, nil
}

func (s FileStore) Save(value State) error {
	if value.Version == 0 {
		value.Version = CurrentVersion
	}
	if err := validate(value); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	file, err := os.CreateTemp(filepath.Dir(s.Path), ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)

	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return fmt.Errorf("protect temporary state: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		file.Close()
		return fmt.Errorf("encode state: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync temporary state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary state: %w", err)
	}
	if err := os.Rename(temporaryPath, s.Path); err != nil {
		return fmt.Errorf("replace state: %w", err)
	}
	directory, err := os.Open(filepath.Dir(s.Path))
	if err != nil {
		return fmt.Errorf("open state directory for sync: %w", err)
	}
	if err := directory.Sync(); err != nil {
		directory.Close()
		return fmt.Errorf("sync state directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close state directory: %w", err)
	}
	return nil
}

func ensureEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return fmt.Errorf("decode trailing state data: %w", err)
	}
	return errors.New("state contains multiple JSON values")
}

func validate(value State) error {
	if value.Version != CurrentVersion {
		return fmt.Errorf("unsupported state version %d", value.Version)
	}
	issues := make(map[int]struct{}, len(value.Runs))
	for _, run := range value.Runs {
		if run.Issue <= 0 {
			return fmt.Errorf("state contains invalid issue number %d", run.Issue)
		}
		if run.RunID == "" {
			return fmt.Errorf("state contains a run without an id for issue #%d", run.Issue)
		}
		if !knownStatus(run.Status) {
			return fmt.Errorf("state contains unknown status %q for issue #%d", run.Status, run.Issue)
		}
		if run.Status == scheduler.StatusRunning {
			if run.PID <= 0 || run.StartedAt.IsZero() || run.ProcessIdentity == "" {
				return fmt.Errorf("state contains running issue #%d without durable process identity", run.Issue)
			}
		}
		if _, exists := issues[run.Issue]; exists {
			return fmt.Errorf("state contains duplicate lease for issue #%d", run.Issue)
		}
		issues[run.Issue] = struct{}{}
	}
	return nil
}

func knownStatus(status scheduler.Status) bool {
	switch status {
	case scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
		scheduler.StatusWaitingForMerge, scheduler.StatusMerged, scheduler.StatusFailed, scheduler.StatusNeedsHuman:
		return true
	default:
		return false
	}
}

type Lock struct {
	file *os.File
	mu   sync.Mutex
	held bool
}

// AcquireLock takes a non-blocking advisory lock. The open file descriptor is
// the lease, so process exit releases stale locks without PID reclamation races.
func AcquireLock(path string) (*Lock, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create lock directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open repository lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.New("repository runner already active")
		}
		return nil, fmt.Errorf("acquire repository lock: %w", err)
	}
	if err := file.Truncate(0); err == nil {
		_, _ = fmt.Fprintf(file, "%d\n", os.Getpid())
		_ = file.Sync()
	}
	return &Lock{file: file, held: true}, nil
}

func (l *Lock) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held {
		return nil
	}
	l.held = false
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	return errors.Join(unlockErr, closeErr)
}
