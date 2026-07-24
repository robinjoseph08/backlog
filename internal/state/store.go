package state

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/robinjoseph08/backlog/internal/scheduler"
)

const CurrentVersion = 2

const legacyVersion = 1
const sha256HexLength = 64

type State struct {
	Version             int               `json:"version"`
	Repo                string            `json:"repo"`
	DefaultBranch       string            `json:"defaultBranch"`
	MaxConcurrentIssues int               `json:"maxConcurrentIssues"`
	Runs                []scheduler.Run   `json:"runs"`
	Leases              []scheduler.Lease `json:"leases"`
}

type legacyState struct {
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
	value, _, err := s.load(true)
	return value, err
}

// Preview loads and validates state without persisting a required migration.
// Callers can use the returned flag to acquire their coordination lock before
// invoking Load to commit the migration.
func (s FileStore) Preview() (State, bool, error) {
	return s.load(false)
}

func (s FileStore) load(persistMigration bool) (State, bool, error) {
	file, err := os.Open(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return State{Version: CurrentVersion}, false, nil
	}
	if err != nil {
		return State{}, false, fmt.Errorf("open state: %w", err)
	}

	var encoded json.RawMessage
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&encoded); err != nil {
		file.Close()
		return State{}, false, fmt.Errorf("decode state: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		file.Close()
		return State{}, false, err
	}
	if err := file.Close(); err != nil {
		return State{}, false, fmt.Errorf("close state after reading: %w", err)
	}

	var header struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(encoded, &header); err != nil {
		return State{}, false, fmt.Errorf("decode state version: %w", err)
	}
	switch header.Version {
	case CurrentVersion:
		var value State
		if err := json.Unmarshal(encoded, &value); err != nil {
			return State{}, false, fmt.Errorf("decode state: %w", err)
		}
		if err := validate(value); err != nil {
			return State{}, false, err
		}
		return value, false, nil
	case legacyVersion:
		var legacy legacyState
		if err := json.Unmarshal(encoded, &legacy); err != nil {
			return State{}, false, fmt.Errorf("decode version 1 state: %w", err)
		}
		value, err := migrateV1(legacy)
		if err != nil {
			return State{}, false, err
		}
		if persistMigration {
			if err := s.Save(value); err != nil {
				return State{}, false, fmt.Errorf("persist version 2 state migration: %w", err)
			}
		}
		return value, true, nil
	default:
		return State{}, false, fmt.Errorf("unsupported state version %d", header.Version)
	}
}

func migrateV1(legacy legacyState) (State, error) {
	if err := validateV1(legacy); err != nil {
		return State{}, err
	}
	value := State{
		Version:             CurrentVersion,
		Repo:                legacy.Repo,
		DefaultBranch:       legacy.DefaultBranch,
		MaxConcurrentIssues: legacy.MaxConcurrentIssues,
		Runs:                append([]scheduler.Run(nil), legacy.Runs...),
	}
	for index := range value.Runs {
		run := &value.Runs[index]
		run.WorkerMode = scheduler.WorkerModePrint
		if run.Status != scheduler.StatusMerged {
			value.Leases = append(value.Leases, scheduler.Lease{
				LeaseID: run.RunID,
				Issue:   run.Issue,
				RunID:   run.RunID,
			})
		}
	}
	if err := validate(value); err != nil {
		return State{}, fmt.Errorf("migrate version 1 state: %w", err)
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

func validateV1(value legacyState) error {
	if value.Version != legacyVersion {
		return fmt.Errorf("unsupported state version %d", value.Version)
	}
	issues := make(map[int]struct{}, len(value.Runs))
	for _, run := range value.Runs {
		if err := validateRun(run, false); err != nil {
			return err
		}
		if _, exists := issues[run.Issue]; exists {
			return fmt.Errorf("version 1 state contains duplicate lease for issue #%d", run.Issue)
		}
		issues[run.Issue] = struct{}{}
	}
	return nil
}

func validate(value State) error {
	if value.Version != CurrentVersion {
		return fmt.Errorf("unsupported state version %d", value.Version)
	}
	runs := make(map[string]scheduler.Run, len(value.Runs))
	for _, run := range value.Runs {
		if err := validateRun(run, true); err != nil {
			return err
		}
		if _, exists := runs[run.RunID]; exists {
			return fmt.Errorf("state contains duplicate run id %q", run.RunID)
		}
		runs[run.RunID] = run
	}

	leaseIDs := make(map[string]struct{}, len(value.Leases))
	leasedIssues := make(map[int]struct{}, len(value.Leases))
	leasedRuns := make(map[string]struct{}, len(value.Leases))
	for _, lease := range value.Leases {
		if lease.LeaseID == "" {
			return errors.New("state contains a Lease without an id")
		}
		if lease.Issue <= 0 {
			return fmt.Errorf("state contains a Lease with invalid issue number %d", lease.Issue)
		}
		if lease.RunID == "" {
			return fmt.Errorf("state contains Lease %q without a Run reference", lease.LeaseID)
		}
		if _, exists := leaseIDs[lease.LeaseID]; exists {
			return fmt.Errorf("state contains duplicate Lease id %q", lease.LeaseID)
		}
		leaseIDs[lease.LeaseID] = struct{}{}
		if _, exists := leasedIssues[lease.Issue]; exists {
			return fmt.Errorf("state contains multiple active Leases for issue #%d", lease.Issue)
		}
		leasedIssues[lease.Issue] = struct{}{}
		if _, exists := leasedRuns[lease.RunID]; exists {
			return fmt.Errorf("state contains multiple active Leases for Run %q", lease.RunID)
		}
		leasedRuns[lease.RunID] = struct{}{}

		run, exists := runs[lease.RunID]
		if !exists {
			return fmt.Errorf("Lease %q references unknown Run %q", lease.LeaseID, lease.RunID)
		}
		if run.Issue != lease.Issue {
			return fmt.Errorf("Lease %q issue #%d does not match Run %q issue #%d", lease.LeaseID, lease.Issue, run.RunID, run.Issue)
		}
		if run.Status == scheduler.StatusMerged {
			return fmt.Errorf("Lease %q references merged Run %q", lease.LeaseID, run.RunID)
		}
		if run.Status == scheduler.StatusReset {
			return fmt.Errorf("Lease %q references reset Run %q", lease.LeaseID, run.RunID)
		}
	}
	for _, run := range value.Runs {
		if scheduler.RequiresLease(run.Status) {
			if _, exists := leasedRuns[run.RunID]; !exists {
				return fmt.Errorf("active Run %q for issue #%d has no Lease", run.RunID, run.Issue)
			}
		}
	}
	return nil
}

func validateRun(run scheduler.Run, requireWorkerMode bool) error {
	if run.Issue <= 0 {
		return fmt.Errorf("state contains invalid issue number %d", run.Issue)
	}
	if run.RunID == "" {
		return fmt.Errorf("state contains a Run without an id for issue #%d", run.Issue)
	}
	if !knownStatus(run.Status) {
		return fmt.Errorf("state contains unknown status %q for issue #%d", run.Status, run.Issue)
	}
	if requireWorkerMode && run.WorkerMode != scheduler.WorkerModePrint && run.WorkerMode != scheduler.WorkerModeRPC {
		return fmt.Errorf("state contains Run %q with unknown worker mode %q", run.RunID, run.WorkerMode)
	}
	if run.WorkerMode == scheduler.WorkerModeRPC && (run.SessionID == "" || run.SessionDir == "") {
		return fmt.Errorf("state contains RPC Run %q without durable session identity and storage", run.RunID)
	}
	if run.WorkerLogOpen && run.LogPath == "" {
		return fmt.Errorf("state contains Run %q with an open Worker log but no log path", run.RunID)
	}
	if run.Status == scheduler.StatusRunning {
		if run.PID <= 0 || run.StartedAt.IsZero() || run.ProcessIdentity == "" {
			return fmt.Errorf("state contains running issue #%d without durable process identity", run.Issue)
		}
	}
	if run.Continuation != nil {
		boundary := run.Continuation
		_, hashErr := hex.DecodeString(boundary.SHA256)
		if run.WorkerMode != scheduler.WorkerModeRPC || boundary.SessionID != run.SessionID || boundary.Worktree != run.Worktree ||
			boundary.SessionFile == "" || boundary.LeafID == "" || boundary.EntryCount <= 0 || len(boundary.SHA256) != sha256HexLength || hashErr != nil || boundary.VerifiedAt.IsZero() {
			return fmt.Errorf("state contains Run %q with an invalid continuation boundary", run.RunID)
		}
		relative, err := filepath.Rel(run.SessionDir, boundary.SessionFile)
		if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("state contains Run %q with a continuation file outside its session directory", run.RunID)
		}
	}
	if run.Status == scheduler.StatusSuspended {
		if run.PID != 0 || run.ProcessIdentity != "" || run.Continuation == nil {
			return fmt.Errorf("state contains suspended issue #%d without a verified stopped continuation", run.Issue)
		}
	}
	return nil
}

func knownStatus(status scheduler.Status) bool {
	switch status {
	case scheduler.StatusClaimed, scheduler.StatusWorktreeReady, scheduler.StatusRunning,
		scheduler.StatusWaitingForMerge, scheduler.StatusSuspended, scheduler.StatusResetting, scheduler.StatusReset,
		scheduler.StatusMerged, scheduler.StatusFailed, scheduler.StatusNeedsHuman:
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
	lock, err := acquireOpenFileLock(file)
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(0); err == nil {
		_, _ = fmt.Fprintf(file, "%d\n", os.Getpid())
		_ = file.Sync()
	}
	return lock, nil
}

// AcquireReadOnlyLock coordinates through an existing file or directory
// without creating, truncating, or otherwise changing it.
func AcquireReadOnlyLock(path string) (*Lock, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open read-only repository lock: %w", err)
	}
	return acquireOpenFileLock(file)
}

// AcquireExistingReadOnlyLock is the optional-file form used to interoperate
// with older lock files while preserving a mutation-free inspection.
func AcquireExistingReadOnlyLock(path string) (*Lock, bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("open existing repository lock: %w", err)
	}
	lock, err := acquireOpenFileLock(file)
	return lock, true, err
}

func acquireOpenFileLock(file *os.File) (*Lock, error) {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.New("repository runner already active")
		}
		return nil, fmt.Errorf("acquire repository lock: %w", err)
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
