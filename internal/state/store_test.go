package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/robinjoseph/pi-backlog-runner/internal/scheduler"
)

func TestFileStoreRoundTripsStateAtomically(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nested", "state.json")
	store := FileStore{Path: path}
	want := State{
		Version:             1,
		Repo:                "acme/widgets",
		DefaultBranch:       "main",
		MaxConcurrentIssues: 3,
		Runs: []scheduler.Run{{
			Issue:           42,
			RunID:           "run-42",
			Status:          scheduler.StatusRunning,
			PID:             1234,
			ProcessIdentity: "1234:Thu Jul  2 03:04:05 2026",
			Branch:          "agent/issue-42-run-42",
			Worktree:        "/tmp/worktree-42",
			StartedAt:       time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC),
		}},
	}

	if err := store.Save(want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Repo != want.Repo || len(got.Runs) != 1 || got.Runs[0].RunID != "run-42" {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state permissions = %v, err = %v; want 0600", info.Mode().Perm(), err)
	}
}

func TestFileStoreRejectsMalformedState(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := (FileStore{Path: path}).Load()
	if err == nil {
		t.Fatal("load succeeded, want malformed-state error")
	}
}

func TestFileStoreRejectsRunningLeaseWithoutStartIdentity(t *testing.T) {
	t.Parallel()

	store := FileStore{Path: filepath.Join(t.TempDir(), "state.json")}
	err := store.Save(State{
		Version: CurrentVersion,
		Runs:    []scheduler.Run{{Issue: 1, RunID: "bad", Status: scheduler.StatusRunning, PID: 1234}},
	})
	if err == nil {
		t.Fatal("save succeeded, want missing process start identity error")
	}
}

func TestRepositoryLockAllowsOnlyOneOwner(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "runner.lock")
	first, err := AcquireLock(path)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer first.Release()

	if _, err := AcquireLock(path); err == nil {
		t.Fatal("second acquire succeeded, want already-running error")
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	second, err := AcquireLock(path)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	defer second.Release()
}
