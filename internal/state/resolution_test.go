package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/scheduler"
)

func TestFileStoreMigratesV3ToV4Losslessly(t *testing.T) {
	acknowledgedAt := time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC)
	fixture := State{Version: 3, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 2,
		Runs: []scheduler.Run{
			{Issue: 1, RunID: "active", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Branch: "agent/active", Worktree: "/worktree", LogPath: "/log", StderrPath: "/stderr", WorkerLogOpen: true, Error: "diagnostic"},
			{Issue: 2, RunID: "history", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, AcknowledgedAt: &acknowledgedAt},
		}, Leases: []scheduler.Lease{{LeaseID: "lease", Issue: 1, RunID: "active"}}}
	path := filepath.Join(t.TempDir(), "state.json")
	encoded, _ := json.Marshal(fixture)
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	preview, migration, err := (FileStore{Path: path}).Preview()
	if err != nil || !migration || preview.Version != CurrentVersion {
		t.Fatalf("preview = %#v %t %v", preview, migration, err)
	}
	expected := fixture
	expected.Version = CurrentVersion
	markLegacyPromptOwnership(&expected)
	if !reflect.DeepEqual(preview, expected) {
		t.Fatalf("migration changed metadata:\ngot %#v\nwant %#v", preview, expected)
	}
	before, _ := os.ReadFile(path)
	if !strings.Contains(string(before), `"version":3`) {
		t.Fatalf("Preview persisted migration: %s", before)
	}
	loaded, err := (FileStore{Path: path}).Load()
	if err != nil || !reflect.DeepEqual(loaded, expected) {
		t.Fatalf("load = %#v, %v", loaded, err)
	}
}

func TestFileStoreValidatesExternalResolutionLeaseAndMetadata(t *testing.T) {
	at := time.Date(2026, 7, 28, 1, 2, 3, 0, time.UTC)
	store := FileStore{Path: filepath.Join(t.TempDir(), "state.json")}
	resolved := scheduler.Run{Issue: 1, RunID: "resolved", Status: scheduler.StatusResolvedExternally, WorkerMode: scheduler.WorkerModePrint, LogPath: "/retained/worker.jsonl", WorkerLogOpen: false, ResolvedExternallyAt: &at, ClosureReason: "completed"}
	for _, reason := range []string{"completed", "not-planned"} {
		valid := resolved
		valid.ClosureReason = reason
		if err := store.Save(State{Version: CurrentVersion, Runs: []scheduler.Run{valid}}); err != nil {
			t.Fatalf("supported closure reason %q: %v", reason, err)
		}
	}

	invalid := []struct {
		name, want string
		mutate     func(*State)
	}{
		{name: "missing resolution timestamp", want: "invalid resolution metadata", mutate: func(value *State) {
			value.Runs[0].ResolvedExternallyAt = nil
		}},
		{name: "zero resolution timestamp", want: "invalid resolution metadata", mutate: func(value *State) {
			zero := time.Time{}
			value.Runs[0].ResolvedExternallyAt = &zero
		}},
		{name: "unsupported closure reason", want: "invalid resolution metadata", mutate: func(value *State) {
			value.Runs[0].ClosureReason = "future"
		}},
		{name: "Completion timestamp", want: "invalid resolution metadata", mutate: func(value *State) {
			value.Runs[0].CompletedAt = &at
		}},
		{name: "pending Completion cleanup", want: "pending Completion cleanup", mutate: func(value *State) {
			value.Runs[0].CleanupPending = true
		}},
		{name: "open Worker-log marker", want: "invalid resolution metadata", mutate: func(value *State) {
			value.Runs[0].WorkerLogOpen = true
		}},
		{name: "active Lease", want: "references externally resolved Run", mutate: func(value *State) {
			value.Leases = []scheduler.Lease{{LeaseID: "lease", Issue: 1, RunID: "resolved"}}
		}},
		{name: "resolution timestamp on other status", want: "External Resolution metadata", mutate: func(value *State) {
			value.Runs[0].Status = scheduler.StatusFailed
			value.Runs[0].ClosureReason = ""
		}},
		{name: "closure reason on other status", want: "External Resolution metadata", mutate: func(value *State) {
			value.Runs[0].Status = scheduler.StatusFailed
			value.Runs[0].ResolvedExternallyAt = nil
		}},
		{name: "diagnostic warning on other status", want: "External Resolution metadata", mutate: func(value *State) {
			value.Runs[0].Status = scheduler.StatusFailed
			value.Runs[0].ResolvedExternallyAt = nil
			value.Runs[0].ClosureReason = ""
			value.Runs[0].DiagnosticWarning = "historical warning"
		}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			value := State{Version: CurrentVersion, Runs: []scheduler.Run{resolved}}
			test.mutate(&value)
			if err := store.Save(value); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Save() error = %v, want rejection containing %q for %#v", err, test.want, value)
			}
		})
	}

	progress := scheduler.Run{Issue: 2, RunID: "progress", Status: scheduler.StatusResolvingExternally, WorkerMode: scheduler.WorkerModePrint}
	if err := store.Save(State{Version: CurrentVersion, Runs: []scheduler.Run{progress}, Leases: []scheduler.Lease{{LeaseID: "progress", Issue: 2, RunID: "progress"}}}); err != nil {
		t.Fatal(err)
	}
}
