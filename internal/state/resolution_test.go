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
			{Issue: 1, RunID: "active", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Branch: "agent/active", Worktree: "/worktree", LogPath: "/log", StderrPath: "/stderr", Error: "diagnostic"},
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
	at := time.Now().UTC()
	store := FileStore{Path: filepath.Join(t.TempDir(), "state.json")}
	resolved := scheduler.Run{Issue: 1, RunID: "resolved", Status: scheduler.StatusResolvedExternally, WorkerMode: scheduler.WorkerModePrint, ResolvedExternallyAt: &at, ClosureReason: "completed"}
	if err := store.Save(State{Version: CurrentVersion, Runs: []scheduler.Run{resolved}}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []State{
		{Version: CurrentVersion, Runs: []scheduler.Run{{Issue: 1, RunID: "progress", Status: scheduler.StatusResolvingExternally, WorkerMode: scheduler.WorkerModePrint}}},
		{Version: CurrentVersion, Runs: []scheduler.Run{resolved}, Leases: []scheduler.Lease{{LeaseID: "lease", Issue: 1, RunID: "resolved"}}},
		{Version: CurrentVersion, Runs: []scheduler.Run{{Issue: 1, RunID: "missing", Status: scheduler.StatusResolvedExternally, WorkerMode: scheduler.WorkerModePrint}}},
	} {
		if err := store.Save(value); err == nil {
			t.Fatalf("accepted invalid state %#v", value)
		}
	}
	progress := scheduler.Run{Issue: 2, RunID: "progress", Status: scheduler.StatusResolvingExternally, WorkerMode: scheduler.WorkerModePrint}
	if err := store.Save(State{Version: CurrentVersion, Runs: []scheduler.Run{progress}, Leases: []scheduler.Lease{{LeaseID: "progress", Issue: 2, RunID: "progress"}}}); err != nil {
		t.Fatal(err)
	}
}
