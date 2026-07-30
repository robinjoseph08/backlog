package state

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/initialprompt"
	"github.com/robinjoseph08/backlog/internal/scheduler"
)

func TestFileStoreRoundTripsStateAtomically(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nested", "state.json")
	store := FileStore{Path: path}
	want := State{
		Version:             CurrentVersion,
		Repo:                "acme/widgets",
		DefaultBranch:       "main",
		MaxConcurrentIssues: 3,
		Runs: []scheduler.Run{{
			Issue:           42,
			RunID:           "run-42",
			Status:          scheduler.StatusRunning,
			WorkerMode:      scheduler.WorkerModePrint,
			PID:             1234,
			ProcessIdentity: "1234:Thu Jul  2 03:04:05 2026",
			Branch:          "agent/issue-42-run-42",
			Worktree:        "/tmp/worktree-42",
			StartedAt:       time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC),
		}},
		Leases: []scheduler.Lease{{LeaseID: "run-42", Issue: 42, RunID: "run-42"}},
	}

	if err := store.Save(want); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Repo != want.Repo || len(got.Runs) != 1 || got.Runs[0].RunID != "run-42" || len(got.Leases) != 1 {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state permissions = %v, err = %v; want 0600", info.Mode().Perm(), err)
	}
}

func TestFileStoreRoundTripsCompleteRecoverySafetyMetadata(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	run := scheduler.Run{
		Issue: 98, IssueTitle: "Recovery", IssueURL: "https://github.com/acme/widgets/issues/98",
		RunID: "run-98", Status: scheduler.StatusSuspended, WorkerMode: scheduler.WorkerModeRPC,
		WorkerGeneration: 3, StoppedWorkerGeneration: 3, StoppedWorkerPID: 999999, StoppedWorkerProcessIdentity: "999999:stopped-start", WorkerStoppedAt: &now,
		Branch: "agent/issue-98-run-98", Worktree: filepath.Join(root, "worktree"), SessionName: "afk #98",
		SessionID: "session-98", SessionDir: filepath.Join(root, "sessions"),
		LogPath: filepath.Join(root, "run.jsonl"), StderrPath: filepath.Join(root, "run.stderr"),
		PullRequest: "https://github.com/acme/widgets/pull/98", Error: "diagnostic", FailureClass: scheduler.FailureValidation,
		WorkflowStage: "afk-coordinator", PreservedCause: "provider cause", ProviderContinuationAttempts: 1,
		RecoveryCount: 1, FirstRecoveredAt: &now, LastRecoveredAt: &now, StartedAt: now.Add(-time.Hour), UpdatedAt: now,
	}
	run.Continuation = &scheduler.ContinuationBoundary{
		SessionID: run.SessionID, SessionFile: filepath.Join(run.SessionDir, "session.jsonl"), Worktree: run.Worktree,
		LeafID: "leaf", EntryCount: 4, SHA256: strings.Repeat("a", 64), Workflow: "afk", WorkflowStage: "afk-coordinator",
		WorkerGeneration: 3, LocalCommit: strings.Repeat("b", 40), RemoteBranchState: "present", RemoteCommit: strings.Repeat("c", 40),
		PullRequest: run.PullRequest, PullRequestHead: strings.Repeat("c", 40), CheckpointFile: filepath.Join(root, "backlog-afk-checkpoint-v1.json"), CheckpointSHA256: strings.Repeat("d", 64), CheckpointStatus: "active", VerifiedAt: now,
	}
	lease := scheduler.Lease{LeaseID: "lease-exact-98", Issue: run.Issue, RunID: run.RunID}
	want := State{Version: CurrentVersion, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 2, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{lease}}
	store := FileStore{Path: filepath.Join(root, "state.json")}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Recovery metadata round trip:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestFileStoreMigratesLiteralV4RunningSuspendedAndFailedRunsToV6(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	fixture := `{
  "version": 4,
  "repo": "acme/widgets",
  "defaultBranch": "main",
  "maxConcurrentIssues": 3,
  "runs": [
    {"issue":41,"runId":"running-v4","status":"running","workerMode":"rpc","pid":4100,"processIdentity":"4100:running-start","branch":"agent/issue-41-running-v4","worktree":"/worktrees/running-v4","sessionId":"backlog-running-v4","sessionDir":"/sessions/running-v4","continuation":{"sessionId":"backlog-running-v4","sessionFile":"/sessions/running-v4/session.jsonl","worktree":"/worktrees/running-v4","leafId":"leaf","entryCount":2,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","verifiedAt":"2026-07-29T00:01:00Z"},"startedAt":"2026-07-29T00:00:00Z","updatedAt":"2026-07-29T00:01:00Z"},
    {"issue":42,"runId":"suspended-v4","status":"suspended","workerMode":"rpc","branch":"agent/issue-42-suspended-v4","worktree":"/worktrees/suspended-v4","sessionId":"backlog-suspended-v4","sessionDir":"/sessions/suspended-v4","continuation":{"sessionId":"backlog-suspended-v4","sessionFile":"/sessions/suspended-v4/session.jsonl","worktree":"/worktrees/suspended-v4","leafId":"leaf","entryCount":2,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","verifiedAt":"2026-07-29T00:02:00Z"},"startedAt":"2026-07-29T00:00:00Z","suspendedAt":"2026-07-29T00:02:01Z","updatedAt":"2026-07-29T00:02:01Z"},
    {"issue":43,"runId":"failed-v4","status":"failed","workerMode":"rpc","processIdentity":"4300:failed-start","branch":"agent/issue-43-failed-v4","worktree":"/worktrees/failed-v4","sessionId":"backlog-failed-v4","sessionDir":"/sessions/failed-v4","error":"validation failed","startedAt":"2026-07-29T00:00:00Z","updatedAt":"2026-07-29T00:03:00Z"}
  ],
  "leases": [
    {"leaseId":"running-v4","issue":41,"runId":"running-v4"},
    {"leaseId":"suspended-v4","issue":42,"runId":"suspended-v4"},
    {"leaseId":"failed-v4","issue":43,"runId":"failed-v4"}
  ]
}`
	if strings.Contains(fixture, "workerGeneration") || strings.Contains(fixture, "failureClass") || strings.Contains(fixture, "recoveryCount") {
		t.Fatal("literal V4 fixture contains post-V4 fields")
	}
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	store := FileStore{Path: path}
	preview, migrationRequired, err := store.Preview()
	if err != nil {
		t.Fatal(err)
	}
	if !migrationRequired || preview.Version != CurrentVersion || len(preview.Runs) != 3 {
		t.Fatalf("V4 Preview = %#v, migration=%t", preview, migrationRequired)
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || string(unchanged) != fixture {
		t.Fatalf("V4 Preview mutated source: %v\n%s", err, unchanged)
	}
	running := preview.Runs[0]
	if running.WorkerGeneration != 1 || running.Continuation == nil || running.Continuation.WorkerGeneration != 1 {
		t.Fatalf("migrated running continuation generation = %#v", running)
	}
	suspended := preview.Runs[1]
	if suspended.WorkerGeneration != 1 || suspended.StoppedWorkerGeneration != 1 || suspended.WorkerStoppedAt == nil || suspended.Continuation.WorkerGeneration != 1 {
		t.Fatalf("migrated suspended Resume proof = %#v", suspended)
	}
	failed := preview.Runs[2]
	if failed.StoppedWorkerPID != 4300 || failed.StoppedWorkerProcessIdentity != "4300:failed-start" || failed.PID != 0 || failed.ProcessIdentity != "" {
		t.Fatalf("migrated failed offline Recovery identity = %#v", failed)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, preview) {
		t.Fatalf("persisted V6 differs from Preview:\ngot=%#v\npreview=%#v", got, preview)
	}
	persisted, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(persisted), `"version": 6`) || !strings.Contains(string(persisted), `"stoppedWorkerPid": 4300`) {
		t.Fatalf("V6 migration persistence = %s, %v", persisted, err)
	}
}

func TestFileStorePreviewDoesNotPersistV1Migration(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	legacy := `{"version":1,"paused":true,"runs":[{"issue":1,"runId":"failed","status":"failed"}]}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}

	got, migrationRequired, err := (FileStore{Path: path}).Preview()
	if err != nil {
		t.Fatal(err)
	}
	if !migrationRequired || got.Version != CurrentVersion || len(got.Runs) != 1 || len(got.Leases) != 1 {
		t.Fatalf("preview = %#v, migration required = %t", got, migrationRequired)
	}
	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(persisted), `"version":1`) || !strings.Contains(string(persisted), `"paused":true`) {
		t.Fatalf("preview persisted migration: %s", persisted)
	}
}

func TestFileStoreReportsV6TargetWhenV1MigrationPersistenceFails(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	path := filepath.Join(directory, "state.json")
	legacy := `{"version":1,"runs":[{"issue":1,"runId":"failed","status":"failed"}]}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(directory, 0o700)

	_, err := (FileStore{Path: path}).Load()
	if err == nil || !strings.Contains(err.Error(), "persist version 6 state migration") {
		t.Fatalf("V1 migration persistence error = %v", err)
	}
}

func TestFileStoreMigratesV1WithoutLosingRunArtifacts(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	legacy := `{
  "version": 1,
  "repo": "acme/widgets",
  "defaultBranch": "main",
  "maxConcurrentIssues": 2,
  "paused": true,
  "runs": [
    {
      "issue": 42,
      "runId": "legacy-failed",
      "status": "needs-human",
      "pid": 4321,
      "processIdentity": "4321:identity",
      "branch": "agent/issue-42-legacy",
      "worktree": "/tmp/legacy-worktree",
      "sessionName": "afk #42",
      "logPath": "/tmp/legacy.jsonl",
      "stderrPath": "/tmp/legacy.stderr.log",
      "pullRequest": "https://example.test/pull/42",
      "error": "diagnostic detail",
      "startedAt": "2026-07-01T01:02:03Z",
      "updatedAt": "2026-07-02T02:03:04Z"
    },
    {
      "issue": 7,
      "runId": "legacy-merged",
      "status": "merged",
      "branch": "agent/issue-7-legacy",
      "worktree": "/tmp/merged-worktree",
      "logPath": "/tmp/merged.jsonl",
      "pullRequest": "https://example.test/pull/7",
      "startedAt": "2026-06-01T01:02:03Z",
      "updatedAt": "2026-06-02T02:03:04Z",
      "completedAt": "2026-06-03T03:04:05Z"
    }
  ]
}`
	var source legacyState
	if err := json.Unmarshal([]byte(legacy), &source); err != nil {
		t.Fatal(err)
	}
	for index := range source.Runs {
		source.Runs[index].WorkerMode = scheduler.WorkerModePrint
	}
	if err := os.WriteFile(path, []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := (FileStore{Path: path}).Load()
	if err != nil {
		t.Fatalf("load and migrate: %v", err)
	}
	if got.Version != CurrentVersion || got.Repo != "acme/widgets" || got.DefaultBranch != "main" || got.MaxConcurrentIssues != 2 {
		t.Fatalf("migrated configuration = %#v", got)
	}
	if len(got.Runs) != 2 || len(got.Leases) != 1 {
		t.Fatalf("migrated Runs/Leases = %d/%d, want 2/1", len(got.Runs), len(got.Leases))
	}
	if !reflect.DeepEqual(got.Runs, source.Runs) {
		t.Fatalf("migrated Runs = %#v, want every source artifact preserved in %#v", got.Runs, source.Runs)
	}
	if lease := got.Leases[0]; lease.LeaseID != "legacy-failed" || lease.Issue != 42 || lease.RunID != "legacy-failed" {
		t.Fatalf("migrated Lease = %#v", lease)
	}

	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(encoded, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted["version"] != float64(CurrentVersion) {
		t.Fatalf("persisted version = %#v", persisted["version"])
	}
	if _, exists := persisted["paused"]; exists {
		t.Fatalf("obsolete paused field survived migration: %s", encoded)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("migrated state permissions = %v, want shared atomic Save permissions 0600", info.Mode().Perm())
	}
	if leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".state-*.tmp")); err != nil || len(leftovers) != 0 {
		t.Fatalf("migration temporary files = %v, err = %v", leftovers, err)
	}
}

func TestFileStoreMigrationRetainsOnlyUnfinishedV1Leases(t *testing.T) {
	t.Parallel()

	statuses := []scheduler.Status{
		scheduler.StatusClaimed,
		scheduler.StatusWorktreeReady,
		scheduler.StatusRunning,
		scheduler.StatusWaitingForMerge,
		scheduler.StatusFailed,
		scheduler.StatusNeedsHuman,
		scheduler.StatusReset,
		scheduler.StatusMerged,
	}
	legacy := legacyState{Version: legacyVersion}
	for index, status := range statuses {
		run := scheduler.Run{
			Issue: index + 1, RunID: string(status), Status: status,
			StartedAt: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		}
		if status == scheduler.StatusRunning {
			run.PID = 1234
			run.ProcessIdentity = "1234:identity"
		}
		legacy.Runs = append(legacy.Runs, run)
	}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := (FileStore{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Runs) != len(statuses) || len(got.Leases) != len(statuses)-2 {
		t.Fatalf("migrated Runs/Leases = %d/%d, want %d/%d", len(got.Runs), len(got.Leases), len(statuses), len(statuses)-2)
	}
	for _, lease := range got.Leases {
		if lease.RunID == string(scheduler.StatusMerged) || lease.RunID == string(scheduler.StatusReset) {
			t.Fatalf("handled V1 Run %q retained an active Lease", lease.RunID)
		}
	}
	reset := got.Runs[len(got.Runs)-2]
	if reset.Status != scheduler.StatusReset || reset.AcknowledgedAt != nil {
		t.Fatalf("migrated Reset = %#v, want handled Historical Run", reset)
	}
}

func TestFileStoreMigratesV2ToV3WithoutAcknowledgingOutcomesOrLosingMetadata(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	completedAt := time.Date(2026, 7, 3, 3, 4, 5, 0, time.UTC)
	fixture := State{Version: versionWithLeases, Repo: "acme/widgets", DefaultBranch: "main", MaxConcurrentIssues: 2,
		Runs: []scheduler.Run{
			{Issue: 1, IssueTitle: "Historical failure", IssueURL: "https://example.test/issues/1", RunID: "failed", Status: scheduler.StatusFailed,
				WorkerMode: scheduler.WorkerModePrint, Branch: "agent/failed", Worktree: "/worktrees/failed", LogPath: "/logs/failed.jsonl",
				Error: "diagnostic", StartedAt: completedAt.Add(-time.Hour), UpdatedAt: completedAt},
			{Issue: 2, RunID: "human", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModePrint, Error: "human diagnostic", UpdatedAt: completedAt},
			{Issue: 3, RunID: "reset", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint, Error: "reset diagnostic", UpdatedAt: completedAt},
			{Issue: 4, RunID: "merged", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, PullRequest: "https://example.test/pull/4", CompletedAt: &completedAt},
		},
	}
	encoded, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	preview, migrationRequired, err := (FileStore{Path: path}).Preview()
	if err != nil {
		t.Fatal(err)
	}
	if !migrationRequired || preview.Version != CurrentVersion {
		t.Fatalf("preview version/migration = %d/%t", preview.Version, migrationRequired)
	}
	for _, run := range preview.Runs {
		if run.AcknowledgedAt != nil {
			t.Fatalf("V2 Run %q was implicitly acknowledged", run.RunID)
		}
	}
	beforeLoad, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(beforeLoad), `"version":2`) {
		t.Fatalf("Preview persisted V2 migration: %s", beforeLoad)
	}

	got, err := (FileStore{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != CurrentVersion || got.Repo != fixture.Repo || got.DefaultBranch != fixture.DefaultBranch || got.MaxConcurrentIssues != fixture.MaxConcurrentIssues ||
		!reflect.DeepEqual(got.Runs, fixture.Runs) || len(got.Leases) != 0 {
		t.Fatalf("V2 migration lost state: got=%#v want=%#v", got, fixture)
	}
	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(persisted), `"version": 6`) {
		t.Fatalf("V6 migration was not persisted: %s", persisted)
	}
}

func TestFileStoreRoundTripsAndValidatesOutcomeAcknowledgmentMetadata(t *testing.T) {
	t.Parallel()

	acknowledgedAt := time.Date(2026, 7, 4, 5, 6, 7, 0, time.UTC)
	store := FileStore{Path: filepath.Join(t.TempDir(), "state.json")}
	value := State{Version: CurrentVersion, Runs: []scheduler.Run{{
		Issue: 1, RunID: "acknowledged", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, AcknowledgedAt: &acknowledgedAt,
	}}}
	if err := store.Save(value); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Runs[0].AcknowledgedAt == nil || !got.Runs[0].AcknowledgedAt.Equal(acknowledgedAt) {
		t.Fatalf("acknowledgment round trip = %#v", got.Runs[0].AcknowledgedAt)
	}

	zero := time.Time{}
	for _, test := range []struct {
		name  string
		run   scheduler.Run
		lease []scheduler.Lease
		want  string
	}{
		{name: "zero timestamp", run: scheduler.Run{Issue: 2, RunID: "zero", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, AcknowledgedAt: &zero}, want: "invalid Outcome Acknowledgment time"},
		{name: "Completion", run: scheduler.Run{Issue: 2, RunID: "merged", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, AcknowledgedAt: &acknowledgedAt}, want: "ineligible Run"},
		{name: "leased failure", run: scheduler.Run{Issue: 2, RunID: "leased", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, AcknowledgedAt: &acknowledgedAt}, lease: []scheduler.Lease{{LeaseID: "leased", Issue: 2, RunID: "leased"}}, want: "ineligible Run"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := store.Save(State{Version: CurrentVersion, Runs: []scheduler.Run{test.run}, Leases: test.lease})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestFileStoreRejectsUnsupportedNewerStateVersion(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"version":7,"runs":[],"leases":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (FileStore{Path: path}).Load(); err == nil || !strings.Contains(err.Error(), "unsupported state version 7") {
		t.Fatalf("newer state error = %v", err)
	}
}

func TestFileStoreAllowsRunHistoryWithOneActiveLeasePerIssue(t *testing.T) {
	t.Parallel()

	store := FileStore{Path: filepath.Join(t.TempDir(), "state.json")}
	value := State{
		Version: CurrentVersion,
		Runs: []scheduler.Run{
			{Issue: 5, RunID: "merged-1", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint},
			{Issue: 5, RunID: "merged-2", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint},
			{Issue: 5, RunID: "current", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		},
		Leases: []scheduler.Lease{{LeaseID: "current", Issue: 5, RunID: "current"}},
	}
	if err := store.Save(value); err != nil {
		t.Fatalf("save history: %v", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Runs) != 3 || len(got.Leases) != 1 {
		t.Fatalf("Runs/Leases = %d/%d, want 3/1", len(got.Runs), len(got.Leases))
	}
}

func TestFileStoreRejectsInvalidRunAndLeaseReferences(t *testing.T) {
	t.Parallel()

	printRun := func(issue int, id string, status scheduler.Status) scheduler.Run {
		return scheduler.Run{Issue: issue, RunID: id, Status: status, WorkerMode: scheduler.WorkerModePrint}
	}
	tests := []struct {
		name  string
		value State
		want  string
	}{
		{
			name:  "unknown Run",
			value: State{Version: CurrentVersion, Leases: []scheduler.Lease{{LeaseID: "lease", Issue: 1, RunID: "missing"}}},
			want:  "unknown Run",
		},
		{
			name: "issue mismatch",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{printRun(1, "run", scheduler.StatusFailed)},
				Leases: []scheduler.Lease{{LeaseID: "lease", Issue: 2, RunID: "run"}}},
			want: "does not match",
		},
		{
			name:  "duplicate Run id",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{printRun(1, "duplicate", scheduler.StatusFailed), printRun(2, "duplicate", scheduler.StatusFailed)}},
			want:  "duplicate run id",
		},
		{
			name: "duplicate Lease id",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{
				printRun(1, "first", scheduler.StatusFailed), printRun(2, "second", scheduler.StatusFailed),
			}, Leases: []scheduler.Lease{
				{LeaseID: "duplicate", Issue: 1, RunID: "first"}, {LeaseID: "duplicate", Issue: 2, RunID: "second"},
			}},
			want: "duplicate Lease id",
		},
		{
			name: "multiple Leases for issue",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{
				printRun(1, "first", scheduler.StatusFailed), printRun(1, "second", scheduler.StatusFailed),
			}, Leases: []scheduler.Lease{
				{LeaseID: "first", Issue: 1, RunID: "first"}, {LeaseID: "second", Issue: 1, RunID: "second"},
			}},
			want: "multiple active Leases",
		},
		{
			name: "merged Run leased",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{printRun(1, "merged", scheduler.StatusMerged)},
				Leases: []scheduler.Lease{{LeaseID: "lease", Issue: 1, RunID: "merged"}}},
			want: "merged Run",
		},
		{
			name: "reset Run leased",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{printRun(1, "reset", scheduler.StatusReset)},
				Leases: []scheduler.Lease{{LeaseID: "lease", Issue: 1, RunID: "reset"}}},
			want: "reset Run",
		},
		{
			name:  "active Run has no Lease",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{printRun(1, "running", scheduler.StatusClaimed)}},
			want:  "has no Lease",
		},
		{
			name:  "resetting Run has no Lease",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{printRun(1, "resetting", scheduler.StatusResetting)}},
			want:  "has no Lease",
		},
		{
			name:  "unknown worker mode",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{{Issue: 1, RunID: "run", Status: scheduler.StatusFailed, WorkerMode: "future"}}},
			want:  "unknown worker mode",
		},
		{
			name: "open Worker log without path",
			value: State{Version: CurrentVersion, Runs: []scheduler.Run{{
				Issue: 1, RunID: "run", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, WorkerLogOpen: true,
			}}},
			want: "open Worker log but no log path",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := (FileStore{Path: filepath.Join(t.TempDir(), "state.json")}).Save(test.value)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("save error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestFileStoreLoadRejectsCorruptLeaseReference(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	corrupt := `{"version":2,"runs":[],"leases":[{"leaseId":"bad","issue":1,"runId":"missing"}]}`
	if err := os.WriteFile(path, []byte(corrupt), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (FileStore{Path: path}).Load(); err == nil || !strings.Contains(err.Error(), "unknown Run") {
		t.Fatalf("load error = %v, want unknown Run rejection", err)
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

func TestFileStorePersistsRPCSessionIdentityAndRequiresItsStorage(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.json")
	store := FileStore{Path: path}
	value := State{
		Version: CurrentVersion,
		Runs: []scheduler.Run{{
			Issue: 5, RunID: "run-5", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
			SessionID: "backlog-run-5", SessionDir: "/state/sessions/run-5",
		}},
		Leases: []scheduler.Lease{{LeaseID: "run-5", Issue: 5, RunID: "run-5"}},
	}
	if err := store.Save(value); err != nil {
		t.Fatalf("save RPC Run: %v", err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Runs[0].SessionID != value.Runs[0].SessionID || got.Runs[0].SessionDir != value.Runs[0].SessionDir {
		t.Fatalf("RPC session metadata = %#v", got.Runs[0])
	}
	value.Runs[0].SessionDir = ""
	if err := store.Save(value); err == nil || !strings.Contains(err.Error(), "without durable session identity and storage") {
		t.Fatalf("missing session storage error = %v", err)
	}
}

func TestFileStorePersistsOnlyVerifiedStoppedSuspension(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := FileStore{Path: filepath.Join(root, "state.json")}
	verifiedAt := time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC)
	run := scheduler.Run{
		Issue: 8, RunID: "run-8", Status: scheduler.StatusSuspended, WorkerMode: scheduler.WorkerModeRPC,
		Branch: "agent/issue-8-run-8", Worktree: filepath.Join(root, "worktree"),
		SessionID: "backlog-run-8", SessionDir: filepath.Join(root, "sessions", "run-8"),
		Continuation: &scheduler.ContinuationBoundary{
			SessionID: "backlog-run-8", SessionFile: filepath.Join(root, "sessions", "run-8", "session.jsonl"),
			Worktree: filepath.Join(root, "worktree"), LeafID: "leaf", EntryCount: 3, SHA256: strings.Repeat("a", 64), VerifiedAt: verifiedAt,
		},
	}
	run.ResumePending = true
	value := State{Version: CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "run-8", Issue: 8, RunID: "run-8"}}}
	if err := store.Save(value); err != nil {
		t.Fatalf("save suspended Run: %v", err)
	}
	got, err := store.Load()
	if err != nil || got.Runs[0].Continuation == nil || got.Runs[0].Continuation.LeafID != "leaf" || !got.Runs[0].ResumePending || len(got.Leases) != 1 {
		t.Fatalf("loaded suspension = %#v, err = %v", got, err)
	}
	got.Runs[0].Status = scheduler.StatusNeedsHuman
	if err := store.Save(got); err != nil {
		t.Fatalf("save interrupted pending Resume as needs-human: %v", err)
	}

	invalid := value
	invalid.Runs = append([]scheduler.Run(nil), value.Runs...)
	invalid.Runs[0].PID = 1234
	invalid.Runs[0].ProcessIdentity = "identity"
	if err := store.Save(invalid); err == nil || !strings.Contains(err.Error(), "verified stopped continuation") {
		t.Fatalf("live suspended Run error = %v", err)
	}
	invalid = value
	invalid.Runs = append([]scheduler.Run(nil), value.Runs...)
	boundary := *invalid.Runs[0].Continuation
	boundary.SessionFile = filepath.Join(root, "outside.jsonl")
	invalid.Runs[0].Continuation = &boundary
	if err := store.Save(invalid); err == nil || !strings.Contains(err.Error(), "outside its session directory") {
		t.Fatalf("outside continuation path error = %v", err)
	}
}

func TestFileStoreLoadsUnsafeSuspensionForNeedsHumanRecovery(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path := filepath.Join(root, "state.json")
	baseRun := scheduler.Run{
		Issue: 9, RunID: "run-9", Status: scheduler.StatusSuspended, WorkerMode: scheduler.WorkerModeRPC,
		Branch: "agent/issue-9-run-9", Worktree: filepath.Join(root, "worktree"),
		SessionID: "backlog-run-9", SessionDir: filepath.Join(root, "sessions", "run-9"),
		Continuation: &scheduler.ContinuationBoundary{
			SessionID: "backlog-run-9", SessionFile: filepath.Join(root, "sessions", "run-9", "session.jsonl"),
			Worktree: filepath.Join(root, "worktree"), LeafID: "leaf", EntryCount: 3, SHA256: strings.Repeat("a", 64), VerifiedAt: time.Now(),
		},
	}
	tests := []struct {
		name   string
		mutate func(*scheduler.Run)
	}{
		{name: "missing continuation", mutate: func(run *scheduler.Run) { run.Continuation = nil }},
		{name: "pending Resume missing continuation", mutate: func(run *scheduler.Run) {
			run.ResumePending = true
			run.Continuation = nil
		}},
		{name: "malformed continuation", mutate: func(run *scheduler.Run) { run.Continuation.EntryCount = 0 }},
		{name: "legacy print suspension", mutate: func(run *scheduler.Run) {
			run.WorkerMode = scheduler.WorkerModePrint
			run.SessionID = ""
			run.SessionDir = ""
			run.Continuation = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := baseRun
			boundary := *baseRun.Continuation
			run.Continuation = &boundary
			test.mutate(&run)
			value := State{Version: CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: "lease-9", Issue: 9, RunID: run.RunID}}}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			loaded, err := (FileStore{Path: path}).Load()
			if err != nil {
				t.Fatalf("load unsafe suspension for reconciliation: %v", err)
			}
			loaded.Runs[0].Status = scheduler.StatusNeedsHuman
			loaded.Runs[0].Error = "unsafe continuation"
			if err := (FileStore{Path: path}).Save(loaded); err != nil {
				t.Fatalf("persist needs-human recovery: %v", err)
			}
			if len(loaded.Leases) != 1 {
				t.Fatalf("unsafe suspension Lease = %#v", loaded.Leases)
			}
		})
	}
}

func TestFileStoreRejectsMalformedRecoveryAndProviderMetadata(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	base := scheduler.Run{Issue: 10, RunID: "run-10", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint}
	tests := []struct {
		name   string
		mutate func(*scheduler.Run)
		want   string
	}{
		{name: "unknown failure class", mutate: func(run *scheduler.Run) { run.FailureClass = "unknown" }, want: "failure class"},
		{name: "stopped PID without identity", mutate: func(run *scheduler.Run) { run.StoppedWorkerPID = 123; run.StoppedWorkerProcessIdentity = "" }, want: "stop proof"},
		{name: "stopped identity without generation", mutate: func(run *scheduler.Run) {
			run.StoppedWorkerPID = 123
			run.StoppedWorkerProcessIdentity = "123:start"
			run.StoppedWorkerGeneration = 0
			run.WorkerStoppedAt = nil
		}, want: "stop proof"},
		{name: "provider budget overflow", mutate: func(run *scheduler.Run) { run.ProviderContinuationAttempts = 2 }, want: "provider continuation budget"},
		{name: "cooldown outside suspension", mutate: func(run *scheduler.Run) { run.ProviderContinuationAttempts = 1; run.ResumeAfter = &now }, want: "cooldown"},
		{name: "count without timestamps", mutate: func(run *scheduler.Run) { run.RecoveryCount = 1 }, want: "Recovery metadata"},
		{name: "timestamps without count", mutate: func(run *scheduler.Run) { run.FirstRecoveredAt = &now; run.LastRecoveredAt = &now }, want: "Recovery metadata"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := base
			test.mutate(&run)
			err := (FileStore{Path: filepath.Join(t.TempDir(), "state.json")}).Save(State{Version: CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}}})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDecodeRecoverableRunPreservesMalformedContinuationEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		encoded          string
		wantContinuation bool
	}{
		{name: "malformed value", encoded: `{"continuation":"malformed"}`, wantContinuation: true},
		{name: "case-variant key", encoded: `{"Continuation":null}`, wantContinuation: true},
		{name: "duplicate exact key", encoded: `{"continuation":null,"continuation":null}`, wantContinuation: true},
		{name: "absent key", encoded: `{}`, wantContinuation: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run, err := decodeRecoverableRun(json.RawMessage(test.encoded))
			if err != nil {
				t.Fatalf("decode recoverable Run: %v", err)
			}
			if (run.Continuation != nil) != test.wantContinuation {
				t.Fatalf("Continuation = %#v, want present %t", run.Continuation, test.wantContinuation)
			}
			if run.Continuation != nil && *run.Continuation != (scheduler.ContinuationBoundary{}) {
				t.Fatalf("Continuation = %#v, want empty recovery sentinel", run.Continuation)
			}
		})
	}
}

func TestFileStoreLoadsStructurallyMalformedContinuationForRecovery(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path := filepath.Join(root, "state.json")
	encoded := map[string]any{
		"version": CurrentVersion,
		"runs": []any{map[string]any{
			"issue": 9, "runId": "run-9", "status": "suspended", "workerMode": "rpc",
			"branch": "agent/issue-9-run-9", "worktree": filepath.Join(root, "worktree"),
			"sessionId": "session-9", "sessionDir": filepath.Join(root, "sessions"),
			"startedAt": time.Now(), "updatedAt": time.Now(), "continuation": "malformed",
		}},
		"leases": []any{map[string]any{"leaseId": "lease-9", "issue": 9, "runId": "run-9"}},
	}
	contents, err := json.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := (FileStore{Path: path}).Load()
	if err != nil {
		t.Fatalf("load malformed continuation for recovery: %v", err)
	}
	if len(loaded.Runs) != 1 || loaded.Runs[0].Continuation == nil || *loaded.Runs[0].Continuation != (scheduler.ContinuationBoundary{}) || len(loaded.Leases) != 1 {
		t.Fatalf("recoverable malformed continuation = %#v", loaded)
	}
	loaded.Runs[0].Status = scheduler.StatusNeedsHuman
	loaded.Runs[0].Error = "malformed continuation"
	if err := (FileStore{Path: path}).Save(loaded); err != nil {
		t.Fatalf("persist malformed continuation recovery: %v", err)
	}
}

func TestFileStoreRejectsRunningLeaseWithoutStartIdentity(t *testing.T) {
	t.Parallel()

	store := FileStore{Path: filepath.Join(t.TempDir(), "state.json")}
	err := store.Save(State{
		Version: CurrentVersion,
		Runs: []scheduler.Run{{
			Issue: 1, RunID: "bad", Status: scheduler.StatusRunning, WorkerMode: scheduler.WorkerModePrint, PID: 1234,
		}},
		Leases: []scheduler.Lease{{LeaseID: "bad", Issue: 1, RunID: "bad"}},
	})
	if err == nil {
		t.Fatal("save succeeded, want missing process start identity error")
	}
}

func TestReadOnlyLocksDoNotCreateOrChangeResources(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	before, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireReadOnlyLock(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode() != after.Mode() || before.Size() != after.Size() {
		t.Fatalf("read-only lock changed directory metadata: before=%v after=%v", before, after)
	}
	missing := filepath.Join(directory, "missing.lock")
	if lock, exists, err := AcquireExistingReadOnlyLock(missing); err != nil || exists || lock != nil {
		t.Fatalf("optional missing lock = %#v, %t, %v", lock, exists, err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("optional lock created a file: %v", err)
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

func TestFileStoreMigratesV5PromptDigestSchemaWithoutChangingLegacyRunEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	fixture := `{
  "version": 5,
  "repo": "acme/widgets",
  "defaultBranch": "main",
  "maxConcurrentIssues": 1,
  "runs": [{
    "issue": 42,
    "issueTitle": "Legacy Run",
    "issueUrl": "https://example.test/issues/42",
    "runId": "legacy-v5",
    "status": "failed",
    "workerMode": "rpc",
    "workerGeneration": 1,
    "stoppedWorkerGeneration": 1,
    "workerStoppedAt": "2026-07-29T01:02:03Z",
    "branch": "agent/issue-42-legacy-v5",
    "worktree": "/worktrees/legacy-v5",
    "sessionName": "afk #42",
    "sessionId": "backlog-legacy-v5",
    "sessionDir": "/sessions/legacy-v5",
    "continuation": {
      "sessionId": "backlog-legacy-v5",
      "sessionFile": "/sessions/legacy-v5/session.jsonl",
      "worktree": "/worktrees/legacy-v5",
      "leafId": "leaf",
      "entryCount": 2,
      "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "workflow": "afk",
      "workflowStage": "afk-coordinator",
      "checkpointStatus": "active",
      "verifiedAt": "2026-07-29T01:02:03Z"
    },
    "logPath": "/logs/legacy-v5.jsonl",
    "stderrPath": "/logs/legacy-v5.stderr.log",
    "error": "retained diagnostic",
    "startedAt": "2026-07-29T00:00:00Z",
    "updatedAt": "2026-07-29T01:02:03Z"
  }],
  "leases": [{"leaseId":"legacy-v5","issue":42,"runId":"legacy-v5"}]
}`
	if err := os.WriteFile(path, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}
	store := FileStore{Path: path}
	preview, migrationRequired, err := store.Preview()
	if err != nil {
		t.Fatal(err)
	}
	if !migrationRequired || preview.Version != CurrentVersion || len(preview.Runs) != 1 || len(preview.Leases) != 1 {
		t.Fatalf("V5 preview = %#v, migration=%t", preview, migrationRequired)
	}
	run := preview.Runs[0]
	if run.PromptDigest != "" || run.Error != "retained diagnostic" || run.Continuation == nil || run.Continuation.LeafID != "leaf" {
		t.Fatalf("V5 migration changed legacy evidence: %#v", run)
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || string(unchanged) != fixture {
		t.Fatalf("Preview changed V5 source: %v\n%s", err, unchanged)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, preview) {
		t.Fatalf("persisted V6 differs from Preview:\ngot=%#v\nwant=%#v", loaded, preview)
	}
}

func TestFileStoreRoundTripsAndValidatesPromptDigest(t *testing.T) {
	store := FileStore{Path: filepath.Join(t.TempDir(), "state.json")}
	run := scheduler.Run{
		Issue: 7, RunID: "run-7", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModeRPC,
		SessionID: "session-7", SessionDir: "/sessions/run-7", PromptDigest: initialprompt.Digest(strings.Repeat("a", 64)),
	}
	value := State{Version: CurrentVersion, Runs: []scheduler.Run{run}, Leases: []scheduler.Lease{{LeaseID: run.RunID, Issue: run.Issue, RunID: run.RunID}}}
	if err := store.Save(value); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Runs[0].PromptDigest != run.PromptDigest {
		t.Fatalf("prompt digest = %q, want %q", loaded.Runs[0].PromptDigest, run.PromptDigest)
	}
	for _, digest := range []string{"short", strings.Repeat("z", 64), strings.Repeat("a", 66)} {
		value.Runs[0].PromptDigest = initialprompt.Digest(digest)
		if err := store.Save(value); err == nil || !strings.Contains(err.Error(), "invalid prompt digest") {
			t.Fatalf("digest %q validation error = %v", digest, err)
		}
	}
}
