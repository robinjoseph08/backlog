package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

func TestAcknowledgeSelectorsAreAtomicAndIdempotentAcrossV2Migration(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	statePath := filepath.Join(stateDir, "state.json")
	updated := time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC)
	fixture := state.State{Version: 2, Repo: "acme/widgets", Runs: []scheduler.Run{
		{Issue: 1, RunID: "42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "numeric exact", UpdatedAt: updated},
		{Issue: 42, RunID: "issue-42", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Error: "issue collision", UpdatedAt: updated.Add(time.Minute)},
		{Issue: 10, RunID: "issue-10-old", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: updated.Add(2 * time.Minute)},
		{Issue: 10, RunID: "issue-10-new", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: updated.Add(3 * time.Minute)},
		{Issue: 20, RunID: "exact-20", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: updated.Add(4 * time.Minute)},
		{Issue: 7, RunID: "leased-failure", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: updated.Add(5 * time.Minute)},
		{Issue: 30, RunID: "merged", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: updated.Add(6 * time.Minute)},
		{Issue: 31, RunID: "reset", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: updated.Add(7 * time.Minute)},
	}, Leases: []scheduler.Lease{{LeaseID: "leased-failure", Issue: 7, RunID: "leased-failure"}}}
	encoded, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, exit := runAcknowledgeMain(repository, stateDir, io.Discard, "issue-42", "leased-failure")
	if exit != 1 || stdout != "" || !strings.Contains(stderr, "retains a Lease") {
		t.Fatalf("pre-migration atomic refusal exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	stillV2, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stillV2, encoded) {
		t.Fatalf("invalid selector persisted migration or acknowledgment before validation: %s", stillV2)
	}

	stdout, stderr, exit = runAcknowledgeMain(repository, stateDir, io.Discard, "42")
	if exit != 0 || stderr != "" || !strings.Contains(stdout, "42 (issue #1)") || strings.Contains(stdout, "issue-42") {
		t.Fatalf("numeric exact acknowledgment exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	migrated := loadAcknowledgeState(t, statePath)
	if migrated.Version != state.CurrentVersion || migrated.Runs[0].AcknowledgedAt == nil || migrated.Runs[1].AcknowledgedAt != nil {
		t.Fatalf("numeric selector or migration state = %#v", migrated)
	}
	firstTimestamp := *migrated.Runs[0].AcknowledgedAt

	stdout, stderr, exit = runAcknowledgeMain(repository, stateDir, io.Discard, "10", "exact-20")
	if exit != 0 || stderr != "" {
		t.Fatalf("multiple selector acknowledgment exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	for _, runID := range []string{"issue-10-old", "issue-10-new", "exact-20"} {
		if !strings.Contains(stdout, runID) {
			t.Fatalf("multiple selector output omitted %q: %s", runID, stdout)
		}
	}
	current := loadAcknowledgeState(t, statePath)
	var atomicTimestamp *time.Time
	for _, runID := range []string{"issue-10-old", "issue-10-new", "exact-20"} {
		acknowledged := findAcknowledgmentRun(t, current, runID).AcknowledgedAt
		if acknowledged == nil {
			t.Fatalf("Run %q was not acknowledged", runID)
		}
		if atomicTimestamp == nil {
			atomicTimestamp = acknowledged
		} else if !acknowledged.Equal(*atomicTimestamp) {
			t.Fatalf("one atomic acknowledgment used different timestamps: %v and %v", acknowledged, atomicTimestamp)
		}
	}

	stdout, stderr, exit = runAcknowledgeMain(repository, stateDir, io.Discard, "42")
	if exit != 0 || stderr != "" || !strings.Contains(stdout, "No additional") || !strings.Contains(stdout, "Already acknowledged") || !strings.Contains(stdout, "42 (issue #1)") {
		t.Fatalf("repeated acknowledgment exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	if got := findAcknowledgmentRun(t, loadAcknowledgeState(t, statePath), "42").AcknowledgedAt; got == nil || !got.Equal(firstTimestamp) {
		t.Fatalf("idempotent timestamp = %v, want %v", got, firstTimestamp)
	}

	beforeRefusal, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, exit = runAcknowledgeMain(repository, stateDir, io.Discard, "issue-42", "leased-failure")
	if exit != 1 || stdout != "" || !strings.Contains(stderr, "retains a Lease") {
		t.Fatalf("atomic refusal exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	afterRefusal, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeRefusal, afterRefusal) || findAcknowledgmentRun(t, loadAcknowledgeState(t, statePath), "issue-42").AcknowledgedAt != nil {
		t.Fatal("ineligible exact selector partially acknowledged another selection")
	}

	stdout, stderr, exit = runAcknowledgeMain(repository, stateDir, io.Discard, "7")
	if exit != 0 || stderr != "" || !strings.Contains(stdout, "No eligible") {
		t.Fatalf("issue selector with only a current Lease exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}

	stdout, stderr, exit = runAcknowledgeMain(repository, stateDir, io.Discard, "--all")
	if exit != 0 || stderr != "" || !strings.Contains(stdout, "issue-42") || strings.Contains(stdout, "leased-failure") || strings.Contains(stdout, "merged") {
		t.Fatalf("acknowledge all exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	current = loadAcknowledgeState(t, statePath)
	if findAcknowledgmentRun(t, current, "issue-42").AcknowledgedAt == nil || findAcknowledgmentRun(t, current, "leased-failure").AcknowledgedAt != nil {
		t.Fatalf("acknowledge --all state = %#v", current.Runs)
	}
	concise := runStatusCommand(t, repository, stateDir)
	if strings.Contains(concise, "Run: issue-42 | State:") || strings.Contains(concise, "Run: reset | State:") || !strings.Contains(concise, "Run: merged | State:") {
		t.Fatalf("post-migration concise status projection = %q", concise)
	}
	var fullOutput, fullErrors bytes.Buffer
	if exit := Main(context.Background(), []string{"status", "--all", "--repo-dir", repository, "--state-dir", stateDir}, &fullOutput, &fullErrors); exit != 0 {
		t.Fatalf("post-migration status --all exit=%d stderr=%q", exit, fullErrors.String())
	}
	for _, runID := range []string{"issue-42", "reset", "merged", "leased-failure"} {
		if !strings.Contains(fullOutput.String(), "Run: "+runID+" | State:") {
			t.Fatalf("post-migration full status omitted %q: %s", runID, fullOutput.String())
		}
	}
	if !strings.Contains(fullOutput.String(), "Run: issue-42 | State: failed\n    Acknowledged:") {
		t.Fatalf("full status omitted acknowledgment timestamp: %s", fullOutput.String())
	}

	current.Runs = append(current.Runs, scheduler.Run{Issue: 50, RunID: "future-failure", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, UpdatedAt: time.Now().Add(time.Hour)})
	if err := (state.FileStore{Path: statePath}).Save(current); err != nil {
		t.Fatal(err)
	}
	futureStatus := runStatusCommand(t, repository, stateDir)
	if !strings.Contains(futureStatus, "Run: future-failure | State: failed") {
		t.Fatalf("future failure was hidden by an earlier acknowledge --all snapshot: %s", futureStatus)
	}
}

func TestAcknowledgeAcceptsFlagsBeforeAndBetweenSelectors(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 1, RunID: "first", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 2, RunID: "second", Status: scheduler.StatusNeedsHuman, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 3, RunID: "--help", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
	}}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	exit := Main(context.Background(), []string{
		"acknowledge", "--repo-dir", repository, "first", "--state-dir", stateDir, "second",
	}, &stdout, &stderr)
	if exit != 0 || !strings.Contains(stdout.String(), "first (issue #1)") || !strings.Contains(stdout.String(), "second (issue #2)") {
		t.Fatalf("interspersed flags exit=%d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	exit = Main(context.Background(), []string{
		"acknowledge", "--repo-dir", repository, "--state-dir", stateDir, "--", "--help",
	}, &stdout, &stderr)
	if exit != 0 || stderr.String() != "" || !strings.Contains(stdout.String(), "--help (issue #3)") ||
		findAcknowledgmentRun(t, loadAcknowledgeState(t, store.Path), "--help").AcknowledgedAt == nil {
		t.Fatalf("flag-like exact selector exit=%d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
	}
}

func TestAcknowledgeAllSucceedsWithoutEligibleOutcomesOrStateMutation(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 1, RunID: "merged", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 2, RunID: "reset", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint},
	}}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	stdout, stderr, exit := runAcknowledgeMain(repository, stateDir, io.Discard, "--all")
	if exit != 0 || stderr != "" || !strings.Contains(stdout, "No eligible Historical Run outcomes") {
		t.Fatalf("empty acknowledge --all exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	after, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("empty acknowledge --all changed state")
	}
}

func TestAcknowledgeRejectsInvalidInvocationWithoutChangingState(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{
		{Issue: 1, RunID: "eligible", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 2, RunID: "merged", Status: scheduler.StatusMerged, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 3, RunID: "reset", Status: scheduler.StatusReset, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 4, RunID: "active", Status: scheduler.StatusClaimed, WorkerMode: scheduler.WorkerModePrint},
		{Issue: 5, RunID: "resetting", Status: scheduler.StatusResetting, WorkerMode: scheduler.WorkerModePrint},
	}, Leases: []scheduler.Lease{
		{LeaseID: "active", Issue: 4, RunID: "active"}, {LeaseID: "resetting", Issue: 5, RunID: "resetting"},
	}}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(store.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing selectors", want: "usage: backlog acknowledge"},
		{name: "all with selector", args: []string{"--all", "eligible"}, want: "cannot be combined"},
		{name: "unknown exact", args: []string{"unknown"}, want: `Run "unknown" was not found`},
		{name: "unknown issue", args: []string{"99"}, want: "issue #99 has no Run history"},
		{name: "Completion is ineligible", args: []string{"merged"}, want: "not eligible"},
		{name: "Reset is ineligible", args: []string{"reset"}, want: "not eligible"},
		{name: "Active Run is ineligible", args: []string{"active"}, want: "retains a Lease"},
		{name: "resetting Run is ineligible", args: []string{"resetting"}, want: "retains a Lease"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stdout, stderr, exit := runAcknowledgeMain(repository, stateDir, io.Discard, test.args...)
			if exit != 1 || stdout != "" || !strings.Contains(stderr, test.want) {
				t.Fatalf("exit=%d stdout=%q stderr=%q, want %q", exit, stdout, stderr, test.want)
			}
			after, err := os.ReadFile(store.Path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("invalid acknowledgment changed state")
			}
		})
	}
}

func TestAcknowledgeRefusesRepositoryLockContention(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 1, RunID: "eligible", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint,
	}}}); err != nil {
		t.Fatal(err)
	}
	lock, err := state.AcquireLock(filepath.Join(repository, ".git", legacyLockFile))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	stdout, stderr, exit := runAcknowledgeMain(repository, stateDir, io.Discard, "eligible")
	if exit != 1 || stdout != "" || !strings.Contains(stderr, "runner already active") {
		t.Fatalf("lock refusal exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	if findAcknowledgmentRun(t, loadAcknowledgeState(t, store.Path), "eligible").AcknowledgedAt != nil {
		t.Fatal("lock refusal changed acknowledgment metadata")
	}
}

func TestAcknowledgePersistenceFailureDoesNotReportOrPersistSuccess(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 1, RunID: "eligible", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint,
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stateDir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(stateDir, 0o700)
	stdout, stderr, exit := runAcknowledgeMain(repository, stateDir, io.Discard, "eligible")
	if exit != 1 || stdout != "" || !strings.Contains(stderr, "persist state for Outcome Acknowledgment") {
		t.Fatalf("persistence failure exit=%d stdout=%q stderr=%q", exit, stdout, stderr)
	}
	if findAcknowledgmentRun(t, loadAcknowledgeState(t, store.Path), "eligible").AcknowledgedAt != nil {
		t.Fatal("persistence failure changed acknowledgment metadata")
	}
}

func TestAcknowledgeOutputFailureReturnsFailureAfterDurableUpdate(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{{
		Issue: 1, RunID: "eligible", Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint,
	}}}); err != nil {
		t.Fatal(err)
	}
	_, stderr, exit := runAcknowledgeMain(repository, stateDir, failingAcknowledgeWriter{}, "eligible")
	if exit != 1 || !strings.Contains(stderr, "acknowledgment output failed") {
		t.Fatalf("output failure exit=%d stderr=%q", exit, stderr)
	}
	if findAcknowledgmentRun(t, loadAcknowledgeState(t, store.Path), "eligible").AcknowledgedAt == nil {
		t.Fatal("successful atomic update was lost after output failure")
	}
}

func TestAcknowledgePreservesMetadataAndFollowModesForHistoricalRun(t *testing.T) {
	repository := initializeFollowRepository(t)
	stateDir := t.TempDir()
	logPath := filepath.Join(stateDir, "acknowledged.jsonl")
	stderrPath := filepath.Join(stateDir, "acknowledged.stderr.log")
	if err := os.WriteFile(logPath, []byte("raw record\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stderrPath, []byte("diagnostic record\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, 7, 2, 3, 4, 5, 0, time.UTC)
	original := scheduler.Run{
		Issue: 5, IssueTitle: "Preserve evidence", IssueURL: "https://example.test/issues/5", RunID: "acknowledged-follow",
		Status: scheduler.StatusFailed, WorkerMode: scheduler.WorkerModePrint, Branch: "agent/issue-5", Worktree: "/worktrees/issue-5",
		SessionName: "afk #5", LogPath: logPath, StderrPath: stderrPath, PullRequest: "https://example.test/pull/5",
		Error: "preserved diagnostic", StartedAt: startedAt, WorkerStartedAt: startedAt.Add(time.Minute), UpdatedAt: startedAt.Add(2 * time.Minute),
	}
	store := state.FileStore{Path: filepath.Join(stateDir, "state.json")}
	if err := store.Save(state.State{Version: state.CurrentVersion, Runs: []scheduler.Run{original}}); err != nil {
		t.Fatal(err)
	}
	acknowledgeOutput, acknowledgeErrors, exit := runAcknowledgeMain(repository, stateDir, io.Discard, original.RunID)
	if exit != 0 || acknowledgeErrors != "" || !strings.Contains(acknowledgeOutput, original.RunID) {
		t.Fatalf("acknowledge exit=%d stdout=%q stderr=%q", exit, acknowledgeOutput, acknowledgeErrors)
	}
	persisted := findAcknowledgmentRun(t, loadAcknowledgeState(t, store.Path), original.RunID)
	if persisted.AcknowledgedAt == nil {
		t.Fatal("acknowledgment timestamp was not persisted")
	}
	expected := original
	expected.AcknowledgedAt = persisted.AcknowledgedAt
	if !reflect.DeepEqual(persisted, expected) {
		t.Fatalf("acknowledgment changed Run metadata:\n got %#v\nwant %#v", persisted, expected)
	}
	for path, want := range map[string]string{logPath: "raw record\n", stderrPath: "diagnostic record\n"} {
		contents, err := os.ReadFile(path)
		if err != nil || string(contents) != want {
			t.Fatalf("retained artifact %q = %q, %v", path, contents, err)
		}
	}

	var stdout, stderr bytes.Buffer
	exit = Main(context.Background(), []string{"follow", "acknowledged-follow", "--repo-dir", repository, "--state-dir", stateDir}, &stdout, &stderr)
	if exit != 0 || !strings.Contains(stdout.String(), "Run: acknowledged-follow") || !strings.Contains(stdout.String(), "State: failed") {
		t.Fatalf("Follow acknowledged Run exit=%d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	exit = Main(context.Background(), []string{"follow", "acknowledged-follow", "--raw", "--repo-dir", repository, "--state-dir", stateDir}, &stdout, &stderr)
	if exit != 0 || stdout.String() != "raw record\n" || !strings.Contains(stderr.String(), "Run: acknowledged-follow") {
		t.Fatalf("raw Follow acknowledged Run exit=%d stdout=%q stderr=%q", exit, stdout.String(), stderr.String())
	}
}

func runAcknowledgeMain(repository, stateDir string, output io.Writer, args ...string) (string, string, int) {
	commandArgs := append([]string{"acknowledge"}, args...)
	commandArgs = append(commandArgs, "--repo-dir", repository, "--state-dir", stateDir)
	var stdout, stderr bytes.Buffer
	if output == nil || output == io.Discard {
		output = &stdout
	}
	exit := Main(context.Background(), commandArgs, output, &stderr)
	return stdout.String(), stderr.String(), exit
}

func loadAcknowledgeState(t *testing.T, path string) state.State {
	t.Helper()
	current, err := (state.FileStore{Path: path}).Load()
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func findAcknowledgmentRun(t *testing.T, current state.State, runID string) scheduler.Run {
	t.Helper()
	for _, run := range current.Runs {
		if run.RunID == runID {
			return run
		}
	}
	t.Fatalf("Run %q not found", runID)
	return scheduler.Run{}
}

type failingAcknowledgeWriter struct{}

func (failingAcknowledgeWriter) Write([]byte) (int, error) {
	return 0, errors.New("acknowledgment output failed")
}
