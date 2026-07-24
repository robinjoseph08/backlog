package worktree

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestManagerPreparesUniqueWorktreeFromLatestRemoteBase(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logPath := filepath.Join(root, "git.log")
	git := fakeGit(t, logPath)
	manager := Manager{
		GitExecutable: git,
		RepositoryDir: filepath.Join(root, "repo"),
		WorktreesDir:  filepath.Join(root, "worktrees"),
		DefaultBranch: "trunk",
	}

	got, err := manager.Plan(42, "20260702-abc")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := manager.Prepare(context.Background(), got); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got.Branch != "agent/issue-42-20260702-abc" {
		t.Fatalf("branch = %q", got.Branch)
	}
	if got.Path != filepath.Join(root, "worktrees", "issue-42-20260702-abc") {
		t.Fatalf("path = %q", got.Path)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	log := string(logData)
	want := []string{
		"-C " + manager.RepositoryDir + " fetch origin trunk",
		"-C " + manager.RepositoryDir + " worktree add -b " + got.Branch + " " + got.Path + " origin/trunk",
	}
	for _, command := range want {
		if !strings.Contains(log, command+"\n") {
			t.Fatalf("git log %q does not contain %q", log, command)
		}
	}
}

func TestManagerRetriesTransientFetchFailures(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logPath := filepath.Join(root, "git.log")
	attemptPath := filepath.Join(root, "fetch-attempts")
	gitPath := filepath.Join(root, "git")
	script := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> ` + shellQuote(logPath) + `
if [ "$3" = "fetch" ]; then
	attempts=0
	if [ -f ` + shellQuote(attemptPath) + ` ]; then
		attempts=$(cat ` + shellQuote(attemptPath) + `)
	fi
	attempts=$((attempts+1))
	printf '%s\n' "$attempts" > ` + shellQuote(attemptPath) + `
	if [ "$attempts" -lt 3 ]; then
		echo 'git@github.com: Permission denied (publickey).' >&2
		exit 128
	fi
fi
`
	if err := os.WriteFile(gitPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	var delays []time.Duration
	manager := Manager{
		GitExecutable: gitPath,
		RepositoryDir: filepath.Join(root, "repo"),
		WorktreesDir:  filepath.Join(root, "worktrees"),
		DefaultBranch: "trunk",
		fetchRetryWait: func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	}
	assignment, err := manager.Plan(10, "transient-fetch")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	if err := manager.Prepare(context.Background(), assignment); err != nil {
		t.Fatalf("prepare after transient publickey failures: %v", err)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	log := string(logData)
	fetch := "-C " + manager.RepositoryDir + " fetch origin trunk\n"
	if got := strings.Count(log, fetch); got != 3 {
		t.Fatalf("fetch attempts = %d, want 3; log = %q", got, log)
	}
	if want := []time.Duration{time.Second, 2 * time.Second}; !reflect.DeepEqual(delays, want) {
		t.Fatalf("fetch retry delays = %v, want %v", delays, want)
	}
	worktreeAdd := "-C " + manager.RepositoryDir + " worktree add -b " + assignment.Branch + " " + assignment.Path + " origin/trunk\n"
	if got := strings.Count(log, worktreeAdd); got != 1 {
		t.Fatalf("worktree add attempts = %d, want 1; log = %q", got, log)
	}
}

func TestManagerBoundsFetchAttempts(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logPath := filepath.Join(root, "git.log")
	manager := Manager{
		GitExecutable:  failingGit(t, logPath),
		RepositoryDir:  filepath.Join(root, "repo"),
		WorktreesDir:   filepath.Join(root, "worktrees"),
		DefaultBranch:  "trunk",
		fetchRetryWait: func(context.Context, time.Duration) error { return nil },
	}
	assignment, err := manager.Plan(10, "persistent-fetch-failure")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	err = manager.Prepare(context.Background(), assignment)
	if err == nil || !strings.Contains(err.Error(), "fetch base branch: after 3 attempts") ||
		!strings.Contains(err.Error(), "git attempt 3 failed: Permission denied (publickey)") {
		t.Fatalf("prepare error = %v, want bounded fetch attempt error with final git failure", err)
	}
	logData, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	fetch := "-C " + manager.RepositoryDir + " fetch origin trunk\n"
	if got := strings.Count(string(logData), fetch); got != fetchMaxAttempts {
		t.Fatalf("fetch attempts = %d, want %d; log = %q", got, fetchMaxAttempts, logData)
	}
	if strings.Contains(string(logData), " worktree add ") {
		t.Fatalf("worktree creation followed failed fetches; log = %q", logData)
	}
}

func TestManagerCancelsFetchBackoff(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logPath := filepath.Join(root, "git.log")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := Manager{
		GitExecutable: failingGit(t, logPath),
		RepositoryDir: filepath.Join(root, "repo"),
		WorktreesDir:  filepath.Join(root, "worktrees"),
		DefaultBranch: "trunk",
		fetchRetryWait: func(waitCtx context.Context, _ time.Duration) error {
			cancel()
			return waitForFetchAttempt(waitCtx, time.Hour)
		},
	}
	assignment, err := manager.Plan(10, "canceled-fetch")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	err = manager.Prepare(ctx, assignment)
	if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("prepare error = %v, want context cancellation", err)
	}
	logData, readErr := os.ReadFile(logPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	fetch := "-C " + manager.RepositoryDir + " fetch origin trunk\n"
	if got := strings.Count(string(logData), fetch); got != 1 {
		t.Fatalf("fetch attempts after cancellation = %d, want 1; log = %q", got, logData)
	}
}

func TestWaitForFetchAttemptStopsAfterBackoffBegins(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Millisecond, cancel)
	done := make(chan error, 1)
	go func() { done <- waitForFetchAttempt(ctx, time.Hour) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("active fetch backoff did not stop after cancellation")
	}
}

func TestDefaultFetchRetryDelay(t *testing.T) {
	t.Parallel()

	if got := defaultFetchRetryDelay(1); got != time.Second {
		t.Fatalf("delay after attempt 1 = %s, want 1s", got)
	}
	if got := defaultFetchRetryDelay(2); got != 2*time.Second {
		t.Fatalf("delay after attempt 2 = %s, want 2s", got)
	}
}

func TestManagerCleansMergedWorktreeAndBranch(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logPath := filepath.Join(root, "git.log")
	git := fakeGit(t, logPath)
	manager := Manager{GitExecutable: git, RepositoryDir: filepath.Join(root, "repo")}
	worktreePath := filepath.Join(root, "worktree")
	if err := os.Mkdir(worktreePath, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := manager.Cleanup(context.Background(), Assignment{Path: worktreePath, Branch: "agent/issue-42-run"}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	logData, _ := os.ReadFile(logPath)
	log := string(logData)
	for _, command := range []string{
		"-C " + manager.RepositoryDir + " worktree remove --force " + worktreePath,
		"-C " + manager.RepositoryDir + " worktree prune",
		"-C " + manager.RepositoryDir + " branch -D agent/issue-42-run",
	} {
		if !strings.Contains(log, command+"\n") {
			t.Fatalf("git log %q does not contain %q", log, command)
		}
	}
}

func fakeGit(t *testing.T, logPath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "git")
	script := "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$*\" >> " + shellQuote(logPath) + "\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func failingGit(t *testing.T, logPath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "git")
	attemptPath := logPath + ".attempts"
	script := "#!/bin/sh\nset -eu\nprintf '%s\\n' \"$*\" >> " + shellQuote(logPath) + "\nattempts=0\nif [ -f " + shellQuote(attemptPath) + " ]; then attempts=$(cat " + shellQuote(attemptPath) + "); fi\nattempts=$((attempts+1))\nprintf '%s\\n' \"$attempts\" > " + shellQuote(attemptPath) + "\necho \"git attempt $attempts failed: Permission denied (publickey).\" >&2\nexit 128\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
