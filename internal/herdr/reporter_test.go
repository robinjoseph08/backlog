package herdr

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReporterIsDisabledOutsideHerdr(t *testing.T) {
	reporter := fromEnvironment(func(string) string { return "" })

	if reporter.Enabled() {
		t.Fatal("reporter is enabled without Herdr environment")
	}
	if err := reporter.Working("draining acme/widgets"); err != nil {
		t.Fatalf("disabled Working returned an error: %v", err)
	}
	if err := reporter.Release(); err != nil {
		t.Fatalf("disabled Release returned an error: %v", err)
	}
}

func TestReporterReportsBacklogLifecycle(t *testing.T) {
	socketDir, err := os.MkdirTemp("", "herdr-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "herdr.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	requests := make(chan socketRequest, 2)
	serverErrors := make(chan error, 1)
	go func() {
		for range 2 {
			connection, err := listener.Accept()
			if err != nil {
				serverErrors <- err
				return
			}
			var request socketRequest
			if err := json.NewDecoder(connection).Decode(&request); err != nil {
				connection.Close()
				serverErrors <- err
				return
			}
			requests <- request
			if err := json.NewEncoder(connection).Encode(socketResponse{ID: request.ID, Result: json.RawMessage(`{"type":"ok"}`)}); err != nil {
				connection.Close()
				serverErrors <- err
				return
			}
			if err := connection.Close(); err != nil {
				serverErrors <- err
				return
			}
		}
		serverErrors <- nil
	}()

	reporter := newReporter(socketPath, "w1:p1", time.Second)
	if err := reporter.Working("draining acme/widgets"); err != nil {
		t.Fatalf("report working: %v", err)
	}
	if err := reporter.Release(); err != nil {
		t.Fatalf("release agent: %v", err)
	}
	if err := <-serverErrors; err != nil {
		t.Fatalf("fake Herdr server: %v", err)
	}

	working := <-requests
	if working.Method != "pane.report_agent" {
		t.Fatalf("working method = %q", working.Method)
	}
	if working.Params.PaneID != "w1:p1" || working.Params.Source != "custom:backlog" || working.Params.Agent != "backlog" {
		t.Fatalf("working identity = %#v", working.Params)
	}
	if working.Params.State != "working" || working.Params.Message != "draining acme/widgets" {
		t.Fatalf("working state = %#v", working.Params)
	}

	release := <-requests
	if release.Method != "pane.release_agent" {
		t.Fatalf("release method = %q", release.Method)
	}
	if release.Params.PaneID != "w1:p1" || release.Params.Source != "custom:backlog" || release.Params.Agent != "backlog" {
		t.Fatalf("release identity = %#v", release.Params)
	}
	if release.Params.Seq <= working.Params.Seq {
		t.Fatalf("release sequence = %d, want greater than working sequence %d", release.Params.Seq, working.Params.Seq)
	}
}

func TestReporterRequiresCompleteHerdrEnvironment(t *testing.T) {
	values := map[string]string{
		"HERDR_ENV":         "1",
		"HERDR_SOCKET_PATH": "/tmp/herdr.sock",
	}
	reporter := fromEnvironment(func(name string) string { return values[name] })

	if reporter.Enabled() {
		t.Fatal("reporter is enabled without HERDR_PANE_ID")
	}
}
