package herdr

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
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
	if working.ID != fmt.Sprintf("custom:backlog:%d", working.Params.Seq) || release.ID != fmt.Sprintf("custom:backlog:%d", release.Params.Seq) || working.ID == release.ID {
		t.Fatalf("request ids = %q/%q, sequences = %d/%d", working.ID, release.ID, working.Params.Seq, release.Params.Seq)
	}
}

func TestReporterRequiresCompleteHerdrEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		enabled bool
	}{
		{name: "missing Herdr marker", values: map[string]string{"HERDR_SOCKET_PATH": "/tmp/herdr.sock", "HERDR_PANE_ID": "w1:p1"}},
		{name: "wrong Herdr marker", values: map[string]string{"HERDR_ENV": "true", "HERDR_SOCKET_PATH": "/tmp/herdr.sock", "HERDR_PANE_ID": "w1:p1"}},
		{name: "missing socket", values: map[string]string{"HERDR_ENV": "1", "HERDR_PANE_ID": "w1:p1"}},
		{name: "missing pane", values: map[string]string{"HERDR_ENV": "1", "HERDR_SOCKET_PATH": "/tmp/herdr.sock"}},
		{name: "complete", values: map[string]string{"HERDR_ENV": "1", "HERDR_SOCKET_PATH": "/tmp/herdr.sock", "HERDR_PANE_ID": "w1:p1"}, enabled: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reporter := fromEnvironment(func(name string) string { return test.values[name] })
			if reporter.Enabled() != test.enabled {
				t.Fatalf("Enabled = %t, want %t", reporter.Enabled(), test.enabled)
			}
		})
	}
}

func TestReporterRejectsInvalidResponses(t *testing.T) {
	tests := []struct {
		name      string
		response  func(socketRequest) string
		wantError string
	}{
		{
			name: "mismatched request id",
			response: func(socketRequest) string {
				return `{"id":"other","result":{"type":"ok"}}` + "\n"
			},
			wantError: "does not match request id",
		},
		{
			name: "explicit error",
			response: func(request socketRequest) string {
				return fmt.Sprintf(`{"id":%q,"error":{"code":"invalid_params"}}`+"\n", request.ID)
			},
			wantError: "Herdr rejected request",
		},
		{
			name: "missing result",
			response: func(request socketRequest) string {
				return fmt.Sprintf(`{"id":%q}`+"\n", request.ID)
			},
			wantError: "has no result",
		},
		{
			name:      "malformed JSON",
			response:  func(socketRequest) string { return "{\n" },
			wantError: "read Herdr response",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			socketPath, serverDone := serveOneHerdrResponse(t, test.response)
			reporter := newReporter(socketPath, "w1:p1", time.Second)
			err := reporter.Working("scheduling Runs")
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Working error = %v, want containing %q", err, test.wantError)
			}
			if err := <-serverDone; err != nil {
				t.Fatalf("fake Herdr server: %v", err)
			}
		})
	}
}

func TestReporterReturnsConnectionFailure(t *testing.T) {
	reporter := newReporter(filepath.Join(t.TempDir(), "missing.sock"), "w1:p1", 20*time.Millisecond)
	if err := reporter.Working("scheduling Runs"); err == nil || !strings.Contains(err.Error(), "connect to Herdr") {
		t.Fatalf("Working error = %v", err)
	}
}

func TestReporterTimesOutWhenServerDoesNotRespond(t *testing.T) {
	socketPath, _ := serveOneHerdrResponse(t, func(request socketRequest) string {
		time.Sleep(200 * time.Millisecond)
		return fmt.Sprintf(`{"id":%q,"result":{"type":"ok"}}`+"\n", request.ID)
	})
	reporter := newReporter(socketPath, "w1:p1", 20*time.Millisecond)
	started := time.Now()
	err := reporter.Working("scheduling Runs")
	if err == nil || !strings.Contains(err.Error(), "read Herdr response") {
		t.Fatalf("Working error = %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 150*time.Millisecond {
		t.Fatalf("Working took %s, want bounded by the socket deadline", elapsed)
	}
}

func serveOneHerdrResponse(t *testing.T, response func(socketRequest) string) (string, <-chan error) {
	t.Helper()
	socketDir, err := os.MkdirTemp("", "herdr-response-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "herdr.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })

	done := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer connection.Close()
		var request socketRequest
		if err := json.NewDecoder(connection).Decode(&request); err != nil {
			done <- err
			return
		}
		_, err = connection.Write([]byte(response(request)))
		done <- err
	}()
	return socketPath, done
}
