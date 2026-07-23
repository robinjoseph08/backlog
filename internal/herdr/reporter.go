package herdr

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

const (
	source         = "custom:backlog"
	agent          = "backlog"
	defaultTimeout = 500 * time.Millisecond
)

// Reporter publishes Backlog's foreground lifecycle to the Herdr pane that
// launched it. A disabled Reporter is a no-op so Herdr remains optional.
type Reporter struct {
	socketPath string
	paneID     string
	timeout    time.Duration

	mu  sync.Mutex
	seq uint64
}

type socketRequest struct {
	ID     string        `json:"id"`
	Method string        `json:"method"`
	Params requestParams `json:"params"`
}

type requestParams struct {
	PaneID  string `json:"pane_id"`
	Source  string `json:"source"`
	Agent   string `json:"agent"`
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`
	Seq     uint64 `json:"seq"`
}

type socketResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

// FromEnvironment creates a reporter from the pane environment Herdr injects.
func FromEnvironment() *Reporter {
	return fromEnvironment(os.Getenv)
}

func fromEnvironment(getenv func(string) string) *Reporter {
	if getenv("HERDR_ENV") != "1" {
		return &Reporter{}
	}
	socketPath := getenv("HERDR_SOCKET_PATH")
	paneID := getenv("HERDR_PANE_ID")
	if socketPath == "" || paneID == "" {
		return &Reporter{}
	}
	return newReporter(socketPath, paneID, defaultTimeout)
}

func newReporter(socketPath, paneID string, timeout time.Duration) *Reporter {
	return &Reporter{socketPath: socketPath, paneID: paneID, timeout: timeout}
}

// Enabled reports whether Backlog inherited a complete Herdr pane environment.
func (r *Reporter) Enabled() bool {
	return r != nil && r.socketPath != "" && r.paneID != ""
}

// Working makes the Backlog runner visible as a working custom agent.
func (r *Reporter) Working(message string) error {
	if !r.Enabled() {
		return nil
	}
	return r.send("pane.report_agent", "working", message)
}

// Release removes Backlog's custom agent authority when the runner exits.
func (r *Reporter) Release() error {
	if !r.Enabled() {
		return nil
	}
	return r.send("pane.release_agent", "", "")
}

func (r *Reporter) send(method, state, message string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	sequence := uint64(time.Now().UnixMicro())
	if sequence <= r.seq {
		sequence = r.seq + 1
	}
	r.seq = sequence
	id := fmt.Sprintf("%s:%d", source, sequence)
	request := socketRequest{
		ID:     id,
		Method: method,
		Params: requestParams{
			PaneID:  r.paneID,
			Source:  source,
			Agent:   agent,
			State:   state,
			Message: message,
			Seq:     sequence,
		},
	}

	connection, err := net.DialTimeout("unix", r.socketPath, r.timeout)
	if err != nil {
		return fmt.Errorf("connect to Herdr: %w", err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(r.timeout)); err != nil {
		return fmt.Errorf("set Herdr socket deadline: %w", err)
	}
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return fmt.Errorf("send Herdr request: %w", err)
	}

	var response socketResponse
	if err := json.NewDecoder(connection).Decode(&response); err != nil {
		return fmt.Errorf("read Herdr response: %w", err)
	}
	if response.ID != id {
		return fmt.Errorf("Herdr response id %q does not match request id %q", response.ID, id)
	}
	if len(response.Error) > 0 && string(response.Error) != "null" {
		return fmt.Errorf("Herdr rejected request: %s", response.Error)
	}
	if len(response.Result) == 0 {
		return errors.New("Herdr response has no result")
	}
	return nil
}
