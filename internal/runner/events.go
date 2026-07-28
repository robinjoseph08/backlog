package runner

import (
	"fmt"
	"sync"
	"time"
)

// OperationalEvent reports transient Runner health and lifecycle changes to a
// presentation adapter. Events are invocation-local and are not durable state.
type OperationalEvent interface {
	operationalEvent()
}

// CandidateDiscoveryOperation identifies the Candidate discovery operation
// that failed without requiring presentation code to classify error prose.
type CandidateDiscoveryOperation string

const (
	CandidateDiscoverySnapshot CandidateDiscoveryOperation = "list and inspect Candidates"
	CandidateDiscoveryList     CandidateDiscoveryOperation = "list candidates"
	CandidateDiscoveryInspect  CandidateDiscoveryOperation = "inspect candidate"
)

// CandidateDiscoveryFailed reports that Admission cannot use an incomplete
// Candidate snapshot. Issue is nil when the failed operation was not scoped to
// one Candidate. Err presents the original discovery error through the
// invocation's bounded Diagnostics retention. Cause is a concise terminal
// cause supplied independently of the retry policy. FirstFailureAt preserves
// the start of the current degradation episode if delivery is coalesced by a
// bounded presentation queue.
type CandidateDiscoveryFailed struct {
	Operation           CandidateDiscoveryOperation
	Issue               *int
	Err                 error
	Cause               string
	FirstFailureAt      time.Time
	OccurredAt          time.Time
	RetryAt             time.Time
	ConsecutiveFailures int
}

func (CandidateDiscoveryFailed) operationalEvent() {}

const candidateDiscoveryDiagnosticLimit = 20

type candidateDiscoveryDiagnostics struct {
	mu      sync.Mutex
	nextID  uint64
	records map[uint64]error
	order   []uint64
}

func (d *candidateDiscoveryDiagnostics) retain(err error) error {
	if err == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.records == nil {
		d.records = make(map[uint64]error, candidateDiscoveryDiagnosticLimit)
	}
	d.nextID++
	id := d.nextID
	d.records[id] = err
	d.order = append(d.order, id)
	if len(d.order) > candidateDiscoveryDiagnosticLimit {
		delete(d.records, d.order[0])
		d.order = d.order[1:]
	}
	return retainedCandidateDiscoveryError{diagnostics: d, id: id}
}

func (d *candidateDiscoveryDiagnostics) lookup(id uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.records[id]
}

func (d *candidateDiscoveryDiagnostics) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.records)
}

type retainedCandidateDiscoveryError struct {
	diagnostics *candidateDiscoveryDiagnostics
	id          uint64
}

func (e retainedCandidateDiscoveryError) Error() string {
	if err := e.diagnostics.lookup(e.id); err != nil {
		return err.Error()
	}
	return "full Candidate discovery diagnostic is no longer retained"
}

func (e retainedCandidateDiscoveryError) Unwrap() error {
	return e.diagnostics.lookup(e.id)
}

// CandidateDiscoveryRecovered reports that a complete Candidate snapshot made
// Admission healthy again after one or more consecutive failures.
type CandidateDiscoveryRecovered struct {
	OccurredAt time.Time
	Failures   int
}

func (CandidateDiscoveryRecovered) operationalEvent() {}

// ShutdownStage identifies the current stage of the staged shutdown lifecycle.
type ShutdownStage string

const (
	ShutdownStageDraining             ShutdownStage = "draining"
	ShutdownStageSuspending           ShutdownStage = "suspending"
	ShutdownStageForceStopping        ShutdownStage = "force-stopping"
	ShutdownStageDrainComplete        ShutdownStage = "drain-complete"
	ShutdownStageDrainIncomplete      ShutdownStage = "drain-incomplete"
	ShutdownStageSuspensionComplete   ShutdownStage = "suspension-complete"
	ShutdownStageSuspensionIncomplete ShutdownStage = "suspension-incomplete"
)

// NextInterruptBehavior identifies what another SIGINT does in the current
// shutdown stage.
type NextInterruptBehavior string

const (
	NextInterruptSuspends         NextInterruptBehavior = "request-suspension"
	NextInterruptForceStops       NextInterruptBehavior = "request-force-stop"
	NextInterruptRepeatsForceStop NextInterruptBehavior = "repeat-force-stop"
	NextInterruptNone             NextInterruptBehavior = "none"
)

// ShutdownEvent reports a staged shutdown transition or progress update.
// RemainingWorkers counts live Workers still supervised by this Runner
// invocation. Message preserves the complete append-only plain rendering while
// typed fields let terminal presentation avoid deriving lifecycle state from
// that prose.
type ShutdownEvent struct {
	Stage            ShutdownStage
	Action           string
	RemainingWorkers int
	NextInterrupt    NextInterruptBehavior
	Message          string
}

func (ShutdownEvent) operationalEvent() {}

// FormatOperationalEvent returns the compatible line-oriented rendering for
// an operational event. CLI plain output sanitizes controls from adapter errors
// after formatting.
func FormatOperationalEvent(event OperationalEvent) string {
	switch event := event.(type) {
	case CandidateDiscoveryFailed:
		return fmt.Sprintf("candidate discovery failed; admission paused; retry due in %s: %v", event.RetryAt.Sub(event.OccurredAt), event.Err)
	case CandidateDiscoveryRecovered:
		noun := "failures"
		if event.Failures == 1 {
			noun = "failure"
		}
		return fmt.Sprintf("candidate discovery recovered; admission resumed after %d %s", event.Failures, noun)
	case ShutdownEvent:
		return event.Message
	default:
		return ""
	}
}
