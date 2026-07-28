package runner

import (
	"errors"
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
// the start of the current degradation episode. Occurrences counts equivalent
// failures represented by this event when bounded delivery queues coalesce
// occurrences; zero means one occurrence.
type CandidateDiscoveryFailed struct {
	Operation           CandidateDiscoveryOperation
	Issue               *int
	Err                 error
	Cause               string
	FirstFailureAt      time.Time
	OccurredAt          time.Time
	RetryAt             time.Time
	ConsecutiveFailures int
	Occurrences         int
}

func (CandidateDiscoveryFailed) operationalEvent() {}

// CandidateSnapshotCompleted reports the first complete Candidate snapshot in
// an invocation without implying that Admission recovered from a failure.
type CandidateSnapshotCompleted struct {
	OccurredAt time.Time
}

func (CandidateSnapshotCompleted) operationalEvent() {}

const candidateDiscoveryDiagnosticLimit = 20

// ErrCandidateDiscoveryDiagnosticExpired marks a full diagnostic reference
// whose bounded invocation-local evidence has been evicted.
var ErrCandidateDiscoveryDiagnosticExpired = errors.New("full Candidate discovery diagnostic is no longer retained")

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
	return ErrCandidateDiscoveryDiagnosticExpired.Error()
}

func (e retainedCandidateDiscoveryError) Unwrap() error {
	if err := e.diagnostics.lookup(e.id); err != nil {
		return err
	}
	return ErrCandidateDiscoveryDiagnosticExpired
}

// SnapshotCandidateDiscoveryDiagnostic resolves one retained diagnostic for a
// presentation cache. The returned error remains valid if later Runner records
// evict its reference, so presentation can sanitize the evidence exactly once.
func SnapshotCandidateDiscoveryDiagnostic(err error) error {
	var retained retainedCandidateDiscoveryError
	if !errors.As(err, &retained) {
		return err
	}
	if resolved := retained.diagnostics.lookup(retained.id); resolved != nil {
		return resolved
	}
	return ErrCandidateDiscoveryDiagnosticExpired
}

// CandidateDiscoveryRecovered reports that a complete Candidate snapshot made
// Admission healthy again after one or more consecutive failures.
type CandidateDiscoveryRecovered struct {
	OccurredAt time.Time
	Failures   int
}

func (CandidateDiscoveryRecovered) operationalEvent() {}

// RunLifecycleStage identifies a Run transition without requiring presentation
// code to classify the compatible plain message.
type RunLifecycleStage string

const (
	RunLifecycleClaimed          RunLifecycleStage = "claimed"
	RunLifecycleStarted          RunLifecycleStage = "started"
	RunLifecycleResumed          RunLifecycleStage = "resumed"
	RunLifecycleCleanupCompleted RunLifecycleStage = "cleanup-completed"
	RunLifecycleMerged           RunLifecycleStage = "merged"
	RunLifecycleFailed           RunLifecycleStage = "failed"
	RunLifecycleNeedsHuman       RunLifecycleStage = "needs-human"
)

// RunLifecycleEvent reports an important Run transition. Message preserves the
// compatible append-only plain rendering while Stage supplies its semantic.
type RunLifecycleEvent struct {
	Stage   RunLifecycleStage
	Message string
}

func (RunLifecycleEvent) operationalEvent() {}

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

// ShutdownResult supplies an explicit success or failure semantic when a
// shutdown event needs outcome-specific presentation. None leaves presentation
// to ShutdownStage.
type ShutdownResult string

const (
	ShutdownResultNone    ShutdownResult = ""
	ShutdownResultSuccess ShutdownResult = "success"
	ShutdownResultFailure ShutdownResult = "failure"
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
	Result           ShutdownResult
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
	case CandidateSnapshotCompleted:
		return ""
	case CandidateDiscoveryRecovered:
		noun := "failures"
		if event.Failures == 1 {
			noun = "failure"
		}
		return fmt.Sprintf("candidate discovery recovered; admission resumed after %d %s", event.Failures, noun)
	case RunLifecycleEvent:
		return event.Message
	case ShutdownEvent:
		return event.Message
	default:
		return ""
	}
}
