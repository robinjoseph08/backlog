package runner

import (
	"fmt"
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
// one Candidate. Err is the original discovery error.
type CandidateDiscoveryFailed struct {
	Operation           CandidateDiscoveryOperation
	Issue               *int
	Err                 error
	OccurredAt          time.Time
	RetryAt             time.Time
	ConsecutiveFailures int
}

func (CandidateDiscoveryFailed) operationalEvent() {}

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
