package retirement

import (
	"fmt"
	"strings"

	"github.com/robinjoseph08/backlog/internal/scheduler"
	"github.com/robinjoseph08/backlog/internal/state"
)

// Policy supplies lifecycle-specific decisions while Service owns all
// safety-critical inspection, mutation, revalidation, and verification.
type Policy struct {
	Operation          string
	SelectRun          func(state.State) (scheduler.Run, scheduler.Lease, error)
	ValidateSnapshot   func(Snapshot) error
	EligibleStatuses   []scheduler.Status
	Explanation        func(scheduler.Run) string
	ExplanationAction  string
	Labels             LabelOutcome
	ProgressStatus     scheduler.Status
	TerminalStatus     scheduler.Status
	RequireDurableLogs bool
}

// LabelOutcome identifies managed labels that must be present or absent when
// retirement completes. Labels not named here must remain unchanged.
type LabelOutcome struct {
	Add    []string
	Remove []string
}

func (p Policy) validate() error {
	if strings.TrimSpace(p.Operation) == "" || p.SelectRun == nil || p.ValidateSnapshot == nil || p.Explanation == nil || strings.TrimSpace(p.ExplanationAction) == "" {
		return fmt.Errorf("owned Run retirement policy is incomplete")
	}
	if p.ProgressStatus == "" || p.TerminalStatus == "" || len(p.EligibleStatuses) == 0 ||
		!p.statusEligible(p.ProgressStatus) || !p.statusEligible(p.TerminalStatus) {
		return fmt.Errorf("owned Run retirement policy has incomplete lifecycle states")
	}
	if len(p.Labels.Add) == 0 && len(p.Labels.Remove) == 0 {
		return fmt.Errorf("owned Run retirement policy has no label outcome")
	}
	return nil
}

func (p Policy) statusEligible(status scheduler.Status) bool {
	for _, candidate := range p.EligibleStatuses {
		if status == candidate {
			return true
		}
	}
	return false
}

func (p Policy) desiredLabels(labels []string) (add, remove []string) {
	current := normalizedLabelSet(labels)
	for _, label := range p.Labels.Remove {
		if current[strings.ToLower(label)] {
			remove = append(remove, label)
		}
	}
	for _, label := range p.Labels.Add {
		if !current[strings.ToLower(label)] {
			add = append(add, label)
		}
	}
	return add, remove
}

func (p Policy) labelsSatisfied(labels []string) bool {
	add, remove := p.desiredLabels(labels)
	return len(add) == 0 && len(remove) == 0
}
