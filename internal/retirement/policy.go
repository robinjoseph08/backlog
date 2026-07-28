package retirement

import (
	"fmt"
	"strings"
	"unicode"

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
	CanTransition      func(scheduler.Status, scheduler.Status) bool
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
	if strings.TrimSpace(p.Operation) == "" || p.SelectRun == nil || p.ValidateSnapshot == nil || p.CanTransition == nil || p.Explanation == nil || strings.TrimSpace(p.ExplanationAction) == "" {
		return fmt.Errorf("owned Run retirement policy is incomplete")
	}
	if p.ProgressStatus == "" || p.TerminalStatus == "" || len(p.EligibleStatuses) == 0 ||
		!p.statusEligible(p.ProgressStatus) || !p.statusEligible(p.TerminalStatus) {
		return fmt.Errorf("owned Run retirement policy has incomplete lifecycle states")
	}
	if p.ProgressStatus == p.TerminalStatus {
		return fmt.Errorf("owned Run retirement policy must have distinct progress and terminal states")
	}
	if !p.CanTransition(p.ProgressStatus, p.TerminalStatus) {
		return fmt.Errorf("owned Run retirement policy cannot transition from progress state %s to terminal state %s", p.ProgressStatus, p.TerminalStatus)
	}
	if len(p.Labels.Add) == 0 && len(p.Labels.Remove) == 0 {
		return fmt.Errorf("owned Run retirement policy has no label outcome")
	}
	add := make(map[string]struct{}, len(p.Labels.Add))
	for _, label := range p.Labels.Add {
		normalized := foldLabel(label)
		if strings.TrimSpace(label) == "" {
			return fmt.Errorf("owned Run retirement policy has an empty label to add")
		}
		if _, duplicate := add[normalized]; duplicate {
			return fmt.Errorf("owned Run retirement policy has duplicate label %q to add", label)
		}
		add[normalized] = struct{}{}
	}
	remove := make(map[string]struct{}, len(p.Labels.Remove))
	for _, label := range p.Labels.Remove {
		normalized := foldLabel(label)
		if strings.TrimSpace(label) == "" {
			return fmt.Errorf("owned Run retirement policy has an empty label to remove")
		}
		if _, duplicate := remove[normalized]; duplicate {
			return fmt.Errorf("owned Run retirement policy has duplicate label %q to remove", label)
		}
		if _, overlaps := add[normalized]; overlaps {
			return fmt.Errorf("owned Run retirement policy cannot both add and remove label %q", label)
		}
		remove[normalized] = struct{}{}
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
		if current[foldLabel(label)] {
			remove = append(remove, label)
		}
	}
	for _, label := range p.Labels.Add {
		if !current[foldLabel(label)] {
			add = append(add, label)
		}
	}
	return add, remove
}

// foldLabel produces the canonical representative of each Unicode simple-fold
// cycle, matching strings.EqualFold semantics without losing map lookups.
func foldLabel(label string) string {
	return strings.Map(func(r rune) rune {
		canonical := r
		for candidate := unicode.SimpleFold(r); candidate != r; candidate = unicode.SimpleFold(candidate) {
			if candidate < canonical {
				canonical = candidate
			}
		}
		return canonical
	}, label)
}

func (p Policy) labelsSatisfied(labels []string) bool {
	add, remove := p.desiredLabels(labels)
	return len(add) == 0 && len(remove) == 0
}
