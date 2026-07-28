package resolution

import (
	"context"
	"fmt"

	"github.com/robinjoseph08/backlog/internal/retirement"
	"github.com/robinjoseph08/backlog/internal/scheduler"
)

// AutomaticReconciler connects supervising Runner reconciliation to the same
// complete owned-artifact retirement module used by backlog resolve.
type AutomaticReconciler struct {
	config     retirement.Config
	repository string
}

// NewAutomaticReconciler validates the shared retirement configuration once.
func NewAutomaticReconciler(config retirement.Config, repository string) (*AutomaticReconciler, error) {
	if repository == "" {
		return nil, fmt.Errorf("automatic External Resolution repository is empty")
	}
	if _, err := retirement.New(config, Policy("configuration-check")); err != nil {
		return nil, err
	}
	return &AutomaticReconciler{config: config, repository: repository}, nil
}

// Reconcile checks whether the issue is closed before performing a complete,
// non-interactive External Resolution with a freshly inspected plan. A Run
// already resolving externally resumes directly because closure was verified
// before its first durable mutation. Preflight failures are not cleanup
// attempts; normal Runner reconciliation remains responsible for the Run.
func (r *AutomaticReconciler) Reconcile(ctx context.Context, run scheduler.Run) (attempted bool, err error) {
	if run.Status != scheduler.StatusResolvingExternally {
		closure, err := r.config.GitHub.IssueClosure(ctx, r.repository, run.Issue)
		if err != nil {
			return false, fmt.Errorf("verify issue closure for automatic External Resolution: %w", err)
		}
		if closure.Open {
			return false, nil
		}
	}

	module, err := retirement.New(r.config, Policy(run.RunID))
	if err != nil {
		return true, err
	}
	plan, err := module.Inspect(ctx)
	if err != nil {
		return true, err
	}
	if err := module.Validate(plan); err != nil {
		return true, err
	}
	fresh, err := module.Inspect(ctx)
	if err != nil {
		return true, err
	}
	if err := module.Validate(fresh); err != nil {
		return true, err
	}
	if err := module.Retire(ctx, fresh); err != nil {
		return true, err
	}
	return true, nil
}
