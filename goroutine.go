package pipelines

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

// FuncStep represents an arbitrary Go function as a pipeline step.
// The function receives a context for cancellation and a StepWriter for output.
type FuncStep struct {
	Name        string
	Description string
	Fn          func(ctx context.Context, sw *StepWriter) error
}

func (f *FuncStep) StepName() string        { return f.Name }
func (f *FuncStep) StepDescription() string { return f.Description }
func (f *FuncStep) Execute(ctx context.Context, sw *StepWriter) error {
	if f.Fn == nil {
		return fmt.Errorf("FuncStep '%s' has a nil function", f.Name)
	}
	return f.Fn(ctx, sw)
}

func (f *FuncStep) HelpInfo() string {
	return fmt.Sprintf("%-10s %s [go func]\n", f.Name, f.Description)
}

// ConcurrentStep runs multiple Steps concurrently within a single pipeline slot.
// All sub-steps must complete before the pipeline advances. If any sub-step fails,
// the remaining ones are cancelled via context.
//
// MaxConcurrency caps the number of sub-steps that run simultaneously. When zero
// or negative, all sub-steps run at once (unbounded). Use a positive value to
// prevent goroutine explosion on large step slices.
type ConcurrentStep struct {
	Name           string
	Description    string
	Steps          []Step
	MaxConcurrency int // 0 = unbounded
}

func (cs *ConcurrentStep) StepName() string        { return cs.Name }
func (cs *ConcurrentStep) StepDescription() string { return cs.Description }
func (cs *ConcurrentStep) Execute(ctx context.Context, sw *StepWriter) error {
	// Create a sub-context so we can cancel sibling steps on first failure.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errChan := make(chan error, len(cs.Steps))

	// Reuse a parent spinner from context for tracking and log enrichment.
	// Only start a new animated spinner if none exists in context.
	spinner := SpinnerFromContext(ctx)
	if spinner == nil {
		spinner = NewMultiSpinner(sw.w)
		// No spinner.Start() — animation is controlled by Pipeline.NoSpinner.
		// Attach a progress hook so sub-step names appear in log lines.
		defer addProgressHook(ctx, spinner)()
	}
	for _, s := range cs.Steps {
		spinner.Add(pipelineNameFromContext(ctx) + "/" + s.StepName())
	}

	// sem is a semaphore channel that bounds the number of goroutines running
	// simultaneously. A nil sem means unbounded (MaxConcurrency <= 0).
	var sem chan struct{}
	if cs.MaxConcurrency > 0 {
		sem = make(chan struct{}, cs.MaxConcurrency)
	}

	for _, s := range cs.Steps {
		wg.Add(1)
		go func(step Step) {
			defer wg.Done()
			// Acquire semaphore slot (blocks if MaxConcurrency is reached).
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}
			// Each goroutine gets its own StepWriter sharing the parent mutex so
			// that Flush calls are serialised against all other concurrent siblings.
			childSW := newStepWriter(sw.w, sw.mu, pipelineNameFromContext(ctx), step.StepName())
			if l := LoggerFromContext(subCtx); l != nil {
				l.WithFields(logrus.Fields{
					"pipeline": pipelineNameFromContext(ctx),
					"step":     step.StepName(),
				}).Debug("starting sub-step")
			}
			if err := step.Execute(subCtx, childSW); err != nil {
				cancel() // cancel siblings
				if l := LoggerFromContext(subCtx); l != nil {
					l.WithFields(logrus.Fields{
						"pipeline": pipelineNameFromContext(ctx),
						"step":     step.StepName(),
						"error":    err,
					}).Error("sub-step failed")
				}
				errChan <- fmt.Errorf("sub-step '%s': %w", step.StepName(), err)
			} else {
				if l := LoggerFromContext(subCtx); l != nil {
					l.WithFields(logrus.Fields{
						"pipeline": pipelineNameFromContext(ctx),
						"step":     step.StepName(),
					}).Info("sub-step completed")
				}
			}
			spinner.Remove(pipelineNameFromContext(ctx) + "/" + step.StepName())
		}(s)
	}
	wg.Wait()
	close(errChan)
	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("concurrent step '%s' had failures:\n  - %s", cs.Name, strings.Join(errs, "\n  - "))
	}
	return nil
}

func (cs *ConcurrentStep) HelpInfo() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%-10s %s [concurrent group]\n", cs.Name, cs.Description)
	for _, s := range cs.Steps {
		fmt.Fprintf(&sb, "       └─ %s", s.HelpInfo())
	}
	return sb.String()
}
