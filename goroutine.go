package pipelines

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// FuncStep is a Step that calls a Go function.
type FuncStep struct {
	Name        string
	Description string
	Fn          func(ctx context.Context, sw *StepWriter) error
}

func (f *FuncStep) StepName() string        { return f.Name }
func (f *FuncStep) StepDescription() string { return f.Description }
func (f *FuncStep) HelpInfo() string {
	return fmt.Sprintf("%-10s %s [go func]\n", f.Name, f.Description)
}

func (f *FuncStep) Execute(ctx context.Context, sw *StepWriter) error {
	if f.Fn == nil {
		return fmt.Errorf("FuncStep '%s' has a nil function", f.Name)
	}
	return f.Fn(ctx, sw)
}

// ConcurrentStep is a Step that runs its sub-steps concurrently and completes
// when all of them have. The first sub-step to fail cancels the others and its
// error is returned; wrap a sub-step with WithPolicy to retry it or to tolerate
// its failure.
type ConcurrentStep struct {
	Name           string
	Description    string
	Steps          []Step
	MaxConcurrency int // sub-steps running at once; <= 0 means all of them
}

func (cs *ConcurrentStep) StepName() string        { return cs.Name }
func (cs *ConcurrentStep) StepDescription() string { return cs.Description }

func (cs *ConcurrentStep) Execute(ctx context.Context, sw *StepWriter) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	limit := cs.MaxConcurrency
	if limit <= 0 {
		limit = len(cs.Steps)
	}
	slots := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for _, s := range cs.Steps {
		wg.Go(func() {
			slots <- struct{}{}
			defer func() { <-slots }()
			if err := runStep(ctx, Log(ctx), s, sw.sub(s.StepName()), Policy{}); err != nil {
				cancel(fmt.Errorf("sub-step '%s': %w", s.StepName(), err))
			}
		})
	}
	wg.Wait()
	return context.Cause(ctx) // the first failure, the parent's cancellation, or nil
}

func (cs *ConcurrentStep) HelpInfo() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%-10s %s [concurrent group]\n", cs.Name, cs.Description)
	for _, s := range cs.Steps {
		fmt.Fprintf(&sb, "       └─ %s", s.HelpInfo())
	}
	return sb.String()
}
