package pipelines

import (
	"context"
	"strings"

	"github.com/codeGROOVE-dev/retry"
)

// Action is what happens to a step that has failed.
type Action int

const (
	Abort    Action = iota // return the error: the pipeline stops (default)
	Continue               // drop the error: the pipeline carries on
	Rerun                  // run the step again with a fresh retry budget
)

// Policy is the control over a step's outcome. A step runs until it succeeds
// or its Retry budget is spent; then exactly one of the callbacks is called.
//
// Cancellation is not a failure: once ctx is done the step is neither retried
// nor reported to OnFailure, and its error is returned as is.
type Policy struct {
	// Retry is passed to retry.Do. Without retry.Attempts the step is tried once;
	// retry.OnRetry replaces the built-in "retrying" log line.
	Retry []retry.Option
	// OnSuccess is called after the step succeeded.
	OnSuccess func(ctx context.Context, s Step)
	// OnFailure is called after the step failed for good and picks the Action.
	// nil means Abort.
	OnFailure func(ctx context.Context, s Step, err error) Action
}

// Always returns an OnFailure callback that picks a whatever the error.
func Always(a Action) func(context.Context, Step, error) Action {
	return func(context.Context, Step, error) Action { return a }
}

// run executes s under the policy.
func (p Policy) run(ctx context.Context, s Step, sw *StepWriter) error {
	log := Log(ctx).WithField("step", s.StepName())
	opts := append([]retry.Option{
		retry.Attempts(1), retry.Context(ctx), retry.LastErrorOnly(true),
		retry.RetryIf(func(error) bool { return ctx.Err() == nil }),
		retry.OnRetry(func(n uint, err error) { log.WithError(err).Warnf("attempt %d failed, retrying", n+1) }),
	}, p.Retry...)
	for {
		err := retry.Do(func() error { return s.Execute(ctx, sw) }, opts...)
		switch {
		case err == nil:
			if p.OnSuccess != nil {
				p.OnSuccess(ctx, s)
			}
			return nil
		case ctx.Err() != nil || p.OnFailure == nil:
			return err
		}
		switch p.OnFailure(ctx, s, err) {
		case Continue:
			log.WithError(err).Warn("step failed, continuing")
			return nil
		case Rerun:
			log.WithError(err).Warn("step failed, rerunning")
		default:
			return err
		}
	}
}

// policyStep is a Step that carries its own Policy.
type policyStep struct {
	Step
	Policy
}

// WithPolicy wraps s so that it runs under p wherever it is placed: in a
// Pipeline, in a ConcurrentStep or inside another WithPolicy. An enclosing
// policy (such as Pipeline.Policy) only sees the error that p let through.
func WithPolicy(s Step, p Policy) Step { return policyStep{s, p} }

// WithRetry wraps s so that it is retried as described by opts, for example
//
//	pipelines.WithRetry(step, retry.Attempts(3), retry.Delay(time.Second), retry.DelayType(retry.FixedDelay))
func WithRetry(s Step, opts ...retry.Option) Step { return WithPolicy(s, Policy{Retry: opts}) }

func (p policyStep) Execute(ctx context.Context, sw *StepWriter) error {
	return p.run(ctx, p.Step, sw)
}

func (p policyStep) HelpInfo() string {
	return strings.Replace(p.Step.HelpInfo(), "\n", " [with policy]\n", 1)
}
