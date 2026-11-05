package pipelines

import (
	"context"
	"fmt"

	"github.com/codeGROOVE-dev/retry"
	"github.com/sirupsen/logrus"
)

// RetryStep wraps any Step and re-executes it on failure using the
// github.com/codeGROOVE-dev/retry option set directly — no wrapper config.
//
// A logrus OnRetry hook is always appended last so every failed attempt is
// logged automatically. Because the library's OnRetry field is last-write-wins,
// do not pass retry.OnRetry(...) in opts if you want the built-in log hook to
// fire; use the logger in context instead.
type RetryStep struct {
	Step Step
	Opts []retry.Option
}

// WithRetry wraps s with the given retry options.
//
// Example:
//
//	pipelines.WithRetry(&step,
//	    retry.Attempts(3),
//	    retry.Delay(time.Second),
//	    retry.DelayType(retry.FixedDelay),
//	)
func WithRetry(s Step, opts ...retry.Option) *RetryStep {
	return &RetryStep{Step: s, Opts: opts}
}

func (r *RetryStep) StepName() string        { return r.Step.StepName() }
func (r *RetryStep) StepDescription() string { return r.Step.StepDescription() }
func (r *RetryStep) HelpInfo() string {
	return fmt.Sprintf("%s [with retry]\n", r.Step.HelpInfo())
}

// Execute runs the wrapped step with retry logic.
// retry.Context and retry.LastErrorOnly(true) are always prepended;
// the logrus OnRetry hook is always appended last.
func (r *RetryStep) Execute(ctx context.Context, sw *StepWriter) error {
	opts := make([]retry.Option, 0, len(r.Opts)+3)
	opts = append(opts, retry.Context(ctx), retry.LastErrorOnly(true))
	opts = append(opts, r.Opts...)
	opts = append(opts, retry.OnRetry(func(attempt uint, err error) {
		if l := LoggerFromContext(ctx); l != nil {
			l.WithFields(logrus.Fields{
				"step":    r.Step.StepName(),
				"attempt": fmt.Sprintf("%d", attempt+1),
				"error":   err,
			}).Warn("step failed, retrying")
		}
	}))

	return retry.Do(
		func() error { return r.Step.Execute(ctx, sw) },
		opts...,
	)
}
