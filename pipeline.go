// Package pipelines builds configurable, retryable workflows out of Steps:
// external processes (Command), Go functions (FuncStep) and groups of steps
// that run concurrently (ConcurrentStep). A Pipeline runs its steps in order,
// and several pipelines can run side by side.
//
// What happens when a step succeeds or fails is decided by a Policy: retries,
// callbacks and, after a failure, whether to abort, continue or rerun. A
// policy is set for a whole pipeline (Pipeline.Policy) or for one step
// (WithPolicy, WithRetry).
//
// All output goes through a Logger: step output and logrus entries are queued
// on one channel and demultiplexed per pipeline into log files and into the
// tabs of a gocui terminal viewer.
//
//	ctx, log, err := pipelines.NewLogger(ctx, "logs", true)
//	...
//	err = pipelines.RunPipelinesConcurrently(ctx, []pipelines.Pipeline{build, test})
//	log.Close()
package pipelines

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

// Pipeline runs its Steps one after another.
type Pipeline struct {
	Name        string
	Description string
	Steps       []Step
	Policy      Policy // applied to every step; see WithPolicy for a single step
	LogDir      string // where <Name>.log is written; defaults to the Logger's Dir
}

func (p *Pipeline) AddStep(s Step)          { p.Steps = append(p.Steps, s) }
func (p *Pipeline) AddCommand(cmd Command)  { p.AddStep(&cmd) }
func (p *Pipeline) AddFuncStep(fs FuncStep) { p.AddStep(&fs) }
func (p *Pipeline) GetLogFilePath() string  { return filepath.Join(p.LogDir, p.Name+".log") }

func (p *Pipeline) AddFunc(name, description string, fn func(ctx context.Context, sw *StepWriter) error) {
	p.AddStep(&FuncStep{Name: name, Description: description, Fn: fn})
}

func (p *Pipeline) HelpText() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s - %s\n\nSteps:\n", p.Name, p.Description)
	for i, step := range p.Steps {
		fmt.Fprintf(&sb, "  %d. %s", i+1, step.HelpInfo())
	}
	return sb.String()
}

// Run executes the steps in order and stops at the first one whose failure the
// policies did not absorb. The pipeline's log goes to the Logger in ctx, under
// the stream p.Name, and is complete on disk when Run returns.
func (p *Pipeline) Run(ctx context.Context) (err error) {
	if p.Name == "" {
		return errors.New("pipeline has no name")
	}
	ctx, l, done, err := session(ctx)
	if err != nil {
		return err
	}
	defer done()
	if p.LogDir == "" {
		p.LogDir = l.Dir
	}
	if err := os.MkdirAll(p.LogDir, 0o755); err != nil {
		return fmt.Errorf("pipeline '%s': %w", p.Name, err)
	}
	file, err := os.Create(p.GetLogFilePath())
	if err != nil {
		return fmt.Errorf("pipeline '%s': %w", p.Name, err)
	}
	l.ch <- record{stream: p.Name, mark: running, file: file}

	ctx = context.WithValue(ctx, pipelineKey{}, p.Name)
	log := Log(ctx)
	log.Info("pipeline started")
	defer func() {
		mark, flushed := passed, make(chan struct{})
		if err != nil {
			mark = failed
			log.WithError(err).Error("pipeline failed")
		} else {
			log.Info("pipeline finished successfully")
		}
		l.ch <- record{stream: p.Name, mark: mark, done: flushed}
		<-flushed
	}()

	out := &StepWriter{pipeline: p.Name, emit: func(line string) { l.ch <- record{stream: p.Name, line: line} }}
	for i, s := range p.Steps {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("pipeline '%s' cancelled: %w", p.Name, err)
		}
		progress := log.WithField("progress", fmt.Sprintf("%d/%d", i+1, len(p.Steps)))
		if err := runStep(ctx, progress, s, out.sub(s.StepName()), p.Policy); err != nil {
			return fmt.Errorf("pipeline '%s': step '%s' failed: %w", p.Name, s.StepName(), err)
		}
	}
	return nil
}

// runStep runs s under pol and logs how it went. Every step of a pipeline,
// top-level or nested, is started here.
func runStep(ctx context.Context, log *logrus.Entry, s Step, sw *StepWriter, pol Policy) error {
	log = log.WithField("step", s.StepName())
	log.Debug("step started")
	err := pol.run(ctx, s, sw)
	sw.Flush()
	switch {
	case err == nil:
		log.Info("step completed")
	case ctx.Err() != nil:
		log.Warn("step interrupted")
	default:
		log.WithError(err).Error("step failed")
	}
	return err
}

// RunOption configures RunPipelinesConcurrently.
type RunOption func(*runOptions)

type runOptions struct{ continueOnError bool }

// ContinueOnError keeps the other pipelines running when one fails. By default
// the first failure cancels them. Either way all errors are returned.
func ContinueOnError() RunOption {
	return func(o *runOptions) { o.continueOnError = true }
}

// RunPipelinesConcurrently runs the pipelines concurrently and returns their
// joined errors. The caller provides a context wired to cancellation.
func RunPipelinesConcurrently(ctx context.Context, pipelines []Pipeline, opts ...RunOption) error {
	var cfg runOptions
	for _, o := range opts {
		o(&cfg)
	}
	ctx, _, done, err := session(ctx)
	if err != nil {
		return err
	}
	defer done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(pipelines))
	var wg sync.WaitGroup
	for i := range pipelines {
		wg.Go(func() {
			if errs[i] = pipelines[i].Run(ctx); errs[i] != nil && !cfg.continueOnError {
				cancel()
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		Log(ctx).Error("one or more pipelines failed")
		return fmt.Errorf("one or more pipelines failed:\n%w", err)
	}
	Log(ctx).Info("all pipelines executed successfully")
	return nil
}
