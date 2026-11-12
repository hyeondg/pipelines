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
// A Pipeline is a state machine: every finished or aborted step moves it to a
// State chosen by Pipeline.Transition, for instance from the step's exit
// code, and the jobs registered for that state (Pipeline.On) are run.
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
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

// State is where a Pipeline stands. The four states below are built in; a
// Transition may return any other value, such as "degraded".
type State string

const (
	Running   State = "running"   // steps are being executed
	Succeeded State = "succeeded" // final: every step was executed, or a Transition ended the run early
	Failed    State = "failed"    // final: Run returns an error
	Aborted   State = "aborted"   // final: Run returns an error
)

// Done reports whether s is final, which ends the run.
func (s State) Done() bool { return s == Succeeded || s == Failed || s == Aborted }

// Result is how a step ended, as the pipeline sees it after the step's policies.
type Result struct {
	Step     Step
	Err      error // nil on success
	ExitCode int   // 0 on success, else Err's ExitCode() (as *exec.ExitError has), or -1 if it has none
	Aborted  bool  // the step ended because the pipeline was cancelled
}

func exitCode(err error) int {
	var coded interface{ ExitCode() int }
	switch {
	case err == nil:
		return 0
	case errors.As(err, &coded):
		return coded.ExitCode()
	}
	return -1
}

// Next is the default Transition: an aborted step makes the pipeline Aborted,
// a failed one Failed, and a successful one leaves the state as it is.
func Next(s State, r Result) State {
	switch {
	case r.Aborted:
		return Aborted
	case r.Err != nil:
		return Failed
	}
	return s
}

// Pipeline is a state machine driven by its Steps, which it runs one after
// another. Run starts in Running. Each time a step finishes or gets aborted,
// Transition picks the next state from the step's Result and the Jobs of that
// state are run. A final state ends the run; a pipeline that is still Running
// after its last step becomes Succeeded.
type Pipeline struct {
	Name        string
	Description string
	Steps       []Step
	Policy      Policy // applied to every step; see WithPolicy for a single step
	LogDir      string // where <Name>.log is written; defaults to the Logger's Dir

	// State is maintained by Run. Read it from Transition, from jobs or after Run.
	State State
	// Transition picks the state that follows a step; nil means Next.
	Transition func(s State, r Result) State
	// Jobs are the steps to run, in order, whenever a step leaves the pipeline
	// in the given state. They run to completion even if the pipeline is
	// cancelled, and their failures are logged without changing the state.
	Jobs map[State][]Step
}

func (p *Pipeline) AddStep(s Step)          { p.Steps = append(p.Steps, s) }
func (p *Pipeline) AddCommand(cmd Command)  { p.AddStep(&cmd) }
func (p *Pipeline) AddFuncStep(fs FuncStep) { p.AddStep(&fs) }
func (p *Pipeline) GetLogFilePath() string  { return filepath.Join(p.LogDir, p.Name+".log") }

func (p *Pipeline) AddFunc(name, description string, fn func(ctx context.Context, sw *StepWriter) error) {
	p.AddStep(&FuncStep{Name: name, Description: description, Fn: fn})
}

// On registers jobs for state s.
func (p *Pipeline) On(s State, jobs ...Step) {
	if p.Jobs == nil {
		p.Jobs = map[State][]Step{}
	}
	p.Jobs[s] = append(p.Jobs[s], jobs...)
}

func (p *Pipeline) HelpText() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s - %s\n\nSteps:\n", p.Name, p.Description)
	for i, step := range p.Steps {
		fmt.Fprintf(&sb, "  %d. %s", i+1, step.HelpInfo())
	}
	for _, state := range slices.Sorted(maps.Keys(p.Jobs)) {
		for _, job := range p.Jobs[state] {
			fmt.Fprintf(&sb, "  on %s: %s", state, job.HelpInfo())
		}
	}
	return sb.String()
}

// Run drives the state machine until it reaches a final state and returns an
// error if that state is Failed or Aborted. The pipeline's log goes to the
// Logger in ctx, under the stream p.Name, and is complete on disk when Run
// returns.
func (p *Pipeline) Run(ctx context.Context) error {
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
	l.ch <- record{stream: p.Name, mark: mark(Running), file: file}

	ctx = context.WithValue(ctx, pipelineKey{}, p.Name)
	log := Log(ctx)
	out := &StepWriter{pipeline: p.Name, emit: func(line string) { l.ch <- record{stream: p.Name, line: line} }}
	next := p.Transition
	if next == nil {
		next = Next
	}
	p.State = Running
	log.Info("state: running")
	// enter is called when a step is over: it makes s the state and runs the state's jobs.
	enter := func(s State) {
		if s != p.State {
			p.State = s
			log.Infof("state: %s", s)
			l.ch <- record{stream: p.Name, mark: mark(s)}
		}
		for _, job := range p.Jobs[s] {
			runStep(context.WithoutCancel(ctx), log.WithField("on", s), job, out.sub(job.StepName()), Policy{})
		}
	}
	for i, s := range p.Steps {
		progress := log.WithField("progress", fmt.Sprintf("%d/%d", i+1, len(p.Steps)))
		cause := runStep(ctx, progress, s, out.sub(s.StepName()), p.Policy)
		enter(next(p.State, Result{Step: s, Err: cause, ExitCode: exitCode(cause), Aborted: cause != nil && ctx.Err() != nil}))
		if p.State == Failed || p.State == Aborted {
			err = fmt.Errorf("pipeline '%s' %s at step '%s': %w", p.Name, p.State, s.StepName(), cmp.Or(cause, errTransition))
			log.WithError(err).Errorf("pipeline %s", p.State)
		}
		if p.State.Done() {
			break
		}
	}
	if p.State == Running {
		enter(Succeeded)
	}
	flushed := make(chan struct{})
	l.ch <- record{stream: p.Name, mark: mark(p.State), done: flushed}
	<-flushed
	return err
}

// errTransition is the cause of a failure that no step error accounts for.
var errTransition = errors.New("state set by Transition")

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
// joined errors. It runs the slice's elements in place, so that is where their
// State is found afterwards. The caller provides a context wired to
// cancellation.
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
