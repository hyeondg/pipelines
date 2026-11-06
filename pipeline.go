package pipelines

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

// Pipeline orchestrates a sequence of Steps.
// NOTE: users must explicitly set `LogDir` when they initialise this struct.
type Pipeline struct {
	Name        string
	Description string
	Steps       []Step
	Verbose     bool
	NoSpinner   bool // disable the animated spinner (on by default)
	Logs        bytes.Buffer
	LogDir      string
}

func (p *Pipeline) AddStep(s Step) {
	p.Steps = append(p.Steps, s)
}

// AddCommand is a convenience method to add a Command step directly.
func (p *Pipeline) AddCommand(cmd Command) {
	p.Steps = append(p.Steps, &cmd)
}

// AddFunc is a convenience method to add a Go function step.
func (p *Pipeline) AddFunc(name, description string, fn func(ctx context.Context, sw *StepWriter) error) {
	p.Steps = append(p.Steps, &FuncStep{
		Name:        name,
		Description: description,
		Fn:          fn,
	})
}

// AddFuncStep registers predefined FuncStep to the pipeline.
func (p *Pipeline) AddFuncStep(fs FuncStep) {
	p.Steps = append(p.Steps, &fs)
}

func (p *Pipeline) HelpText() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s - %s\n\n", p.Name, p.Description)
	fmt.Fprintf(&sb, "Steps:\n")
	for i, step := range p.Steps {
		fmt.Fprintf(&sb, "  %d. %s", i+1, step.HelpInfo())
	}
	return sb.String()
}

func (p *Pipeline) GetLogFilePath() string {
	return filepath.Join(p.LogDir, p.Name+".log")
}

func (p *Pipeline) Run(ctx context.Context) error {
	return p.RunWithWriter(ctx, os.Stdout)
}

// RunWithWriter runs the pipeline and sends live status and step output to w.
func (p *Pipeline) RunWithWriter(ctx context.Context, w io.Writer) error {
	// Initialize LogDir: use pre-set value, derive from context, or create a temp dir.
	if p.LogDir == "" {
		p.LogDir = filepath.Join(PipelinesDir(ctx), p.Name)
	}
	if err := os.MkdirAll(p.LogDir, 0o755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	logFile, err := os.Create(p.GetLogFilePath())
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer logFile.Close()

	// Always create a MultiSpinner to track running steps for log enrichment.
	// Only start the animated display unless NoSpinner is set.
	// Reuse a parent spinner from context if one is already active.
	spinner := SpinnerFromContext(ctx)
	ownsSpinner := false
	if spinner == nil {
		spinner = NewMultiSpinner(w)
		if !p.NoSpinner {
			spinner.Start()
		}
		ownsSpinner = true
	}
	// When the animated spinner is running, route step output through its
	// InterceptWriter so text lines land above the spinner.
	outW := io.Writer(w)
	if !p.NoSpinner && ownsSpinner {
		outW = spinner.InterceptWriter()
	}
	sinkW := io.MultiWriter(outW, &p.Logs, logFile)
	// Enrich the context with the spinner and attach a progress hook so every
	// logrus line gets a "running" field showing in-progress step names.
	stepCtx := ContextWithSpinner(ctx, spinner)
	stepCtx = contextWithPipelineName(stepCtx, p.Name)
	defer addProgressHook(ctx, spinner)()

	l := LoggerFromContext(ctx)

	// Use a shared mutex from context when running concurrently so all pipelines
	// serialise their Flush calls against the same lock. Fall back to a local one.
	mu := sharedMuFromContext(ctx)
	if mu == nil {
		mu = new(sync.Mutex)
	}

	if l != nil {
		l.WithField("pipeline", p.Name).Info("pipeline started")
	}

	for i, step := range p.Steps {
		if err := ctx.Err(); err != nil {
			if !p.NoSpinner && ownsSpinner {
				spinner.Stop()
			}
			if l != nil {
				l.WithFields(logrus.Fields{
					"pipeline": p.Name,
					"error":    err,
				}).Warn("pipeline cancelled")
			}
			return fmt.Errorf("pipeline '%s' cancelled: %w", pipelineColor(p.Name), err)
		}
		if l != nil {
			l.WithFields(logrus.Fields{
				"pipeline": p.Name,
				"step":     step.StepName(),
				"progress": fmt.Sprintf("%d/%d", i+1, len(p.Steps)),
			}).Debug("running step")
		}
		switch step.(type) {
		case *ConcurrentStep:
			// ConcurrentStep manages its own spinner entries.
		default:
			spinner.Add(p.Name + "/" + step.StepName())
		}
		sw := newStepWriter(sinkW, mu, p.Name, step.StepName())
		err := step.Execute(stepCtx, sw)
		spinner.Remove(p.Name + "/" + step.StepName())
		if err != nil {
			if !p.NoSpinner && ownsSpinner {
				spinner.Stop()
			}
			if ctx.Err() != nil {
				if l != nil {
					l.WithFields(logrus.Fields{
						"pipeline": p.Name,
						"step":     step.StepName(),
					}).Warn("step interrupted by signal")
				}
				return fmt.Errorf("pipeline '%s' interrupted during '%s': %w", pipelineColor(p.Name), stepColor(step.StepName()), ctx.Err())
			}
			if l != nil {
				l.WithFields(logrus.Fields{
					"pipeline": p.Name,
					"step":     step.StepName(),
					"error":    err,
				}).Error("step failed")
			}
			return fmt.Errorf("step '%s' failed: %w", stepColor(step.StepName()), err)
		}
		if l != nil {
			l.WithFields(logrus.Fields{
				"pipeline": p.Name,
				"step":     step.StepName(),
				"progress": fmt.Sprintf("%d/%d", i+1, len(p.Steps)),
			}).Info("step completed")
		}
	}

	if !p.NoSpinner && ownsSpinner {
		spinner.Stop()
	}
	if l != nil {
		l.WithField("pipeline", p.Name).Info("pipeline finished successfully")
	}
	return nil
}

// RunOption configures the behaviour of RunPipelinesConcurrently[WithWriter].
type RunOption func(*runOptions)

type runOptions struct {
	continueOnError bool
	noSpinner       bool
}

// ContinueOnError returns a RunOption that allows remaining pipelines to keep
// running even when one of them fails. All errors are still collected and
// returned at the end. By default the first failure cancels the others.
func ContinueOnError() RunOption {
	return func(o *runOptions) { o.continueOnError = true }
}

// WithoutSpinner disables the animated terminal spinner during concurrent execution.
func WithoutSpinner() RunOption {
	return func(o *runOptions) { o.noSpinner = true }
}

// RunPipelinesConcurrently executes a slice of Pipelines concurrently.
// The caller is responsible for providing a context that is already wired to
// OS-signal cancellation (e.g. via signal.NotifyContext in main).
func RunPipelinesConcurrently(ctx context.Context, pipelines []Pipeline, opts ...RunOption) error {
	return RunPipelinesConcurrentlyWithWriter(ctx, pipelines, os.Stdout, opts...)
}

// RunPipelinesConcurrentlyWithWriter executes pipelines concurrently and streams
// status and command output to w as it is produced.
// Pass ContinueOnError() to keep remaining pipelines running after a failure.
func RunPipelinesConcurrentlyWithWriter(ctx context.Context, pipelines []Pipeline, w io.Writer, opts ...RunOption) error {
	var cfg runOptions
	for _, o := range opts {
		o(&cfg)
	}
	// Concurrent pipelines share the caller's writer; serialize writes so writers
	// such as bytes.Buffer remain safe while still receiving output immediately.
	liveWriter := &syncWriter{w: w}

	// Create a child context so we can cancel remaining pipelines on first failure.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type pipelineResult struct {
		name string
		err  error
	}

	// resultCh carries the outcome of every pipeline as it finishes (fan-in).
	resultCh := make(chan pipelineResult, len(pipelines))

	// Always create a MultiSpinner for running-pipeline tracking and log enrichment.
	// Only disable the animated display when WithoutSpinner() is passed.
	spinner := NewMultiSpinner(liveWriter)
	if !cfg.noSpinner {
		spinner.Start()
	}
	spinnerWriter := spinner.InterceptWriter()
	spinnerCtx := ContextWithSpinner(ctx, spinner)
	defer addProgressHook(ctx, spinner)()

	if liveLog := LoggerFromContext(ctx); liveLog != nil {
		names := make([]string, len(pipelines))
		for i, p := range pipelines {
			names[i] = p.Name
		}
		liveLog.WithField("pipelines", strings.Join(names, ", ")).Debug("starting concurrent execution")
	}

	// A single shared mutex is injected so steps across all concurrent pipelines
	// serialise their StepWriter.Flush calls — no output sections will interleave.
	sharedMu := new(sync.Mutex)
	spinnerCtx = contextWithSharedMu(spinnerCtx, sharedMu)

	var wg sync.WaitGroup
	for i := range pipelines {
		wg.Add(1)
		go func(pl *Pipeline) {
			defer wg.Done()
			err := pl.RunWithWriter(spinnerCtx, spinnerWriter)
			spinner.Remove(pl.Name)
			resultCh <- pipelineResult{name: pl.Name, err: err}
		}(&pipelines[i])
	}

	// Aggregator goroutine: reads results as they arrive.
	// On failure it cancels remaining pipelines unless ContinueOnError was set.
	var errs []string
	aggDone := make(chan struct{})
	go func() {
		defer close(aggDone)
		for r := range resultCh {
			if r.err != nil {
				errs = append(errs, r.err.Error())
				if !cfg.continueOnError {
					cancel() // signal remaining pipelines to stop
				}
			}
		}
	}()

	wg.Wait()
	close(resultCh) // signal aggregator that no more results are coming
	<-aggDone       // wait for aggregator to finish draining

	if !cfg.noSpinner {
		spinner.Stop()
	}

	l := LoggerFromContext(ctx)
	if len(errs) > 0 {
		if l != nil {
			l.Error("one or more pipelines failed")
		}
		return fmt.Errorf("one or more pipelines failed:\n- %s", strings.Join(errs, "\n- "))
	}
	if l != nil {
		l.Info("all pipelines executed successfully")
	}
	return nil
}
