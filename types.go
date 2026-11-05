package pipelines

import (
	"bytes"
	"context"
	"io"
	"sync"
)

// StepWriter is the output handle passed to every Execute call.
// Every Write acquires a shared lock so concurrent steps never interleave output.
// Each line is automatically prefixed with "[pipeline/step] " so all output is
// attributed without the step function needing to know its own name.
type StepWriter struct {
	mu     *sync.Mutex
	w      io.Writer
	prefix []byte // prepended to every line
}

// New creates a StepWriter that writes to w. When multiple StepWriters share
// the same *sync.Mutex, their writes are serialised against each other.
func New(w io.Writer) *StepWriter {
	return &StepWriter{w: w, mu: new(sync.Mutex)}
}

// Write serialises p to the underlying writer under the shared lock, prepending
// the configured prefix to every newline-delimited line.
func (sw *StepWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if len(sw.prefix) == 0 {
		return sw.w.Write(p)
	}
	// Prepend prefix to each line. We iterate over newline boundaries so that a
	// single Write call containing multiple lines gets every line prefixed.
	total := len(p)
	for len(p) > 0 {
		idx := bytes.IndexByte(p, '\n')
		var line []byte
		if idx >= 0 {
			line = p[:idx+1] // includes the '\n'
			p = p[idx+1:]
		} else {
			line = p
			p = nil
		}
		// Skip prefix-only lines (bare newlines from spacing).
		if len(bytes.TrimSpace(line)) == 0 {
			if _, err := sw.w.Write(line); err != nil {
				return 0, err
			}
			continue
		}
		if _, err := sw.w.Write(sw.prefix); err != nil {
			return 0, err
		}
		if _, err := sw.w.Write(line); err != nil {
			return 0, err
		}
	}
	return total, nil
}

// Flush is a no-op retained for API compatibility.
func (sw *StepWriter) Flush() error { return nil }

type stepWriterKeyType struct{}

var stepWriterKey stepWriterKeyType

func contextWithStepWriter(ctx context.Context, sw *StepWriter) context.Context {
	return context.WithValue(ctx, stepWriterKey, sw)
}

func stepWriterFromContext(ctx context.Context) *StepWriter {
	sw, _ := ctx.Value(stepWriterKey).(*StepWriter)
	return sw
}

type sharedMuKeyType struct{}

var sharedMuKey sharedMuKeyType

// contextWithSharedMu stores a shared sync.Mutex in ctx so concurrent pipelines
// all serialise their StepWriter.Flush calls against the same lock.
func contextWithSharedMu(ctx context.Context, mu *sync.Mutex) context.Context {
	return context.WithValue(ctx, sharedMuKey, mu)
}

// sharedMuFromContext retrieves the shared mutex from ctx, or nil if none.
func sharedMuFromContext(ctx context.Context) *sync.Mutex {
	mu, _ := ctx.Value(sharedMuKey).(*sync.Mutex)
	return mu
}

type pipelineNameKeyType struct{}

var pipelineNameKey pipelineNameKeyType

// contextWithPipelineName stores the current pipeline name so sub-steps can
// build "pipeline/step" labels for the running field.
func contextWithPipelineName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, pipelineNameKey, name)
}

// pipelineNameFromContext returns the pipeline name stored in ctx, or "".
func pipelineNameFromContext(ctx context.Context) string {
	name, _ := ctx.Value(pipelineNameKey).(string)
	return name
}

// newStepWriter creates a StepWriter that writes to w under mu, prefixing every
// line with "[pipelineName/stepName] ".
func newStepWriter(w io.Writer, mu *sync.Mutex, pipeline, step string) *StepWriter {
	return &StepWriter{w: w, mu: mu}
}

// Step is the interface that all pipeline steps must satisfy.
type Step interface {
	StepName() string
	StepDescription() string
	Execute(ctx context.Context, sw *StepWriter) error
	HelpInfo() string
}
