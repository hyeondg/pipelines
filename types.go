package pipelines

import (
	"bytes"
	"context"
	"io"
	"sync"
)

// StepWriter is the single output handle passed to every Execute call.
// Writes accumulate in an internal buffer and are never visible to other steps
// until Flush is called. Flush acquires the shared lock, copies the buffer to
// the real writer atomically, then resets the buffer — guaranteeing that
// everything written between two Flush calls arrives contiguously with no
// interleaving from concurrent siblings.
type StepWriter struct {
	mu  *sync.Mutex
	w   io.Writer
	buf bytes.Buffer
}

// Write appends p to the internal buffer. It never blocks on the shared lock.
func (sw *StepWriter) Write(p []byte) (int, error) {
	return sw.buf.Write(p)
}

// Flush atomically writes the buffered content to the underlying writer and
// resets the buffer. Call it whenever a logically complete chunk of output
// (a status line, a multi-line block, etc.) is ready to be displayed.
func (sw *StepWriter) Flush() error {
	if sw.buf.Len() == 0 {
		return nil
	}
	sw.mu.Lock()
	defer sw.mu.Unlock()
	_, err := sw.buf.WriteTo(sw.w)
	return err
}

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

// newStepWriter creates a StepWriter that flushes to w under mu.
func newStepWriter(w io.Writer, mu *sync.Mutex) *StepWriter {
	return &StepWriter{w: w, mu: mu}
}

// Step is the interface that all pipeline steps must satisfy.
type Step interface {
	StepName() string
	StepDescription() string
	Execute(ctx context.Context, sw *StepWriter) error
	HelpInfo() string
}
