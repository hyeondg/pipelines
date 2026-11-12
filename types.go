package pipelines

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

// Step is one unit of work in a Pipeline. Execute reports failure by returning
// an error; what happens next is decided by the Policy the step runs under.
type Step interface {
	StepName() string
	StepDescription() string
	HelpInfo() string
	Execute(ctx context.Context, sw *StepWriter) error
}

// StepWriter is the output handle passed to every Execute call. It cuts what
// is written into lines, tags them "[pipeline/step] " and queues them on the
// Logger. It is safe for concurrent use.
type StepWriter struct {
	mu             sync.Mutex
	buf            []byte // unterminated tail of the last Write
	pipeline, step string
	emit           func(line string)
}

// New returns a StepWriter that prints its lines to w instead of a Logger,
// which is handy for running a Step on its own.
func New(w io.Writer) *StepWriter {
	return &StepWriter{emit: func(line string) { fmt.Fprintln(w, line) }}
}

// sub returns a writer for another step that feeds the same sink.
func (sw *StepWriter) sub(step string) *StepWriter {
	return &StepWriter{pipeline: sw.pipeline, step: step, emit: sw.emit}
}

func (sw *StepWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.buf = append(sw.buf, p...)
	for {
		line, rest, found := bytes.Cut(sw.buf, []byte("\n"))
		if !found {
			return len(p), nil
		}
		sw.put(line)
		sw.buf = rest
	}
}

// Flush emits a trailing line that was not terminated by a newline.
func (sw *StepWriter) Flush() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if len(sw.buf) > 0 {
		sw.put(sw.buf)
		sw.buf = nil
	}
	return nil
}

func (sw *StepWriter) put(line []byte) {
	s := strings.TrimRight(string(line), "\r")
	if tag := strings.Trim(sw.pipeline+"/"+sw.step, "/"); tag != "" && strings.TrimSpace(s) != "" {
		s = "[" + tag + "] " + s
	}
	sw.emit(s)
}
