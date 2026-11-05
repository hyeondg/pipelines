package pipelines

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// spinner frames for terminal animation
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// spinnerKeyType is the private context key for passing a shared MultiSpinner.
type spinnerKeyType struct{}

var spinnerKey spinnerKeyType

// ContextWithSpinner returns a child context that carries the given MultiSpinner.
func ContextWithSpinner(ctx context.Context, ms *MultiSpinner) context.Context {
	return context.WithValue(ctx, spinnerKey, ms)
}

// SpinnerFromContext retrieves the shared MultiSpinner from ctx, or nil if none.
func SpinnerFromContext(ctx context.Context) *MultiSpinner {
	ms, _ := ctx.Value(spinnerKey).(*MultiSpinner)
	return ms
}

// MultiSpinner displays a single animated line showing all in-progress items.
// As items complete, they are removed from the display.
//
// The spinner line is always kept below log output: any write to the underlying
// writer via SpinnerAwareWriter clears the spinner first, flushes the log line,
// then redraws the spinner underneath.
type MultiSpinner struct {
	mu      sync.Mutex
	items   []string
	w       io.Writer
	stop    chan struct{}
	done    chan struct{}
	lastLen int  // byte-width of the last spinner line written
	shown   bool // true while a spinner line is on screen
}

func NewMultiSpinner(w io.Writer) *MultiSpinner {
	return &MultiSpinner{w: w, stop: make(chan struct{}), done: make(chan struct{})}
}

func (ms *MultiSpinner) Add(name string) {
	ms.mu.Lock()
	ms.items = append(ms.items, name)
	ms.mu.Unlock()
}

func (ms *MultiSpinner) Remove(name string) {
	ms.mu.Lock()
	for i, item := range ms.items {
		if item == name {
			ms.items = append(ms.items[:i], ms.items[i+1:]...)
			break
		}
	}
	ms.mu.Unlock()
}

// clearLocked erases the spinner line from the terminal.
// Caller must hold ms.mu.
func (ms *MultiSpinner) clearLocked() {
	if !ms.shown {
		return
	}
	fmt.Fprintf(ms.w, "\r%s\r", strings.Repeat(" ", ms.lastLen))
	ms.shown = false
}

// drawLocked writes the spinner line to the terminal.
// Caller must hold ms.mu.
func (ms *MultiSpinner) drawLocked(frame int) {
	if len(ms.items) == 0 {
		return
	}
	msg := fmt.Sprintf("%s (%d running) ", spinnerFrames[frame%len(spinnerFrames)], len(ms.items))
	if ms.shown && ms.lastLen > len(msg) {
		// Pad to overwrite any leftover characters from a longer previous line.
		fmt.Fprintf(ms.w, "\r%s", strings.Repeat(" ", ms.lastLen))
	}
	fmt.Fprintf(ms.w, "\r%s", msg)
	ms.lastLen = len(msg)
	ms.shown = true
}

// InterceptWriter returns an io.Writer that, on every Write, clears the spinner
// line, forwards the bytes to the underlying writer, then redraws the spinner.
// Use this in place of the bare underlying writer for all log/status output so
// that log lines always appear above the spinner.
func (ms *MultiSpinner) InterceptWriter() io.Writer {
	return &spinnerInterceptWriter{ms: ms}
}

type spinnerInterceptWriter struct {
	ms *MultiSpinner
}

func (sw *spinnerInterceptWriter) Write(p []byte) (int, error) {
	ms := sw.ms
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.clearLocked()
	n, err := ms.w.Write(p)
	// Don't redraw here — the ticker will redraw on its next tick, keeping the
	// spinner below whatever was just written without an immediate flicker.
	return n, err
}

func (ms *MultiSpinner) Start() {
	go func() {
		defer close(ms.done)
		ticker := time.NewTicker(80 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-ms.stop:
				ms.mu.Lock()
				ms.clearLocked()
				ms.mu.Unlock()
				return
			case <-ticker.C:
				ms.mu.Lock()
				ms.drawLocked(i)
				ms.mu.Unlock()
				i++
			}
		}
	}()
}

func (ms *MultiSpinner) Stop() {
	close(ms.stop)
	<-ms.done
}
