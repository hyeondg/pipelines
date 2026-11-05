package pipelines

import (
	"io"
	"sync"
)

// logLine is a single write payload sent through the serialising channel.
type logLine struct {
	data []byte
}

// ChanWriter is a goroutine-safe io.Writer that forwards every Write call
// through a shared channel to a single serialising goroutine, preventing
// interleaved or racy writes when multiple goroutines log to the same sink.
//
// Usage:
//
//	mux := NewChanWriterMux(underlying, 256)
//	defer mux.Close()
//	w1 := mux.Writer() // hand one per goroutine
//	w2 := mux.Writer()
type ChanWriter struct {
	ch chan<- logLine
}

// Write sends a copy of p to the serialising channel and never blocks the caller
// for longer than the channel drain rate.
func (cw *ChanWriter) Write(p []byte) (int, error) {
	buf := make([]byte, len(p))
	copy(buf, p)
	cw.ch <- logLine{data: buf}
	return len(p), nil
}

// ChanWriterMux owns the channel and the single drain goroutine.
type ChanWriterMux struct {
	ch   chan logLine
	done chan struct{}
}

// NewChanWriterMux creates a mux that drains writes to dst.
// bufSize controls how many in-flight writes can queue before callers block.
func NewChanWriterMux(dst io.Writer, bufSize int) *ChanWriterMux {
	m := &ChanWriterMux{
		ch:   make(chan logLine, bufSize),
		done: make(chan struct{}),
	}
	go func() {
		defer close(m.done)
		for line := range m.ch {
			_, _ = dst.Write(line.data)
		}
	}()
	return m
}

// Writer returns a new ChanWriter that sends to this mux.
// All ChanWriters from the same mux share the same serialising goroutine.
func (m *ChanWriterMux) Writer() *ChanWriter {
	return &ChanWriter{ch: m.ch}
}

// Close signals the drain goroutine to stop after flushing all queued writes,
// then blocks until it has exited.
func (m *ChanWriterMux) Close() {
	close(m.ch)
	<-m.done
}

// syncWriter wraps an io.Writer with a mutex for safe concurrent use.
// Used as a lightweight alternative when a full channel mux is not needed.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (sw *syncWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}
