package pipelines

import (
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

// All is the stream every line is copied to: the first tab and all.log.
const All = "all"

// record is what travels on a Logger's queue: a line of a stream, or, when
// mark is set, a state change of its pipeline, which may also be the start
// (file) or the end (done) of the stream.
type record struct {
	stream string          // pipeline name; "" for lines that only belong to All
	line   string          // without the trailing newline
	mark   string          // the pipeline's new state, rendered for its tab
	file   *os.File        // start: the stream's log file, owned by the demuxer from now on
	done   chan<- struct{} // end: closed once the stream's file is complete
}

// Logger demultiplexes the log of concurrently running pipelines. Step output
// and logrus entries are queued on one channel and a single goroutine routes
// each line, by the pipeline it belongs to, to that pipeline's log file and
// terminal tab, as well as to All.
//
// The embedded logrus.Logger is the front end for structured entries: level,
// formatter and hooks work as usual, and an entry is routed by its "pipeline"
// field (see Log).
type Logger struct {
	*logrus.Logger
	Dir string // holds all.log and, unless Pipeline.LogDir is set, <pipeline>.log

	ch      chan record
	drained chan struct{}
	show    func(record) // terminal side: prints the line or updates the TUI
	wait    func()       // returns when the terminal side is finished
	once    sync.Once
}

// NewLogger starts a Logger that stores its files in dir (default
// PipelinesDir) and returns it together with a context that carries it.
//
// With tui set, the terminal becomes a gocui viewer with one tab per stream,
// and the returned context is cancelled when the user presses q or Ctrl-C.
// Otherwise the lines of All are printed to stdout.
func NewLogger(ctx context.Context, dir string, tui bool) (context.Context, *Logger, error) {
	if dir == "" {
		dir = PipelinesDir(ctx)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ctx, nil, err
	}
	all, err := os.Create(filepath.Join(dir, All+".log"))
	if err != nil {
		return ctx, nil, err
	}
	l := &Logger{Logger: logrus.New(), Dir: dir, ch: make(chan record, 256), drained: make(chan struct{})}
	l.show, l.wait = func(r record) {
		if r.mark == "" {
			fmt.Println(r.line)
		}
	}, func() {}
	if tui {
		parent := ctx
		ctx, l.show, l.wait, err = newTUI(parent)
		if err != nil {
			all.Close()
			return parent, nil, err
		}
	}
	l.SetOutput(io.Discard) // entries leave through the hook instead
	l.SetFormatter(PipelineFormatter{})
	l.AddHook(hook{l.ch})
	l.ExitFunc = func(code int) { l.Close(); os.Exit(code) } // Fatal must not leave the terminal in TUI mode
	go l.demux(all)
	return context.WithValue(ctx, loggerKey{}, l), l, nil
}

var ansi = regexp.MustCompile("\x1b\\[[0-9;]*m")

// demux is the only reader of the queue and the only owner of the log files.
func (l *Logger) demux(all *os.File) {
	defer close(l.drained)
	files := map[string]*os.File{} // a missing stream yields a nil file, on which writes are no-ops
	for r := range l.ch {
		l.show(r)
		switch {
		case r.mark == "":
			text := ansi.ReplaceAllString(r.line, "") + "\n"
			all.WriteString(text)
			files[r.stream].WriteString(text)
		case r.file != nil:
			files[r.stream].Close()
			files[r.stream] = r.file
		case r.done != nil:
			files[r.stream].Close()
			delete(files, r.stream)
			close(r.done)
		}
	}
	all.Close()
}

// Close completes the log files and, in TUI mode, keeps the viewer open until
// the user quits it. Call it once every pipeline has returned; entries logged
// afterwards go to stderr.
func (l *Logger) Close() {
	l.once.Do(func() {
		l.ReplaceHooks(logrus.LevelHooks{})
		close(l.ch)
		<-l.drained
		l.wait()
		l.SetOutput(os.Stderr)
	})
}

// hook queues logrus entries on the Logger.
type hook struct{ ch chan<- record }

func (hook) Levels() []logrus.Level { return logrus.AllLevels }

func (h hook) Fire(e *logrus.Entry) error {
	b, err := e.Bytes()
	stream, _ := e.Data["pipeline"].(string)
	h.ch <- record{stream: stream, line: strings.TrimRight(string(b), "\n")}
	return err
}

type (
	loggerKey   struct{}
	pipelineKey struct{}
)

// LoggerFromContext returns the Logger carried by ctx, or nil.
func LoggerFromContext(ctx context.Context) *Logger {
	l, _ := ctx.Value(loggerKey{}).(*Logger)
	return l
}

// Log returns an entry for the Logger in ctx that is bound to the pipeline
// running in ctx, so that what it logs lands in that pipeline's tab and file.
// Without a Logger it falls back to logrus's standard logger.
func Log(ctx context.Context) *logrus.Entry {
	e := logrus.NewEntry(logrus.StandardLogger())
	if l := LoggerFromContext(ctx); l != nil {
		e = logrus.NewEntry(l.Logger)
	}
	if name, ok := ctx.Value(pipelineKey{}).(string); ok {
		e = e.WithField("pipeline", name)
	}
	return e
}

// session returns the Logger in ctx. If there is none, it starts a plain one
// that lasts until done is called.
func session(ctx context.Context) (_ context.Context, l *Logger, done func(), err error) {
	if l = LoggerFromContext(ctx); l != nil {
		return ctx, l, func() {}, nil
	}
	if ctx, l, err = NewLogger(ctx, "", false); err != nil {
		return ctx, nil, nil, err
	}
	return ctx, l, l.Close, nil
}

func paint(attrs ...color.Attribute) func(...any) string { return color.New(attrs...).SprintFunc() }

var (
	levelPaint = map[logrus.Level]func(...any) string{
		logrus.TraceLevel: paint(color.Faint, color.FgWhite),
		logrus.DebugLevel: paint(color.Faint, color.FgCyan),
		logrus.InfoLevel:  paint(color.FgCyan),
		logrus.WarnLevel:  paint(color.FgYellow),
	} // anything more severe is red
	headFields = []struct {
		key   string
		paint func(...any) string
	}{{"pipeline", paint(color.FgRed)}, {"step", paint(color.FgYellow)}, {"progress", paint(color.FgGreen)}}
)

// PipelineFormatter is a logrus.Formatter that renders entries as
//
//	LEVL[15:04:05](pipeline)(step)(progress) message  key=value ...
//
// where the three leading fields are shown only if the entry has them.
type PipelineFormatter struct{}

func (PipelineFormatter) Format(e *logrus.Entry) ([]byte, error) {
	level, ok := levelPaint[e.Level]
	if !ok {
		level = paint(color.FgRed)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s[%s]", level(strings.ToUpper(e.Level.String())[:4]), e.Time.Format("15:04:05"))
	rest := maps.Clone(e.Data)
	for _, f := range headFields {
		if v, _ := rest[f.key].(string); v != "" {
			fmt.Fprintf(&b, "(%s)", f.paint(v))
		}
		delete(rest, f.key)
	}
	b.WriteString(" " + e.Message)
	for _, k := range slices.Sorted(maps.Keys(rest)) {
		fmt.Fprintf(&b, "  %s=%v", k, rest[k])
	}
	return []byte(b.String() + "\n"), nil
}
