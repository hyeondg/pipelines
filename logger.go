package pipelines

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

// pipelineColor renders a pipeline name in green.
var pipelineColor = color.New(color.FgGreen).SprintFunc()

// stepColor renders a step name in yellow.
var stepColor = color.New(color.FgYellow).SprintFunc()

// progressColor renders a progress step in yellow.
var progressColor = color.New(color.FgYellow).SprintFunc()

// levelColor returns a coloring function appropriate for the log level.
var levelColor = func(level logrus.Level) func(...interface{}) string {
	switch level {
	case logrus.TraceLevel:
		return color.New(color.Faint, color.FgWhite).SprintFunc()
	case logrus.DebugLevel:
		return color.New(color.Faint, color.FgCyan).SprintFunc()
	case logrus.WarnLevel:
		return color.New(color.FgYellow).SprintFunc()
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		return color.New(color.FgRed).SprintFunc()
	default: // Info
		return color.New(color.FgCyan).SprintFunc()
	}
}

// PipelineFormatter is a logrus.Formatter that renders entries in the layout:
//
//	LEVEL[time] (pipeline) (step) (progress) message  key=val …
//
// The fields "pipeline", "step", and "progress" are consumed from the entry's
// Data map and never printed as trailing key=value pairs. All other fields are
// appended after the message in key=value form.
type PipelineFormatter struct{}

func (f *PipelineFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b bytes.Buffer

	lvl := strings.ToUpper(entry.Level.String())[:4]
	paint := levelColor(entry.Level)
	fmt.Fprintf(&b, "%s[%s]", paint(lvl), entry.Time.Format("15:04:05"))

	if pipeline, ok := entry.Data["pipeline"].(string); ok && pipeline != "" {
		fmt.Fprintf(&b, "(%s)", pipelineColor(pipeline))
	}
	if step, ok := entry.Data["step"].(string); ok && step != "" {
		fmt.Fprintf(&b, "(%s)", stepColor(step))
	}
	if progress, ok := entry.Data["progress"].(string); ok && progress != "" {
		fmt.Fprintf(&b, "(%s)", progressColor(progress))
	}

	fmt.Fprintf(&b, " %s", entry.Message)

	// Append any remaining fields as key=value pairs.
	for k, v := range entry.Data {
		if k == "pipeline" || k == "step" || k == "progress" {
			continue
		}
		fmt.Fprintf(&b, "  %s=%v", k, v)
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

type loggerKeyType struct{}

var loggerKey loggerKeyType

func ContextWithLogger(ctx context.Context, l *logrus.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

func LoggerFromContext(ctx context.Context) *logrus.Logger {
	if l, ok := ctx.Value(loggerKey).(*logrus.Logger); ok {
		return l
	}
	return nil
}

// progressHook clears the spinner line before each log entry so text always
// lands above the spinner.
type progressHook struct {
	ms *MultiSpinner
}

func (h *progressHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *progressHook) Fire(entry *logrus.Entry) error {
	h.ms.mu.Lock()
	defer h.ms.mu.Unlock()
	h.ms.clearLocked()
	return nil
}

// addProgressHook attaches a progressHook to the logger in ctx and returns a
// function that removes it. If no logger is present in ctx it is a no-op.
func addProgressHook(ctx context.Context, ms *MultiSpinner) (remove func()) {
	l := LoggerFromContext(ctx)
	if l == nil {
		return func() {}
	}
	hook := &progressHook{ms: ms}
	l.AddHook(hook)
	return func() {
		l.ReplaceHooks(removeHook(l.Hooks, hook))
	}
}

// removeHook returns a copy of hooks with the given hook removed from all levels.
func removeHook(hooks logrus.LevelHooks, target logrus.Hook) logrus.LevelHooks {
	next := make(logrus.LevelHooks)
	for level, hs := range hooks {
		for _, h := range hs {
			if h != target {
				next[level] = append(next[level], h)
			}
		}
	}
	return next
}
