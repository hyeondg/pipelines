package pipelines

import (
	"context"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

// pipelineColor renders a pipeline name in green.
var pipelineColor = color.New(color.FgGreen).SprintFunc()

// stepColor renders a step name in yellow.
var stepColor = color.New(color.FgYellow).SprintFunc()

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

// progressHook is a logrus hook attached while a MultiSpinner is active.
// On every log entry it:
//  1. Snapshots the spinner's running items and appends them as a "running" field.
//  2. Clears the spinner line from the terminal so the log line prints cleanly above it.
type progressHook struct {
	ms *MultiSpinner
}

func (h *progressHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

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
