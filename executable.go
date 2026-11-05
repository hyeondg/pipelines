package pipelines

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// Option represents a CLI flag or argument.
type Option struct {
	Short       string
	Long        string
	Description string
	HasValue    bool
	Value       string
}

// Command represents an external command step.
type Command struct {
	Name        string
	Description string
	Executable  string
	Args        []string
	Options     []Option
}

func (c *Command) AddOption(opt Option) {
	c.Options = append(c.Options, opt)
}

func (c *Command) BuildArgs() []string {
	args := make([]string, 0, len(c.Args)+len(c.Options)*2)
	args = append(args, c.Args...)
	for _, opt := range c.Options {
		flag := opt.Long
		if flag == "" {
			flag = opt.Short
		}
		if opt.HasValue && opt.Value != "" {
			args = append(args, flag, opt.Value)
		} else if !opt.HasValue {
			args = append(args, flag)
		}
	}
	return args
}

// Step interface implementation for Command
func (c *Command) StepName() string        { return c.Name }
func (c *Command) StepDescription() string { return c.Description }
func (c *Command) Execute(ctx context.Context, sw *StepWriter) error {
	args := c.BuildArgs()
	if l := LoggerFromContext(ctx); l != nil {
		l.WithFields(logrus.Fields{
			"executable": c.Executable,
			"args":       strings.Join(args, " "),
		}).Debugf("[%s] executing command", stepColor(c.Name))
	}
	execCmd := exec.CommandContext(ctx, c.Executable, args...)
	// Route subprocess output through the StepWriter buffer so that output is
	// flushed atomically after the command completes.
	execCmd.Stdout = sw
	execCmd.Stderr = sw
	execCmd.Cancel = func() error {
		return execCmd.Process.Signal(syscall.SIGINT)
	}
	execCmd.WaitDelay = 5 * time.Second
	err := execCmd.Run()
	return err
}

func (c *Command) HelpInfo() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%-10s %s\n", c.Name, c.Description)
	if len(c.Options) > 0 {
		fmt.Fprintf(&sb, "     Options:\n")
		for _, opt := range c.Options {
			fmt.Fprintf(&sb, "       %-6s %-12s %s", opt.Short, opt.Long, opt.Description)
			if opt.HasValue && opt.Value != "" {
				fmt.Fprintf(&sb, " (value: %s)", opt.Value)
			}
			fmt.Fprintf(&sb, "\n")
		}
	}
	return sb.String()
}
