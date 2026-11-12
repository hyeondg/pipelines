package pipelines

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
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

// Command is a Step that runs an external command.
type Command struct {
	Name        string
	Description string
	Environment map[string]string
	Executable  string
	Args        []string
	Options     []Option
}

func (c *Command) AddOption(opt Option) { c.Options = append(c.Options, opt) }

func (c *Command) BuildArgs() []string {
	args := append([]string{}, c.Args...)
	for _, opt := range c.Options {
		flag := opt.Long
		if flag == "" {
			flag = opt.Short
		}
		if !opt.HasValue {
			args = append(args, flag)
		} else if opt.Value != "" {
			args = append(args, flag, opt.Value)
		}
	}
	return args
}

func (c *Command) StepName() string        { return c.Name }
func (c *Command) StepDescription() string { return c.Description }

// Execute runs the command with its output sent to sw. On cancellation the
// process is interrupted and, if it lingers, killed five seconds later.
func (c *Command) Execute(ctx context.Context, sw *StepWriter) error {
	args := c.BuildArgs()
	Log(ctx).WithFields(logrus.Fields{
		"step":        c.Name,
		"environment": c.Environment,
		"executable":  c.Executable,
		"args":        strings.Join(args, " "),
	}).Debug("executing command")
	cmd := exec.CommandContext(ctx, c.Executable, args...)
	cmd.Stdout, cmd.Stderr = sw, sw
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = os.Environ()
	for k, v := range c.Environment {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd.Run()
}

func (c *Command) HelpInfo() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%-10s %s\n", c.Name, c.Description)
	if len(c.Options) > 0 {
		fmt.Fprintf(&sb, "     Options:\n")
	}
	for _, opt := range c.Options {
		fmt.Fprintf(&sb, "       %-6s %-12s %s", opt.Short, opt.Long, opt.Description)
		if opt.HasValue && opt.Value != "" {
			fmt.Fprintf(&sb, " (value: %s)", opt.Value)
		}
		fmt.Fprintln(&sb)
	}
	return sb.String()
}
