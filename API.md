# pipelines API reference

```go
import "github.com/hyeondg/pipelines"
```

Package `pipelines` builds configurable, retryable workflows across processes, goroutines and foreign calls.

| Concept | Type | Role |
|---|---|---|
| Step | `Step`, `Command`, `FuncStep`, `ConcurrentStep` | One unit of work; succeeds or fails by returning an error. |
| Policy | `Policy`, `Action`, `WithPolicy`, `WithRetry` | What to do with a step's outcome: retry, callbacks, abort / continue / rerun. |
| Pipeline | `Pipeline`, `State`, `Result` | A state machine that runs steps in order, changes state after each one and runs the jobs of that state. |
| Logger | `Logger`, `StepWriter`, `Log` | One queue for all output, demultiplexed per pipeline into log files and terminal tabs. |

Requirements: Go 1.26.8 or later, as set in `go.mod`. Dependencies: `github.com/codeGROOVE-dev/retry` (retry options), `github.com/sirupsen/logrus` (structured entries), `github.com/awesome-gocui/gocui` (terminal viewer), `github.com/fatih/color`.

## Contents

- [Quick start](#quick-start)
- [Execution model](#execution-model)
- [Steps](#steps)
- [Policies](#policies)
- [Pipeline](#pipeline)
- [Running several pipelines](#running-several-pipelines)
- [Logging](#logging)
- [Utilities](#utilities)
- [Limitations](#limitations)

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/codeGROOVE-dev/retry"
	"github.com/hyeondg/pipelines"
)

func main() {
	// true: gocui viewer with one tab per pipeline; false: plain lines on stdout.
	ctx, log, err := pipelines.NewLogger(context.Background(), "logs", true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	build := pipelines.Pipeline{Name: "build"}
	build.AddCommand(pipelines.Command{Name: "compile", Executable: "go", Args: []string{"build", "./..."}})

	test := pipelines.Pipeline{Name: "test"}
	test.AddStep(pipelines.WithRetry(
		&pipelines.Command{Name: "unit", Executable: "go", Args: []string{"test", "./..."}},
		retry.Attempts(3), retry.Delay(time.Second),
	))
	test.On(pipelines.Failed, &pipelines.Command{Name: "report", Executable: "echo", Args: []string{"tests failed"}})

	all := []pipelines.Pipeline{build, test}
	err = pipelines.RunPipelinesConcurrently(ctx, all)
	log.Close() // viewer mode: returns when the user presses q

	for _, p := range all {
		fmt.Println(p.Name, p.State, p.GetLogFilePath())
	}
	if err != nil {
		os.Exit(1)
	}
}
```

## Execution model

For every step of a pipeline, in order:

1. **Policies run the step.** The step is executed, and retried according to `Policy.Retry`. Then `OnSuccess` or `OnFailure` is called; the `Action` returned by `OnFailure` decides whether the failure is returned (`Abort`), dropped (`Continue`) or the step is run again (`Rerun`). A step wrapped with `WithPolicy` applies its own policy first; `Pipeline.Policy` is applied around it.
2. **The pipeline builds a `Result`** from what the policies left: the error, its exit code, and whether the step was aborted by cancellation.
3. **`Transition` picks the next `State`** from the current state and the `Result`.
4. **The jobs of that state run**, in order.
5. **A final state ends the run.** Otherwise the next step starts. A pipeline still `Running` after its last step becomes `Succeeded` (and the `Succeeded` jobs run).

Everything a step writes and every log entry goes through the `Logger` of the context.

## Steps

### type Step

```go
type Step interface {
	StepName() string
	StepDescription() string
	HelpInfo() string
	Execute(ctx context.Context, sw *StepWriter) error
}
```

`Execute` does the work, writes its output to `sw` and reports failure by returning an error. It should return promptly once `ctx` is done.

`HelpInfo` returns the step's entry in `Pipeline.HelpText`; it ends with a newline.

To give a custom step an exit code, return an error that has an `ExitCode() int` method (see [`Result`](#type-result)).

### type Command

```go
type Command struct {
	Name        string
	Description string
	Environment map[string]string
	Executable  string
	Args        []string
	Options     []Option
}

func (c *Command) AddOption(opt Option)
func (c *Command) BuildArgs() []string
```

A step that runs an external process.

- **Arguments:** `BuildArgs` returns `Args` followed by the `Options`.
- **Environment:** the process inherits the parent's environment plus `Environment`.
- **Output:** stdout and stderr both go to the `StepWriter`. Stdin is not connected.
- **Result:** a non-zero exit is returned as `*exec.ExitError`, so its code reaches `Result.ExitCode`.
- **Cancellation:** the process receives an interrupt signal and is killed if it has not exited five seconds later.

### type Option

```go
type Option struct {
	Short       string
	Long        string
	Description string
	HasValue    bool
	Value       string
}
```

A command-line flag of a `Command`. The flag is `Long`, or `Short` if `Long` is empty.

| `HasValue` | `Value` | Appended to the arguments |
|---|---|---|
| `false` | any | the flag |
| `true` | non-empty | the flag, then the value |
| `true` | empty | nothing |

### type FuncStep

```go
type FuncStep struct {
	Name        string
	Description string
	Fn          func(ctx context.Context, sw *StepWriter) error
}
```

A step that calls a Go function. A nil `Fn` makes `Execute` return an error.

### type ConcurrentStep

```go
type ConcurrentStep struct {
	Name           string
	Description    string
	Steps          []Step
	MaxConcurrency int // sub-steps running at once; <= 0 means all of them
}
```

A step that runs its sub-steps concurrently and completes when all of them have returned.

- **Failure:** the first sub-step to fail cancels the others. `Execute` returns that first failure as `sub-step '<name>': <error>`; errors of the cancelled siblings are not returned.
- **Cancellation:** if the surrounding context is cancelled, `Execute` returns its cause.
- **Policies:** sub-steps run under an empty policy. Wrap a sub-step with `WithPolicy` to retry it or to tolerate its failure; a tolerated failure does not cancel the others.
- **Output:** each sub-step has its own writer, tagged with its own name.

To the pipeline the group is one step: one `Result`, one transition.

### type StepWriter

```go
func New(w io.Writer) *StepWriter
func (sw *StepWriter) Write(p []byte) (int, error)
func (sw *StepWriter) Flush() error
```

The output handle passed to `Execute`. It is an `io.Writer`, safe for concurrent use.

- Output is cut into lines. Each non-blank line is prefixed with `[pipeline/step] ` and queued on the `Logger`.
- A partial line is held until its newline arrives. `Flush` emits it; the pipeline calls `Flush` when the step is over.
- `New(w)` returns a writer that prints untagged lines to `w` instead of a `Logger`. Use it to run a step on its own, for example in a test:

```go
var buf bytes.Buffer
err := step.Execute(ctx, pipelines.New(&buf))
```

## Policies

### type Policy

```go
type Policy struct {
	Retry     []retry.Option
	OnSuccess func(ctx context.Context, s Step)
	OnFailure func(ctx context.Context, s Step, err error) Action
}
```

The control over a step's outcome.

| Field | Meaning |
|---|---|
| `Retry` | Options for `retry.Do`: attempts, delay, back-off, `RetryIf`, `OnRetry`, and so on. Without `retry.Attempts` the step is tried once. By default each retry is logged as a warning; `retry.OnRetry` replaces that log line. `retry.Unrecoverable(err)` returned by a step stops the retries. |
| `OnSuccess` | Called once after the step succeeded. |
| `OnFailure` | Called once the retries are spent. It picks the `Action`. `nil` means `Abort`. |

Rules:

- A failure dropped with `Continue` does not call `OnSuccess`.
- With `Rerun`, `OnFailure` is called again each time the step fails for good.
- Cancellation is not a failure. Once `ctx` is done the step is not retried, `OnFailure` is not called, and the error is returned as it is.
- Callbacks run on the goroutine that runs the step. Under a `ConcurrentStep` or concurrent pipelines, callbacks shared between steps must be safe for concurrent use.

### type Action

```go
type Action int

const (
	Abort    Action = iota // return the error (default)
	Continue               // drop the error; the step counts as succeeded
	Rerun                  // run the step again with a fresh retry budget
)
```

`Rerun` repeats until the step succeeds, `OnFailure` picks something else, or the context is cancelled.

### func Always

```go
func Always(a Action) func(context.Context, Step, error) Action
```

Returns an `OnFailure` callback that picks `a` whatever the error:

```go
pipelines.Policy{OnFailure: pipelines.Always(pipelines.Continue)}
```

### func WithPolicy

```go
func WithPolicy(s Step, p Policy) Step
```

Wraps `s` so that it runs under `p` wherever it is placed: in a `Pipeline`, in a `ConcurrentStep`, or inside another `WithPolicy`.

Policies nest. An enclosing policy, such as `Pipeline.Policy`, sees only the error the inner policy let through, and its own retries repeat the inner policy as a whole. Three attempts inside and three outside can therefore execute the step nine times.

```go
step := pipelines.WithPolicy(upload, pipelines.Policy{
	Retry: []retry.Option{retry.Attempts(5), retry.Delay(time.Second)},
	OnFailure: func(ctx context.Context, s pipelines.Step, err error) pipelines.Action {
		pipelines.Log(ctx).Warnf("%s gave up: %v", s.StepName(), err)
		return pipelines.Abort
	},
})
```

### func WithRetry

```go
func WithRetry(s Step, opts ...retry.Option) Step
```

Shorthand for `WithPolicy(s, Policy{Retry: opts})`.

## Pipeline

### type Pipeline

```go
type Pipeline struct {
	Name        string
	Description string
	Steps       []Step
	Policy      Policy
	LogDir      string

	State      State
	Transition func(s State, r Result) State
	Jobs       map[State][]Step
}
```

| Field | Meaning |
|---|---|
| `Name` | Required. Names the log stream: the pipeline's tab and `<Name>.log`. Use a unique name per running pipeline, and not `all`. |
| `Description` | Shown by `HelpText`. |
| `Steps` | Executed in order. |
| `Policy` | Applied to every step, around any policy the step carries itself. |
| `LogDir` | Directory of `<Name>.log`. If empty, `Run` sets it to the `Logger`'s `Dir`. |
| `State` | Maintained by `Run`. Read it from `Transition`, from jobs, or after `Run` has returned. It is not synchronised for other goroutines. |
| `Transition` | Picks the state that follows a step. `nil` means `Next`. |
| `Jobs` | Steps to run, per state. See [Jobs](#jobs). |

### Building

```go
func (p *Pipeline) AddStep(s Step)
func (p *Pipeline) AddCommand(cmd Command)
func (p *Pipeline) AddFuncStep(fs FuncStep)
func (p *Pipeline) AddFunc(name, description string, fn func(ctx context.Context, sw *StepWriter) error)
func (p *Pipeline) On(s State, jobs ...Step)
```

The `Add` methods append to `Steps`. `On` appends jobs for state `s`.

### func (*Pipeline) Run

```go
func (p *Pipeline) Run(ctx context.Context) error
```

Drives the state machine until it reaches a final state.

- **Return value:** an error if the final state is `Failed` or `Aborted`, reading `pipeline '<name>' <state> at step '<step>': <cause>`. The cause is the step's error and can be matched with `errors.Is` and `errors.As`. A run that ends in `Succeeded` or in a custom state returns `nil`.
- **Setup errors:** an empty `Name`, or a log directory or file that cannot be created, makes `Run` return an error before any step runs.
- **Log:** `<LogDir>/<Name>.log` is truncated at the start and complete on disk when `Run` returns.
- **Logger:** if `ctx` carries no `Logger`, `Run` starts a plain one in `PipelinesDir(ctx)` for its duration.
- **Cancellation:** when `ctx` is cancelled, the running step is interrupted and the remaining steps are not executed.

### Other methods

```go
func (p *Pipeline) HelpText() string
func (p *Pipeline) GetLogFilePath() string
```

`HelpText` lists the name, description, steps and jobs. `GetLogFilePath` returns `<LogDir>/<Name>.log`; before the first `Run` of a pipeline with an empty `LogDir` the directory is not known yet.

### type State

```go
type State string

const (
	Running   State = "running"
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Aborted   State = "aborted"
)

func (s State) Done() bool
```

| State | Final | Meaning |
|---|---|---|
| `Running` | no | Steps are being executed. `Run` starts here. |
| `Succeeded` | yes | Every step was executed, or a transition ended the run early. `Run` returns `nil`. |
| `Failed` | yes | `Run` returns an error. |
| `Aborted` | yes | `Run` returns an error. |
| any other value | no | A custom state, such as `"degraded"`. The pipeline goes on. |

`Done` reports whether the state is final. Custom states cannot be final. A custom state stays until a transition changes it, so a run can end in one.

### type Result

```go
type Result struct {
	Step     Step
	Err      error
	ExitCode int
	Aborted  bool
}
```

How a step ended, as the pipeline sees it after the step's policies.

| Field | Value |
|---|---|
| `Step` | The step. |
| `Err` | `nil` on success, including a failure dropped with `Continue`. |
| `ExitCode` | `0` on success. Otherwise the result of `ExitCode()` on the first error in `Err`'s chain that has that method, as `*exec.ExitError` from a `Command` does. `-1` if there is none, and for a process that was killed by a signal. |
| `Aborted` | The step returned an error while the pipeline's context was cancelled. A step that had not started yet when the context was cancelled is reported the same way, without being executed. |

For a `ConcurrentStep`, the exit code is that of the first sub-step that failed.

### Transitions

```go
func Next(s State, r Result) State
```

`Next` is the default transition:

| Result | Next state |
|---|---|
| `Aborted` | `Aborted` |
| `Err != nil` | `Failed` |
| success | unchanged |

A custom transition usually handles its own cases and leaves the rest to `Next`:

```go
p.Transition = func(s pipelines.State, r pipelines.Result) pipelines.State {
	switch r.ExitCode {
	case 3:
		return "degraded" // custom: the pipeline goes on
	case 75:
		return pipelines.Succeeded // final: skip the remaining steps
	}
	return pipelines.Next(s, r)
}
```

Notes:

- A transition may map a failed step to a non-final state. The error is then dropped, as with `Continue`.
- A transition may map a successful step to `Failed` or `Aborted`. The cause in `Run`'s error is then `state set by Transition`.
- A failure dropped by a policy arrives as a success with exit code `0`. To branch on an exit code, map it here instead of dropping it with `Continue`.

### Jobs

Jobs are ordinary steps registered per state:

```go
p.On("degraded", warn)
p.On(pipelines.Failed, rollback, notify)
p.On(pipelines.Aborted, cleanup)
```

- **When:** after every step that leaves the pipeline in the job's state, whether or not the state changed. Jobs of `Running` therefore run after each successful step, and jobs of `Succeeded` once at the end. No job runs when `Run` enters `Running` at the start.
- **Order:** the jobs of a state run one after another, in the order registered, before the next step starts.
- **Cancellation:** jobs run with a context that is never cancelled, so that cleanup after an abort completes. Bound a job that may hang with its own timeout.
- **Failure:** a failing job is logged. It changes neither the state nor `Run`'s return value, and the remaining jobs still run.
- **Policies:** jobs run under an empty policy; wrap one with `WithPolicy` to retry it.
- **Output:** a job's output and log entries go to the pipeline's stream. Its entries carry the field `on=<state>`.

## Running several pipelines

### func RunPipelinesConcurrently

```go
func RunPipelinesConcurrently(ctx context.Context, pipelines []Pipeline, opts ...RunOption) error
```

Runs every pipeline of the slice in its own goroutine and waits for all of them.

- **Failure:** by default the first pipeline to fail cancels the others; those still running end `Aborted`. With `ContinueOnError()` the others keep running.
- **Return value:** `nil` if no pipeline returned an error, otherwise `one or more pipelines failed:` followed by every pipeline's error. `errors.Is` and `errors.As` see through to the causes.
- **State:** the slice's elements are run in place. Read `State` and `LogDir` from the slice afterwards, not from variables the slice was built from.
- **Logger:** as with `Run`, a plain `Logger` is started if `ctx` has none.

### type RunOption

```go
type RunOption func(*runOptions)

func ContinueOnError() RunOption
```

## Logging

### type Logger

```go
type Logger struct {
	*logrus.Logger
	Dir string
}

func NewLogger(ctx context.Context, dir string, tui bool) (context.Context, *Logger, error)
func LoggerFromContext(ctx context.Context) *Logger
func (l *Logger) Close()
```

All output is queued on one channel, and a single goroutine routes each line by the pipeline it belongs to. A line of pipeline `p` goes to stream `p` and to the stream `All`; a line of no pipeline goes to `All` only.

**`NewLogger`** starts a `Logger` and returns it with a context that carries it. Pass that context to `Run` and `RunPipelinesConcurrently`.

| Argument | Meaning |
|---|---|
| `dir` | Directory of the log files; created if missing. Empty means `PipelinesDir(ctx)`. |
| `tui` | `true`: the terminal becomes the [viewer](#terminal-viewer). `false`: the lines of `All` are printed to stdout. |

It returns an error if the directory or `all.log` cannot be created, or if `tui` is set and no terminal is available.

**Files.** Colour codes are stripped.

| File | Content |
|---|---|
| `<Dir>/all.log` | Every line. Truncated by `NewLogger`. |
| `<LogDir>/<Name>.log` | The lines of one pipeline. Truncated by each `Run`. |

**Structured entries.** The embedded `*logrus.Logger` works as usual: `SetLevel`, `SetFormatter`, `AddHook`, `WithField`, `Info`, and so on. The default level is `Info` and the default formatter is `PipelineFormatter`. An entry is routed by its `pipeline` field; use [`Log`](#func-log) to have it set. `Fatal` closes the `Logger` before exiting, which in viewer mode waits for the user to quit.

**`Close`** completes the log files. In viewer mode it then keeps the viewer open until the user quits it, or returns at once if the context given to `NewLogger` has been cancelled. Call it after every pipeline has returned. Calling it again does nothing. Entries logged after `Close` go to stderr.

**`LoggerFromContext`** returns the `Logger` carried by `ctx`, or `nil`.

### Terminal viewer

One tab per stream: `all` first, then each pipeline in order of appearance. The tab shows the pipeline's state: `*` running, `✓` succeeded, `✗` failed, and `[name]` for `Aborted` and custom states.

| Key | Action |
|---|---|
| Tab, Right | Next tab |
| Left | Previous tab |
| Up, Down | Scroll one line |
| PgUp, PgDn | Scroll one page |
| q, Ctrl-C | While pipelines run: cancel the context returned by `NewLogger`. After `Close` was called: quit. |

A tab follows its log until it is scrolled up, and follows again once scrolled back to the end.

In the viewer the terminal is in raw mode, so Ctrl-C does not raise `SIGINT`; it cancels the returned context instead. `SIGINT` and `SIGTERM` from outside still work when the context passed to `NewLogger` comes from `signal.NotifyContext`.

### func Log

```go
func Log(ctx context.Context) *logrus.Entry
```

Returns an entry for the `Logger` in `ctx`, bound to the pipeline running in `ctx`. Use it inside steps, jobs and callbacks so that the entry lands in that pipeline's tab and file:

```go
pipelines.Log(ctx).WithField("step", "upload").Warn("slow connection")
```

Without a `Logger` in `ctx` it uses logrus's standard logger.

### type PipelineFormatter

```go
type PipelineFormatter struct{}

func (PipelineFormatter) Format(e *logrus.Entry) ([]byte, error)
```

A `logrus.Formatter` that renders entries as

```
LEVL[15:04:05](pipeline)(step)(progress) message  key=value ...
```

The fields `pipeline`, `step` and `progress` are shown in front, only if present. The other fields follow the message, sorted by key. Colours are used when stdout is a terminal.

### Entries logged by the package

| Level | Message | Fields |
|---|---|---|
| Info | `state: <state>` | `pipeline` |
| Debug | `step started` | `pipeline`, `step`, `progress` |
| Info | `step completed` | `pipeline`, `step`, `progress` |
| Warn | `step interrupted` | `pipeline`, `step`, `progress` |
| Error | `step failed` | `pipeline`, `step`, `progress`, `error` |
| Warn | `attempt <n> failed, retrying` | `pipeline`, `step`, `error` |
| Warn | `step failed, continuing` / `step failed, rerunning` | `pipeline`, `step`, `error` |
| Error | `pipeline failed` / `pipeline aborted` | `pipeline`, `error` |
| Debug | `executing command` | `pipeline`, `step`, `executable`, `args`, `environment` |
| Info / Error | `all pipelines executed successfully` / `one or more pipelines failed` | none |

`progress` is `<index>/<count>` for the steps of a pipeline. Sub-steps of a `ConcurrentStep` have none, and jobs carry `on` instead.

### const All

```go
const All = "all"
```

The name of the stream every line is copied to: the first tab and `all.log`.

## Utilities

### func PipelinesDir

```go
func PipelinesDir(ctx context.Context) string
```

The package's default directory, taken from the first of: the context value under the string key `"pipelinesDir"`, the environment variable `PIPELINES_DIR`, `$HOME/.pipelines`.

### func CopyFile

```go
func CopyFile(src string, dst string) error
```

Copies the content of `src` to `dst`, creating or truncating `dst`, and syncs it to disk.

### Config locks

```go
func WithLockToken(ctx context.Context) context.Context
func AcquireConfigLock(ctx context.Context, symb string) error
func ReleaseConfigLock(ctx context.Context, symb string) error
```

A lock named `symb`, backed by the file `<PipelinesDir>/<symb>.lock`.

- `WithLockToken` returns a context that identifies one owner.
- `AcquireConfigLock` blocks until the lock is acquired or `ctx` is done. An owner that already holds the lock acquires it again at once; each acquisition needs its own release.
- `ReleaseConfigLock` returns an error if the lock is not held, or is held by another owner.
- An empty `symb` is an error.

```go
ctx = pipelines.WithLockToken(ctx)
if err := pipelines.AcquireConfigLock(ctx, "deploy"); err != nil {
	return err
}
defer pipelines.ReleaseConfigLock(ctx, "deploy")
```

### Time helpers

```go
func RemainingTime(timestamp string) (time.Duration, error)
func RemainingTimeHuman(timestamp string) (string, error)
func ShorterEol(eol1, eol2 string) (string, error)
```

Timestamps are RFC 3339, for example `2026-09-03T13:57:17.560506+00:00`.

- `RemainingTime` returns the time from now until the timestamp; negative if it is in the past.
- `RemainingTimeHuman` returns it as text, such as `1d 2h 3m 4s remaining` or `5m 10s ago`.
- `ShorterEol` returns the earlier of two timestamps.

## Limitations

- **Late output panics.** Writing to a `StepWriter` or running a pipeline after `Logger.Close` panics. Close the logger only after every pipeline has returned and no step has left goroutines behind that still write.
- **Jobs cannot be cancelled.** A job that hangs keeps `Run` from returning.
- **One viewer per process.** gocui keeps its screen in a package-level variable.
- **Viewer memory.** Tabs keep every line for scrolling.
- **Tab bar.** Tabs that do not fit the terminal width are cut off.
- **Race detector.** Running the viewer under `-race` reports a data race inside awesome-gocui v1.1.0. Plain mode is clean.
- **Viewer failure.** If the viewer's main loop ends abnormally while output is still flowing, the logger can block.
