// example demonstrates the pipelines package.
//
// It runs "build" on its own and then "verify", "report" and "deploy"
// concurrently. "deploy" shows the pipeline state machine. Each pipeline has
// its own tab in the terminal and its own log file; the "all" tab and all.log
// carry everything.
//
// Run from the repo root:
//
//	go run ./example          # gocui viewer
//	go run ./example -plain   # plain lines on stdout
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codeGROOVE-dev/retry"
	"github.com/hyeondg/pipelines"
	"github.com/sirupsen/logrus"
)

func main() {
	plain := flag.Bool("plain", false, "print the log to stdout instead of opening the viewer")
	debug := flag.Bool("debug", false, "log at debug level")
	flag.Parse()

	// SIGINT and SIGTERM cancel the pipelines. In the viewer, so do q and Ctrl-C.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logDir, err := os.MkdirTemp("", "pipelines-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, log, err := pipelines.NewLogger(ctx, logDir, !*plain)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *debug {
		log.SetLevel(logrus.DebugLevel)
	}

	ran, err := run(ctx, log)
	log.Close() // in the viewer: returns once the user quits it
	for _, p := range ran {
		fmt.Printf("%-8s %s\n", p.Name, p.State)
	}
	fmt.Println("logs are in", logDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run runs the pipelines and returns them, each in its final state.
func run(ctx context.Context, log *pipelines.Logger) ([]pipelines.Pipeline, error) {
	// 1. build: a single command. Its pipeline policy registers callbacks; a
	//    failure is reported and, as OnFailure says, aborts the pipeline.
	build := pipelines.Pipeline{
		Name:        "build",
		Description: "Compile the module",
		Policy: pipelines.Policy{
			OnSuccess: func(ctx context.Context, s pipelines.Step) {
				pipelines.Log(ctx).Infof("callback: %s succeeded", s.StepName())
			},
			OnFailure: func(ctx context.Context, s pipelines.Step, err error) pipelines.Action {
				pipelines.Log(ctx).Errorf("callback: %s failed: %v", s.StepName(), err)
				return pipelines.Abort
			},
		},
	}
	build.AddCommand(pipelines.Command{
		Name:        "compile",
		Description: "compile all packages",
		Executable:  "go",
		Args:        []string{"build", "./..."},
	})

	// 2. verify: vet, echo and the test suite at the same time; the tests are
	//    retried up to three times, one second apart.
	verify := pipelines.Pipeline{Name: "verify", Description: "Static analysis and test suite"}
	verify.AddStep(&pipelines.ConcurrentStep{
		Name:        "checks",
		Description: "run vet, echo and tests at the same time",
		Steps: []pipelines.Step{
			&pipelines.Command{Name: "vet", Description: "static analysis", Executable: "go", Args: []string{"vet", "./..."}},
			&pipelines.Command{Name: "echo", Description: "say hello", Executable: "echo", Args: []string{"Hello, world"}},
			pipelines.WithRetry(
				&pipelines.Command{Name: "test", Description: "run test suite", Executable: "go", Args: []string{"test", "-count=1", "."}},
				retry.Attempts(3), retry.Delay(time.Second), retry.DelayType(retry.FixedDelay),
			),
		},
	})

	// 3. report: two Go functions writing slowly at the same time.
	slow := func(name string, pause time.Duration) *pipelines.FuncStep {
		return &pipelines.FuncStep{
			Name:        name,
			Description: "write a few lines slowly",
			Fn: func(ctx context.Context, sw *pipelines.StepWriter) error {
				for i := 1; i <= 5; i++ {
					fmt.Fprintf(sw, "line %d/5\n", i)
					select {
					case <-time.After(pause):
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
		}
	}
	report := pipelines.Pipeline{Name: "report", Description: "Print a post-run summary"}
	report.AddStep(&pipelines.ConcurrentStep{
		Name:        "summaries",
		Description: "two writers at the same time",
		Steps:       []pipelines.Step{slow("checks", 400*time.Millisecond), slow("coverage", 250*time.Millisecond)},
	})

	// 4. deploy: a state machine. Policies deal with single steps first; then
	//    Transition maps what is left of each step to a state, here from its
	//    exit code, and the jobs registered for that state are run.
	var uploads atomic.Int32
	say := func(name, text string) *pipelines.Command {
		return &pipelines.Command{Name: name, Description: text, Executable: "echo", Args: []string{text}}
	}
	deploy := pipelines.Pipeline{
		Name:        "deploy",
		Description: "State machine",
		Transition: func(s pipelines.State, r pipelines.Result) pipelines.State {
			if r.ExitCode == 3 {
				return "degraded" // not a final state: the pipeline goes on
			}
			return pipelines.Next(s, r)
		},
	}
	deploy.On("degraded", say("warn", "running with pending migrations"))
	deploy.On(pipelines.Failed, say("rollback", "restoring the previous release"))
	deploy.On(pipelines.Aborted, say("cleanup", "removing the partial upload"))

	deploy.AddStep(pipelines.WithPolicy( // retried: succeeds at the third attempt
		&pipelines.FuncStep{Name: "upload", Description: "flaky upload", Fn: func(_ context.Context, sw *pipelines.StepWriter) error {
			if n := uploads.Add(1); n < 3 {
				return fmt.Errorf("connection reset (attempt %d)", n)
			}
			fmt.Fprintln(sw, "uploaded")
			return nil
		}},
		pipelines.Policy{Retry: []retry.Option{retry.Attempts(5), retry.Delay(500 * time.Millisecond), retry.DelayType(retry.FixedDelay)}},
	))
	deploy.AddStep(pipelines.WithPolicy( // tolerated: the policy drops the failure, the state is unaffected
		&pipelines.Command{Name: "notify", Description: "optional notification", Executable: "false"},
		pipelines.Policy{OnFailure: pipelines.Always(pipelines.Continue)},
	))
	deploy.AddCommand(pipelines.Command{ // exit code 3: the pipeline becomes "degraded"
		Name: "migrate", Description: "schema migration", Executable: "sh", Args: []string{"-c", "echo 2 migrations pending; exit 3"},
	})
	deploy.AddFunc("smoke", "smoke test", func(context.Context, *pipelines.StepWriter) error { // any other failure: Failed
		return errors.New("health check returned 503")
	})
	deploy.AddFunc("announce", "never reached", func(_ context.Context, sw *pipelines.StepWriter) error {
		fmt.Fprintln(sw, "deployed")
		return nil
	})

	all := []pipelines.Pipeline{verify, report, deploy}
	for _, p := range append([]pipelines.Pipeline{build}, all...) {
		log.Info(p.HelpText())
	}

	// Sequential: build must pass before anything else starts.
	if err := build.Run(ctx); err != nil {
		return []pipelines.Pipeline{build}, err
	}
	// Concurrent: a failing pipeline does not stop the others.
	err := pipelines.RunPipelinesConcurrently(ctx, all, pipelines.ContinueOnError())
	return append([]pipelines.Pipeline{build}, all...), err
}
