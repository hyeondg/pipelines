// example demonstrates the pipelines package.
//
// It runs "build" on its own and then "verify", "report" and "deploy"
// concurrently. Each pipeline has its own tab in the terminal and its own log
// file; the "all" tab and all.log carry everything.
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

	err = run(ctx, log)
	log.Close() // in the viewer: returns once the user quits it
	fmt.Println("logs are in", logDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *pipelines.Logger) error {
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

	// 4. deploy: one policy per way of handling a failure.
	var uploads atomic.Int32
	deploy := pipelines.Pipeline{Name: "deploy", Description: "Failure policies"}
	deploy.AddStep(pipelines.WithPolicy( // retry: succeeds at the third attempt
		&pipelines.FuncStep{Name: "upload", Description: "flaky upload", Fn: func(_ context.Context, sw *pipelines.StepWriter) error {
			if n := uploads.Add(1); n < 3 {
				return fmt.Errorf("connection reset (attempt %d)", n)
			}
			fmt.Fprintln(sw, "uploaded")
			return nil
		}},
		pipelines.Policy{Retry: []retry.Option{retry.Attempts(5), retry.Delay(500 * time.Millisecond), retry.DelayType(retry.FixedDelay)}},
	))
	deploy.AddStep(pipelines.WithPolicy( // continue: the failure is logged and ignored
		&pipelines.Command{Name: "notify", Description: "optional notification", Executable: "false"},
		pipelines.Policy{OnFailure: pipelines.Always(pipelines.Continue)},
	))
	deploy.AddStep(pipelines.WithPolicy( // failure callback, then abort
		&pipelines.FuncStep{Name: "smoke", Description: "smoke test", Fn: func(context.Context, *pipelines.StepWriter) error {
			return errors.New("health check returned 503")
		}},
		pipelines.Policy{OnFailure: func(ctx context.Context, s pipelines.Step, err error) pipelines.Action {
			pipelines.Log(ctx).Warnf("callback: rolling back after %s: %v", s.StepName(), err)
			return pipelines.Abort
		}},
	))
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
		return err
	}
	// Concurrent: a failing pipeline does not stop the others.
	return pipelines.RunPipelinesConcurrently(ctx, all, pipelines.ContinueOnError())
}
