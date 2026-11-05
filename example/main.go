// example demonstrates the pipelines package.
//
// It builds three pipelines:
//
//  1. "build"   – compiles the project with `go build`.
//  2. "verify"  – runs vet and tests concurrently, with retry on the test step.
//  3. "report"  – a pure-Go FuncStep that prints a summary.
//
// Pipelines 1 and 2 are executed sequentially (build must pass before verify).
// A second run then executes all three concurrently to show RunPipelinesConcurrently.
//
// Run from the repo root:
//
//	go run ./example
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codeGROOVE-dev/retry"
	"github.com/hyeondg/pipelines"
	"github.com/sirupsen/logrus"
)

func main() {
	// Wire OS signals so Ctrl-C cancels the pipeline gracefully.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Inject a shared logrus logger so every pipeline step writes structured logs.
	log := logrus.New()
	log.SetOutput(os.Stdout)
	log.SetFormatter(&logrus.TextFormatter{})
	log.SetLevel(logrus.InfoLevel)
	ctx = pipelines.ContextWithLogger(ctx, log)

	// -------------------------------------------------------------------------
	// 1. Build pipeline – single Command step.
	// -------------------------------------------------------------------------
	buildPipeline := pipelines.Pipeline{
		Name:        "build",
		Description: "Compile the module",
		LogDir:      os.TempDir(),
	}
	buildPipeline.AddCommand(pipelines.Command{
		Name:        "go-build",
		Description: "compile all packages",
		Executable:  "go",
		Args:        []string{"build", "./..."},
	})

	// -------------------------------------------------------------------------
	// 2. Verify pipeline – vet + test run concurrently; test step has retry.
	// -------------------------------------------------------------------------
	vetStep := pipelines.Command{
		Name:        "go-vet",
		Description: "static analysis",
		Executable:  "go",
		Args:        []string{"vet", "./..."},
	}

	testStep := pipelines.Command{
		Name:        "go-test",
		Description: "run test suite",
		Executable:  "go",
		Args:        []string{"test", "-count=1", "./..."},
	}

	// Wrap the test step: retry up to 3 times with a 1 s fixed delay.
	// Pass retry.Option values directly — no wrapper config needed.
	retryTestStep := pipelines.WithRetry(&testStep,
		retry.Attempts(3),
		retry.Delay(1*time.Second),
		retry.DelayType(retry.FixedDelay),
	)

	concurrentVerify := &pipelines.ConcurrentStep{
		Name:        "vet-and-test",
		Description: "run vet and tests at the same time",
		Steps:       []pipelines.Step{&vetStep, retryTestStep},
	}

	verifyPipeline := pipelines.Pipeline{
		Name:        "verify",
		Description: "Static analysis and test suite",
		LogDir:      os.TempDir(),
	}
	verifyPipeline.AddStep(concurrentVerify)

	// -------------------------------------------------------------------------
	// 3. Report pipeline – ConcurrentStep with two FuncSteps that both write
	//    multi-line blocks with a sleep in the middle.
	//    This tests atomicity: each block must appear whole, never interleaved.
	// -------------------------------------------------------------------------

	// blockStep returns a FuncStep that writes a labelled box, sleeping between
	// the top and bottom halves to maximise the interleave window.
	blockStep := func(label, symbol string, delay time.Duration) *pipelines.FuncStep {
		return &pipelines.FuncStep{
			Name:        label,
			Description: "write a labelled result block",
			Fn: func(_ context.Context, sw *pipelines.StepWriter) error {
				fmt.Fprintf(sw, "\n[%s] starting...\n", label)
				fmt.Fprintln(sw, "+------------------------------+")
				fmt.Fprintf(sw, "|  %-28s|\n", label)
				time.Sleep(delay)
				fmt.Fprintf(sw, "|  result: %-20s|\n", symbol)
				fmt.Fprintln(sw, "+------------------------------+")
				return nil
			},
		}
	}

	reportPipeline := pipelines.Pipeline{
		Name:        "report",
		Description: "Print a post-run summary",
		LogDir:      os.TempDir(),
	}
	reportPipeline.AddStep(&pipelines.ConcurrentStep{
		Name:        "parallel-report",
		Description: "two blocks written concurrently - must not interleave",
		Steps: []pipelines.Step{
			blockStep("checks", "all passed", 2*time.Second),
			blockStep("coverage", "87%", 1*time.Second),
		},
	})

	// -------------------------------------------------------------------------
	// Demo A: sequential execution – build must pass before verify.
	// -------------------------------------------------------------------------
	fmt.Println("\n=== Demo A: sequential execution ===")
	fmt.Println(buildPipeline.HelpText())

	if err := buildPipeline.Run(ctx); err != nil {
		log.WithError(err).Fatal("build pipeline failed")
	}
	if err := verifyPipeline.Run(ctx); err != nil {
		// Non-fatal: show the error and continue to the concurrent demo.
		log.WithError(err).Error("verify pipeline failed")
	}

	// -------------------------------------------------------------------------
	// Demo B: all three pipelines run concurrently.
	// -------------------------------------------------------------------------
	fmt.Println("\n=== Demo B: concurrent execution ===")

	// Fresh pipeline instances so log files are recreated cleanly.
	all := []pipelines.Pipeline{
		{
			Name:        "build-2",
			Description: "Compile (concurrent run)",
			LogDir:      os.TempDir(),
			Steps:       buildPipeline.Steps,
		},
		{
			Name:        "verify-2",
			Description: "Verify (concurrent run)",
			LogDir:      os.TempDir(),
			Steps:       verifyPipeline.Steps,
		},
		{
			Name:        "report-2",
			Description: "Report (concurrent run)",
			LogDir:      os.TempDir(),
			Steps:       reportPipeline.Steps,
		},
	}

	err := pipelines.RunPipelinesConcurrently(ctx, all, pipelines.ContinueOnError())
	if err != nil {
		// Unwrap multi-pipeline errors to decide on exit code.
		log.WithError(err).Error("one or more concurrent pipelines failed")
		if !errors.Is(err, context.Canceled) {
			os.Exit(1)
		}
	}
}
