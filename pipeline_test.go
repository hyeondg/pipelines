package pipelines_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/codeGROOVE-dev/retry"
	"github.com/hyeondg/pipelines"
)

// failing returns a step that fails its first n executions, and the counter of executions.
func failing(name string, n int) (*pipelines.FuncStep, *int) {
	runs := new(int)
	return &pipelines.FuncStep{Name: name, Fn: func(_ context.Context, sw *pipelines.StepWriter) error {
		*runs++
		fmt.Fprintf(sw, "%s run %d\n", name, *runs)
		if *runs <= n {
			return errors.New("boom")
		}
		return nil
	}}, runs
}

func logger(t *testing.T) (context.Context, *pipelines.Logger) {
	t.Helper()
	ctx, l, err := pipelines.NewLogger(context.Background(), t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Close)
	return ctx, l
}

func TestPolicy(t *testing.T) {
	always := 1 << 30
	for name, tc := range map[string]struct {
		fails    int // executions that fail
		policy   pipelines.Policy
		wantRuns int
		wantErr  bool
		wantNext bool // the step after it runs
	}{
		"success":            {0, pipelines.Policy{}, 1, false, true},
		"abort by default":   {always, pipelines.Policy{}, 1, true, false},
		"retry until ok":     {2, pipelines.Policy{Retry: []retry.Option{retry.Attempts(3), retry.Delay(0), retry.MaxJitter(0)}}, 3, false, true},
		"retry exhausted":    {always, pipelines.Policy{Retry: []retry.Option{retry.Attempts(3), retry.Delay(0), retry.MaxJitter(0)}}, 3, true, false},
		"continue":           {always, pipelines.Policy{OnFailure: pipelines.Always(pipelines.Continue)}, 1, false, true},
		"callback and abort": {always, pipelines.Policy{OnFailure: pipelines.Always(pipelines.Abort)}, 1, true, false},
	} {
		for _, where := range []string{"pipeline policy", "step policy"} {
			t.Run(name+"/"+where, func(t *testing.T) {
				ctx, _ := logger(t)
				step, runs := failing("flaky", tc.fails)
				next, nextRuns := failing("next", 0)
				var succeeded, failed int
				pol := tc.policy
				pol.OnSuccess = func(_ context.Context, s pipelines.Step) {
					if s.StepName() == "flaky" {
						succeeded++
					}
				}
				if decide := pol.OnFailure; decide != nil {
					pol.OnFailure = func(ctx context.Context, s pipelines.Step, err error) pipelines.Action {
						failed++
						return decide(ctx, s, err)
					}
				}
				p := pipelines.Pipeline{Name: "p", Steps: []pipelines.Step{step, next}, Policy: pol}
				if where == "step policy" {
					p = pipelines.Pipeline{Name: "p", Steps: []pipelines.Step{pipelines.WithPolicy(step, pol), next}}
				}
				err := p.Run(ctx)
				if (err != nil) != tc.wantErr || *runs != tc.wantRuns || (*nextRuns == 1) != tc.wantNext {
					t.Fatalf("err=%v runs=%d nextRuns=%d", err, *runs, *nextRuns)
				}
				wantSucceeded, wantFailed := 0, 0
				if tc.fails < tc.wantRuns { // it got through eventually
					wantSucceeded = 1
				} else if tc.policy.OnFailure != nil {
					wantFailed = 1
				}
				if succeeded != wantSucceeded || failed != wantFailed {
					t.Fatalf("OnSuccess called %d times (want %d), OnFailure %d times (want %d)", succeeded, wantSucceeded, failed, wantFailed)
				}
			})
		}
	}
}

func TestRerun(t *testing.T) {
	ctx, _ := logger(t)
	step, runs := failing("flaky", 2)
	p := pipelines.Pipeline{Name: "p", Policy: pipelines.Policy{OnFailure: pipelines.Always(pipelines.Rerun)}}
	p.AddStep(step)
	if err := p.Run(ctx); err != nil || *runs != 3 {
		t.Fatalf("err=%v runs=%d", err, *runs)
	}
}

func TestCancellationIsNotAFailure(t *testing.T) {
	base, _ := logger(t)
	ctx, cancel := context.WithCancel(base)
	runs, called := 0, false
	p := pipelines.Pipeline{Name: "p", Policy: pipelines.Policy{
		Retry:     []retry.Option{retry.Attempts(5), retry.Delay(0)},
		OnFailure: func(context.Context, pipelines.Step, error) pipelines.Action { called = true; return pipelines.Rerun },
	}}
	p.AddFunc("wait", "", func(ctx context.Context, _ *pipelines.StepWriter) error {
		runs++
		cancel()
		<-ctx.Done()
		return ctx.Err()
	})
	if err := p.Run(ctx); !errors.Is(err, context.Canceled) || runs != 1 || called {
		t.Fatalf("err=%v runs=%d OnFailure called=%v", err, runs, called)
	}
}

func TestConcurrentStep(t *testing.T) {
	ctx, _ := logger(t)
	tolerated, _ := failing("tolerated", 1)
	var active, peak atomic.Int32
	gate := make(chan struct{})
	step := func(name string, fn func(ctx context.Context) error) pipelines.Step {
		return &pipelines.FuncStep{Name: name, Fn: func(ctx context.Context, _ *pipelines.StepWriter) error {
			peak.Store(max(peak.Load(), active.Add(1)))
			defer active.Add(-1)
			return fn(ctx)
		}}
	}
	p := pipelines.Pipeline{Name: "p"}
	p.AddStep(&pipelines.ConcurrentStep{Name: "tolerant", Steps: []pipelines.Step{
		pipelines.WithPolicy(tolerated, pipelines.Policy{OnFailure: pipelines.Always(pipelines.Continue)}),
	}})
	p.AddStep(&pipelines.ConcurrentStep{Name: "group", MaxConcurrency: 2, Steps: []pipelines.Step{
		step("slow", func(ctx context.Context) error { close(gate); <-ctx.Done(); return ctx.Err() }), // ends only when cancelled
		step("bad", func(context.Context) error { <-gate; return errors.New("boom") }),
		step("late", func(context.Context) error { return nil }),
	}})
	err := p.Run(ctx)
	if want := "pipeline 'p' failed at step 'group': sub-step 'bad': boom"; err == nil || err.Error() != want {
		t.Fatalf("err=%v, want %s", err, want)
	}
	if peak.Load() > 2 {
		t.Fatalf("%d sub-steps ran at once, limit is 2", peak.Load())
	}
}

// exit returns a command step that exits with the given code.
func exit(name string, code int) *pipelines.Command {
	return &pipelines.Command{Name: name, Executable: "sh", Args: []string{"-c", fmt.Sprintf("exit %d", code)}}
}

// TestStateMachine checks the state after each step and the jobs that were run for it.
func TestStateMachine(t *testing.T) {
	byExitCode := func(s pipelines.State, r pipelines.Result) pipelines.State {
		switch r.ExitCode {
		case 3:
			return "degraded" // not final: the pipeline goes on
		case 4:
			return pipelines.Succeeded // final: the remaining steps are skipped
		}
		return pipelines.Next(s, r)
	}
	for name, tc := range map[string]struct {
		codes      []int // exit codes of the steps
		transition func(pipelines.State, pipelines.Result) pipelines.State
		wantState  pipelines.State
		wantErr    bool
		wantJobs   string // states whose job ran, in order
	}{
		"all succeed":      {[]int{0, 0}, nil, pipelines.Succeeded, false, "running running succeeded "},
		"default failure":  {[]int{0, 3, 0}, nil, pipelines.Failed, true, "running failed "},
		"custom state":     {[]int{0, 3, 0}, byExitCode, "degraded", false, "running degraded degraded "},
		"custom then fail": {[]int{3, 1, 0}, byExitCode, pipelines.Failed, true, "degraded failed "},
		"early success":    {[]int{4, 1}, byExitCode, pipelines.Succeeded, false, "succeeded "},
		"no steps":         {nil, nil, pipelines.Succeeded, false, "succeeded "},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, _ := logger(t)
			p := pipelines.Pipeline{Name: "p", Transition: tc.transition}
			for i, code := range tc.codes {
				p.AddStep(exit(fmt.Sprint("step", i), code))
			}
			var jobs string
			for _, state := range []pipelines.State{pipelines.Running, pipelines.Succeeded, pipelines.Failed, pipelines.Aborted, "degraded"} {
				p.On(state, &pipelines.FuncStep{Name: "job", Fn: func(context.Context, *pipelines.StepWriter) error {
					if p.State != state {
						t.Errorf("job of %s ran in state %s", state, p.State)
					}
					jobs += string(state) + " "
					return errors.New("a failing job changes nothing")
				}})
			}
			err := p.Run(ctx)
			if p.State != tc.wantState || (err != nil) != tc.wantErr || jobs != tc.wantJobs {
				t.Fatalf("state=%s err=%v jobs=%q", p.State, err, jobs)
			}
		})
	}
}

// TestAbortedJobs checks that a cancelled pipeline becomes Aborted and that its jobs still run to completion.
func TestAbortedJobs(t *testing.T) {
	base, _ := logger(t)
	ctx, cancel := context.WithCancel(base)
	cleaned := false
	p := pipelines.Pipeline{Name: "p"}
	p.AddFunc("wait", "", func(ctx context.Context, _ *pipelines.StepWriter) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	})
	p.AddStep(exit("never", 0))
	p.On(pipelines.Aborted, &pipelines.FuncStep{Name: "cleanup", Fn: func(ctx context.Context, sw *pipelines.StepWriter) error {
		if err := (&pipelines.Command{Executable: "true"}).Execute(ctx, sw); err != nil { // would be killed if ctx were cancelled
			return err
		}
		cleaned = true
		return nil
	}})
	if err := p.Run(ctx); !errors.Is(err, context.Canceled) || p.State != pipelines.Aborted || !cleaned {
		t.Fatalf("state=%s err=%v cleaned=%v", p.State, err, cleaned)
	}
}

// TestDemux checks that each pipeline's file holds exactly its own lines,
// complete when Run returns, and that all.log holds everything.
func TestDemux(t *testing.T) {
	ctx, l := logger(t)
	var ps []pipelines.Pipeline
	for _, name := range []string{"alpha", "beta"} {
		p := pipelines.Pipeline{Name: name}
		p.AddFunc("work", "", func(ctx context.Context, sw *pipelines.StepWriter) error {
			for i := range 50 {
				fmt.Fprintf(sw, "%s line %d\npartial ", name, i)
				fmt.Fprintf(sw, "%s\n", name)
			}
			pipelines.Log(ctx).Info("event of " + name)
			fmt.Fprint(sw, "no newline of "+name)
			return nil
		})
		ps = append(ps, p)
	}
	if err := pipelines.RunPipelinesConcurrently(ctx, ps); err != nil {
		t.Fatal(err)
	}
	read := func(path string) string {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	for i, name := range []string{"alpha", "beta"} {
		got, other := read(ps[i].GetLogFilePath()), []string{"beta", "alpha"}[i]
		for _, want := range []string{"[" + name + "/work] " + name + " line 49\n", "[" + name + "/work] partial " + name + "\n", "event of " + name, "[" + name + "/work] no newline of " + name + "\n", "state: succeeded"} {
			if !strings.Contains(got, want) {
				t.Errorf("%s.log lacks %q", name, want)
			}
		}
		if strings.Contains(got, other) || strings.Contains(got, "\x1b") {
			t.Errorf("%s.log has foreign lines or escape codes:\n%s", name, got)
		}
		if n := strings.Count(got, " line "); n != 50 {
			t.Errorf("%s.log has %d of 50 lines", name, n)
		}
	}
	l.Info("after the run")
	l.Close()
	all := read(l.Dir + "/all.log")
	for _, want := range []string{"alpha line 49", "beta line 49", "all pipelines executed successfully", "after the run"} {
		if !strings.Contains(all, want) {
			t.Errorf("all.log lacks %q", want)
		}
	}
}

func TestStepWriterStandalone(t *testing.T) {
	var buf bytes.Buffer
	sw := pipelines.New(&buf)
	step, _ := failing("solo", 0)
	if err := step.Execute(context.Background(), sw); err != nil || buf.String() != "solo run 1\n" {
		t.Fatalf("err=%v out=%q", err, buf.String())
	}
}

// TestRunWithoutLogger checks that a pipeline run with a bare context still logs to stdout and to files.
func TestRunWithoutLogger(t *testing.T) {
	dir := t.TempDir()
	p := pipelines.Pipeline{Name: "p"}
	p.AddCommand(pipelines.Command{Name: "echo", Executable: "echo", Args: []string{"hi"}})
	if err := p.Run(context.WithValue(context.Background(), "pipelinesDir", dir)); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dir + "/p.log"); !strings.Contains(string(b), "[p/echo] hi\n") {
		t.Fatalf("p.log: %q", b)
	}
}
