package pipelines_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hyeondg/pipelines"
)

// landscape returns a context with a temp landscape dir, which configLockPath requires.
func landscape(t *testing.T) context.Context {
	t.Helper()
	return context.WithValue(context.Background(), "landscape", t.TempDir())
}

// TestAcquireRelease is the basic happy path: acquire then release.
func TestAcquireRelease(t *testing.T) {
	ctx := pipelines.WithLockToken(landscape(t))

	if err := pipelines.AcquireConfigLock(ctx, "sym"); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := pipelines.ReleaseConfigLock(ctx, "sym"); err != nil {
		t.Fatalf("release: %v", err)
	}
}

// TestReentrant verifies that the same token can acquire multiple times
// and requires the same number of releases before the lock is freed.
func TestReentrant(t *testing.T) {
	base := landscape(t)
	ctx := pipelines.WithLockToken(base)

	for i := 0; i < 3; i++ {
		if err := pipelines.AcquireConfigLock(ctx, "sym"); err != nil {
			t.Fatalf("acquire #%d: %v", i+1, err)
		}
	}
	// Two releases: lock still held.
	for i := 0; i < 2; i++ {
		if err := pipelines.ReleaseConfigLock(ctx, "sym"); err != nil {
			t.Fatalf("release #%d: %v", i+1, err)
		}
		// A second token sharing the same landscape should still be blocked.
		ctx2 := pipelines.WithLockToken(base)
		tctx, cancel := context.WithTimeout(ctx2, 300*time.Millisecond)
		defer cancel()
		if err := pipelines.AcquireConfigLock(tctx, "sym"); err == nil {
			t.Fatal("expected second token to be blocked, but it acquired the lock")
		}
	}
	// Final release: lock is now free.
	if err := pipelines.ReleaseConfigLock(ctx, "sym"); err != nil {
		t.Fatalf("final release: %v", err)
	}
}

// TestReleaseUnheld verifies that releasing a lock that was never acquired returns an error.
func TestReleaseUnheld(t *testing.T) {
	ctx := pipelines.WithLockToken(landscape(t))
	if err := pipelines.ReleaseConfigLock(ctx, "sym"); err == nil {
		t.Fatal("expected error releasing unheld lock, got nil")
	}
}

// TestContextCancellation verifies that a blocked Acquire returns promptly
// when the context is cancelled.
func TestContextCancellation(t *testing.T) {
	base := landscape(t)
	holder := pipelines.WithLockToken(base)
	if err := pipelines.AcquireConfigLock(holder, "sym"); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}
	defer pipelines.ReleaseConfigLock(holder, "sym") //nolint:errcheck

	waiter := pipelines.WithLockToken(base)
	ctx, cancel := context.WithTimeout(waiter, 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := pipelines.AcquireConfigLock(ctx, "sym"); err == nil {
		t.Fatal("expected cancellation error, got nil")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("acquire did not return promptly after cancellation: %v", elapsed)
	}
}

// TestWaiterUnblocksOnRelease verifies that a goroutine waiting on a lock
// is unblocked promptly when the holder releases.
func TestWaiterUnblocksOnRelease(t *testing.T) {
	base := landscape(t)
	holder := pipelines.WithLockToken(base)
	if err := pipelines.AcquireConfigLock(holder, "sym"); err != nil {
		t.Fatalf("holder acquire: %v", err)
	}

	waiter := pipelines.WithLockToken(base)
	acquired := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(waiter, 5*time.Second)
		defer cancel()
		acquired <- pipelines.AcquireConfigLock(ctx, "sym")
	}()

	// Give the waiter goroutine time to block.
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	if err := pipelines.ReleaseConfigLock(holder, "sym"); err != nil {
		t.Fatalf("holder release: %v", err)
	}

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("waiter acquire after release: %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("waiter took too long to unblock: %v", elapsed)
		}
		// Clean up.
		if err := pipelines.ReleaseConfigLock(waiter, "sym"); err != nil {
			t.Fatalf("waiter release: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never unblocked")
	}
}

// TestConcurrentAcquire verifies that N goroutines each acquire and release
// the lock exactly once, with no two holding it simultaneously.
func TestConcurrentAcquire(t *testing.T) {
	base := landscape(t)
	const n = 8

	var (
		wg            sync.WaitGroup
		mu            sync.Mutex // guards concurrent
		concurrent    int
		maxConcurrent int
	)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := pipelines.WithLockToken(base)
			tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			if err := pipelines.AcquireConfigLock(tctx, "sym"); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}

			mu.Lock()
			concurrent++
			if concurrent > maxConcurrent {
				maxConcurrent = concurrent
			}
			mu.Unlock()

			time.Sleep(20 * time.Millisecond) // hold briefly

			mu.Lock()
			concurrent--
			mu.Unlock()

			if err := pipelines.ReleaseConfigLock(ctx, "sym"); err != nil {
				t.Errorf("release: %v", err)
			}
		}()
	}

	wg.Wait()

	if maxConcurrent > 1 {
		t.Fatalf("lock held by %d goroutines simultaneously", maxConcurrent)
	}
}

// TestEmptySymbol verifies that an empty symbol is rejected.
func TestEmptySymbol(t *testing.T) {
	ctx := pipelines.WithLockToken(landscape(t))
	if err := pipelines.AcquireConfigLock(ctx, ""); err == nil {
		t.Fatal("expected error for empty symbol, got nil")
	}
}

// TestMissingLandscape verifies that a missing landscape key is rejected.
func TestMissingLandscape(t *testing.T) {
	ctx := pipelines.WithLockToken(context.Background())
	if err := pipelines.AcquireConfigLock(ctx, "sym"); err == nil {
		t.Fatal("expected error for missing landscape, got nil")
	}
}

// TestWrongOwnerCannotRelease verifies that a different token cannot release
// a lock it does not own.
func TestWrongOwnerCannotRelease(t *testing.T) {
	base := landscape(t)
	owner := pipelines.WithLockToken(base)
	other := pipelines.WithLockToken(base)

	if err := pipelines.AcquireConfigLock(owner, "sym"); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer pipelines.ReleaseConfigLock(owner, "sym") //nolint:errcheck

	if err := pipelines.ReleaseConfigLock(other, "sym"); err == nil {
		t.Fatal("expected error when wrong owner releases, got nil")
	}
}
