package pipelines

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type lockTokenKey struct{}

// lockTokenCounter generates monotonically increasing reentrancy tokens.
var lockTokenCounter atomic.Uint64

// WithLockToken returns a child context that carries a fresh, unique lock token.
// Call this once per logical "owner" (e.g. at the top of each command handler)
// before passing the context to AcquireConfigLock.
func WithLockToken(ctx context.Context) context.Context {
	return context.WithValue(ctx, lockTokenKey{}, lockTokenCounter.Add(1))
}

// lockTokenFromCtx extracts the reentrancy token from ctx, or 0 if none is set.
func lockTokenFromCtx(ctx context.Context) uint64 {
	v, _ := ctx.Value(lockTokenKey{}).(uint64)
	return v
}

// lockState tracks reentrant ownership for a given lock path.
// refCount is updated atomically; owner is written once at acquisition time
// (while no concurrent releaser can exist) so plain uint64 is fine.
type lockState struct {
	owner    uint64
	refCount atomic.Int32
}

// locks maps lock-file path -> *lockState.
var locks sync.Map

var (
	lockRelease   = make(map[string]chan struct{})
	lockReleaseMu sync.Mutex
)

// releaseChanFor returns the current wakeup channel for lockPath, creating one if absent.
func releaseChanFor(lockPath string) chan struct{} {
	lockReleaseMu.Lock()
	defer lockReleaseMu.Unlock()
	if ch, ok := lockRelease[lockPath]; ok {
		return ch
	}
	ch := make(chan struct{})
	lockRelease[lockPath] = ch
	return ch
}

// resetReleaseChan replaces the wakeup channel for lockPath with a fresh one
// so the next waiter gets a new channel to select on.
func resetReleaseChan(lockPath string) {
	lockReleaseMu.Lock()
	defer lockReleaseMu.Unlock()
	lockRelease[lockPath] = make(chan struct{})
}

// configLockPath returns the lock file path for symb.
func configLockPath(ctx context.Context, symb string) (string, error) {
	if len(symb) == 0 {
		return "", fmt.Errorf("config lock: symbol must not be empty")
	}
	return filepath.Join(PipelinesDir(ctx), fmt.Sprintf("%s.lock", symb)), nil
}

// AcquireConfigLock acquires a filesystem-based reentrant lock for symb.
// It blocks until the lock is acquired or the context is cancelled.
func AcquireConfigLock(ctx context.Context, symb string) error {
	lockPath, err := configLockPath(ctx, symb)
	if err != nil {
		return fmt.Errorf("acquiring config lock: %w", err)
	}

	ownerToken := lockTokenFromCtx(ctx)

	// Re-entrant fast path: if this owner already holds the lock, bump refCount.
	if ownerToken != 0 {
		if v, ok := locks.Load(lockPath); ok {
			state := v.(*lockState)
			if state.owner == ownerToken && state.refCount.Load() > 0 {
				state.refCount.Add(1)
				return nil
			}
		}
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
			return fmt.Errorf("acquiring config lock at %s: %w", lockPath, err)
		}

		file, acquireErr := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if acquireErr == nil {
			_ = file.Close()
			state := &lockState{owner: ownerToken}
			state.refCount.Store(1)
			locks.Store(lockPath, state)
			resetReleaseChan(lockPath)
			return nil
		}
		if !os.IsExist(acquireErr) {
			return fmt.Errorf("acquiring config lock at %s: %w", lockPath, acquireErr)
		}

		releaseCh := releaseChanFor(lockPath)
		select {
		case <-ctx.Done():
			return fmt.Errorf("acquiring config lock at %s: %w", lockPath, ctx.Err())
		case <-releaseCh:
		case <-ticker.C:
		}
	}
}

// ReleaseConfigLock releases the filesystem-based reentrant lock for symb.
func ReleaseConfigLock(ctx context.Context, symb string) error {
	lockPath, err := configLockPath(ctx, symb)
	if err != nil {
		return fmt.Errorf("releasing config lock: %w", err)
	}

	ownerToken := lockTokenFromCtx(ctx)

	v, ok := locks.Load(lockPath)
	if !ok {
		return fmt.Errorf("releasing config lock at %s: lock not held", lockPath)
	}
	state := v.(*lockState)

	if state.refCount.Load() <= 0 {
		return fmt.Errorf("releasing config lock at %s: lock not held", lockPath)
	}
	if ownerToken != 0 && state.owner != ownerToken {
		return fmt.Errorf("releasing config lock at %s: not owned by this context (owner token: %d, caller token: %d)",
			lockPath, state.owner, ownerToken)
	}

	if state.refCount.Add(-1) == 0 {
		locks.Delete(lockPath)
		if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("releasing config lock at %s: %w", lockPath, err)
		}
		lockReleaseMu.Lock()
		if ch, ok := lockRelease[lockPath]; ok {
			close(ch)
			delete(lockRelease, lockPath)
		}
		lockReleaseMu.Unlock()
	}
	return nil
}
