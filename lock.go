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

// lockState tracks reentrant ownership for a given lock directory.
type lockState struct {
	owner    uint64
	refCount int
}

var (
	locks   = make(map[string]*lockState)
	locksMu sync.Mutex

	lockRelease   = make(map[string]chan struct{})
	lockReleaseMu sync.Mutex
)

// releaseChanFor returns the current wakeup channel for lockDir, creating one if absent.
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

// resetReleaseChan replaces the wakeup channel for lockDir with a fresh one
// so the next waiter gets a new channel to select on.
func resetReleaseChan(lockPath string) {
	lockReleaseMu.Lock()
	defer lockReleaseMu.Unlock()
	lockRelease[lockPath] = make(chan struct{})
}

// configLockPath returns the lock file for symb. Each symbol gets an independent
// lock beside its logs, preventing unrelated landscapes from blocking one another.
// The optional argument is retained for compatibility with older callers that
// supplied LockDirKey directly; new callers must pass a symbol.
func configLockPath(ctx context.Context, symb string) (path string, err error) {
	if len(symb) == 0 {
		return "", fmt.Errorf("config lock: symbol must not be empty")
	}
	baseDir, ok := ctx.Value("landscape").(string)
	if !ok || baseDir == "" {
		return "", fmt.Errorf("config lock: missing or invalid landscape in context")
	}
	return filepath.Join(baseDir, "hdlogs", symb, ".lock"), nil
}

// AcquireConfigLock acquires a filesystem-based reentrant lock for symb.
// The lock file is created at $LANDSCAPE_HOME/hdlogs/{symb}/.lock. It blocks
// until the lock is acquired or the context is cancelled.
func AcquireConfigLock(ctx context.Context, symb string) error {
	lockPath, err := configLockPath(ctx, symb)
	if err != nil {
		return fmt.Errorf("acquiring config lock: %w", err)
	}

	ownerToken := lockTokenFromCtx(ctx)
	if ownerToken != 0 {
		locksMu.Lock()
		state, exists := locks[lockPath]
		if exists && state.owner == ownerToken && state.refCount > 0 {
			state.refCount++
			locksMu.Unlock()
			return nil
		}
		locksMu.Unlock()
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		var acquireErr error
		if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
			return fmt.Errorf("acquiring config lock at %s: %w", lockPath, err)
		}
		var file *os.File
		file, acquireErr = os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if acquireErr == nil {
			_ = file.Close()
		}
		if acquireErr == nil {
			locksMu.Lock()
			locks[lockPath] = &lockState{owner: ownerToken, refCount: 1}
			locksMu.Unlock()
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
	locksMu.Lock()
	defer locksMu.Unlock()

	state, exists := locks[lockPath]
	if !exists || state.refCount <= 0 {
		return fmt.Errorf("releasing config lock at %s: lock not held", lockPath)
	}
	if ownerToken != 0 && state.owner != ownerToken {
		return fmt.Errorf("releasing config lock at %s: not owned by this context (owner token: %d, caller token: %d)", lockPath, state.owner, ownerToken)
	}

	state.refCount--
	if state.refCount == 0 {
		delete(locks, lockPath)
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
