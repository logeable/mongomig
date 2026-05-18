package shutdown

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Coordinator handles two-phase shutdown: first signal stops new work and allows
// the current tenant pipeline to finish; second signal or grace timeout forces cancel.
type Coordinator struct {
	grace    time.Duration
	forceCtx context.Context
	force    context.CancelFunc

	mu       sync.Mutex
	stopping bool
	forced   atomic.Bool

	graceDone chan struct{}
	once      sync.Once
}

// New creates a coordinator. grace<=0 defaults to 30s.
func New(grace time.Duration) *Coordinator {
	if grace <= 0 {
		grace = 30 * time.Second
	}
	forceCtx, force := context.WithCancel(context.Background())
	c := &Coordinator{
		grace:     grace,
		forceCtx:  forceCtx,
		force:     force,
		graceDone: make(chan struct{}),
	}
	go c.listenSignals()
	return c
}

func (c *Coordinator) listenSignals() {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(ch)

	<-ch
	c.mu.Lock()
	c.stopping = true
	c.mu.Unlock()

	timer := time.NewTimer(c.grace)
	defer timer.Stop()

	select {
	case <-ch:
		c.forceShutdown()
	case <-timer.C:
		c.forceShutdown()
	case <-c.graceDone:
	}
}

func (c *Coordinator) forceShutdown() {
	if c.forced.Swap(true) {
		return
	}
	c.force()
}

// Context is canceled on forced shutdown (second signal or grace timeout).
func (c *Coordinator) Context() context.Context {
	return c.forceCtx
}

// Stopping is true after the first SIGINT/SIGTERM (no new tenants/hours).
func (c *Coordinator) Stopping() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopping
}

// Forced is true after the second signal or grace period elapsed.
func (c *Coordinator) Forced() bool {
	return c.forced.Load()
}

// TenantContext is for the current tenant dump/upload/tar. It is not canceled on
// the first signal so mongodump can finish; it is canceled on forced shutdown.
func (c *Coordinator) TenantContext() context.Context {
	if c.Forced() {
		return c.forceCtx
	}
	return context.WithoutCancel(c.forceCtx)
}

// NotifyPersisted ends the grace-period waiter after checkpoint meta is saved.
func (c *Coordinator) NotifyPersisted() {
	c.once.Do(func() { close(c.graceDone) })
}

// Stop is the cancel function for the force context (tests / defer).
func (c *Coordinator) Stop() {
	c.force()
}
