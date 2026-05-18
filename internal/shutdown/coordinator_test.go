package shutdown

import (
	"testing"
	"time"
)

func TestCoordinator_TenantContext_notCanceledOnStopping(t *testing.T) {
	c := New(time.Minute)
	c.mu.Lock()
	c.stopping = true
	c.mu.Unlock()

	ctx := c.TenantContext()
	if err := ctx.Err(); err != nil {
		t.Fatalf("tenant ctx should not be canceled yet: %v", err)
	}
}

func TestCoordinator_TenantContext_canceledWhenForced(t *testing.T) {
	c := New(time.Minute)
	c.forceShutdown()

	if err := c.TenantContext().Err(); err == nil {
		t.Fatal("expected forced tenant ctx canceled")
	}
}

func TestCoordinator_NotifyPersisted_closesGraceDone(t *testing.T) {
	c := New(time.Minute)
	c.NotifyPersisted()
	select {
	case <-c.graceDone:
	default:
		t.Fatal("graceDone should be closed")
	}
}
