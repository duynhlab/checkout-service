package workflow

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
)

// ErrTemporalUnavailable reports that no Temporal connection exists yet. The
// Notifier treats it like any other signal failure: logged and swallowed —
// the lazy expires_at backstop covers the session.
var ErrTemporalUnavailable = errors.New("temporal client not connected")

// Signaler is the slice of client.Client the Notifier needs. Lazy implements
// it; tests pass fakes.
type Signaler interface {
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any,
		options client.StartWorkflowOptions, workflow any, workflowArgs ...any) (client.WorkflowRun, error)
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error
}

// Lazy keeps dialing Temporal in the background until it succeeds (ported
// from order-service's fulfillment.Lazy, which fixed the same bug). The old
// serve-path flow gave up after the startup retry budget and ran with a nil
// notifier forever — a checkout pod that raced Temporal at bring-up never
// started AbandonedCheckoutWorkflow for any session until a human restarted
// it (BUGS-6). Lazy makes that startup race self-healing; a connection that
// breaks later is still the SDK's reconnect job, as before.
type Lazy struct {
	logger *zap.Logger

	mu     sync.RWMutex
	client client.Client

	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// NewLazy returns a Lazy that dials in the background, waiting interval
// between failed attempts, until one dial succeeds. dial is injected for
// testability (production passes a temporalx.Dial closure).
func NewLazy(dial func() (client.Client, error), interval time.Duration, logger *zap.Logger) *Lazy {
	l := &Lazy{logger: logger, stop: make(chan struct{}), done: make(chan struct{})}
	go l.redial(dial, interval)
	return l
}

// NewLazySeeded returns an already-connected Lazy (the startup dial
// succeeded); no background loop runs.
func NewLazySeeded(c client.Client, logger *zap.Logger) *Lazy {
	l := &Lazy{logger: logger, client: c, stop: make(chan struct{}), done: make(chan struct{})}
	close(l.done)
	return l
}

func (l *Lazy) redial(dial func() (client.Client, error), interval time.Duration) {
	defer close(l.done)
	for attempt := 1; ; attempt++ {
		// Run the dial in its own goroutine so Close never waits on an
		// unresponsive endpoint (the dial itself takes no context). An
		// abandoned dial that succeeds late closes its own client.
		type result struct {
			c   client.Client
			err error
		}
		ch := make(chan result, 1)
		go func() {
			c, err := dial()
			ch <- result{c, err}
		}()

		select {
		case <-l.stop:
			go func() {
				if r := <-ch; r.err == nil && r.c != nil {
					r.c.Close()
				}
			}()
			return
		case r := <-ch:
			if r.err == nil {
				l.mu.Lock()
				l.client = r.c
				l.mu.Unlock()
				l.logger.Info("Temporal connected by background redial; abandonment notifier active",
					zap.Int("attempt", attempt))
				return
			}
			// The first attempts are worth a warning; after that one line
			// per interval forever is just pager noise.
			if attempt <= 3 || attempt%20 == 0 {
				l.logger.Warn("Temporal background redial failed", zap.Int("attempt", attempt), zap.Error(r.err))
			}
		}

		// Pace AFTER the failed attempt so interval is a floor between
		// dials, not a ticker that queues up during a slow one.
		select {
		case <-l.stop:
			return
		case <-time.After(interval):
		}
	}
}

// TemporalReady reports whether a connection exists. Safe on a nil receiver.
func (l *Lazy) TemporalReady() bool {
	if l == nil {
		return false
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.client != nil
}

func (l *Lazy) current() client.Client {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.client
}

// SignalWithStartWorkflow delegates to the connected client, or returns
// ErrTemporalUnavailable while the background dial is still trying.
func (l *Lazy) SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any,
	options client.StartWorkflowOptions, workflow any, workflowArgs ...any) (client.WorkflowRun, error) {
	c := l.current()
	if c == nil {
		return nil, ErrTemporalUnavailable
	}
	return c.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, workflow, workflowArgs...)
}

// SignalWorkflow delegates to the connected client, or returns
// ErrTemporalUnavailable while the background dial is still trying.
func (l *Lazy) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error {
	c := l.current()
	if c == nil {
		return ErrTemporalUnavailable
	}
	return c.SignalWorkflow(ctx, workflowID, runID, signalName, arg)
}

// Close stops the background loop (without waiting on an in-flight dial) and
// closes the connection if one was established. Safe to call more than once
// and on a nil receiver.
func (l *Lazy) Close() {
	if l == nil {
		return
	}
	l.stopOnce.Do(func() { close(l.stop) })
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.client != nil {
		l.client.Close()
		l.client = nil
	}
}
