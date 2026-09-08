package zipserver

import (
	"context"
	"sync"
)

// jobCounter is a WaitGroup whose Wait takes a context. sync.WaitGroup
// forbids an Add from zero concurrent with a Wait, and a Wait abandoned at a
// deadline would leave exactly that waiter behind for the next Add.
type jobCounter struct {
	mu   sync.Mutex
	n    int
	idle chan struct{}
}

func (c *jobCounter) Add(delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == 0 && delta > 0 {
		c.idle = make(chan struct{})
	}
	c.n += delta
	if c.n < 0 {
		panic("jobCounter: negative count")
	}
	if c.n == 0 && c.idle != nil {
		close(c.idle)
		c.idle = nil
	}
}

func (c *jobCounter) Done() {
	c.Add(-1)
}

// Wait blocks until the count reaches zero or ctx expires.
func (c *jobCounter) Wait(ctx context.Context) error {
	c.mu.Lock()
	idle := c.idle
	c.mu.Unlock()
	if idle == nil {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// activeJobs tracks work that outlives the request that started it, so a
// shutdown can wait for it before the process exits.
var activeJobs jobCounter

func startJob(fn func()) {
	activeJobs.Add(1)
	go func() {
		defer activeJobs.Done()
		fn()
	}()
}
