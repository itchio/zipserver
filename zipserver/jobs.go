package zipserver

import (
	"context"
	"sync"
)

// activeJobs tracks work that outlives the request that started it, so a
// shutdown can wait for it before the process exits.
var activeJobs sync.WaitGroup

func startJob(fn func()) {
	activeJobs.Add(1)
	go func() {
		defer activeJobs.Done()
		fn()
	}()
}

func waitGroupWithContext(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
