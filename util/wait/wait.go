package wait

import (
	"sync"
	"time"
)

type Wait struct {
	wg sync.WaitGroup
}

func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) Wait() {
	w.wg.Wait()
}

func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	done := make(chan struct{})

	go func() {
		w.Wait()
		close(done)
	}()

	select {
	case <-done:
		return false

	case <-time.After(timeout):
		return true
	}
}
