package offline

import (
	"context"
	"log"
	"time"
)

// RunSweeper periodically calls Sweep until ctx is cancelled. Logs each
// sweep that deleted any rows. Intended to be launched as `go RunSweeper(...)`
// from main.
func RunSweeper(ctx context.Context, s *Store, interval, maxAge time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := s.Sweep(ctx, maxAge)
			if err != nil {
				log.Printf("[offline] sweep_err: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("[offline] sweep_deleted=%d maxAge=%s", n, maxAge)
			}
		}
	}
}
