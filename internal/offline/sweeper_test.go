package offline

import (
	"context"
	"testing"
	"time"
)

func TestRunSweeperRunsOnTickAndExitsOnCtx(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_ = s.Enqueue(ctx, "alice", "bob", []byte("old"))
	tenDaysAgo := time.Now().Add(-10 * 24 * time.Hour).UnixMilli()
	if _, err := s.db.Exec(
		`UPDATE offline_queue SET enqueued_at = ? WHERE envelope = ?`,
		tenDaysAgo, []byte("old"),
	); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunSweeper(runCtx, s, 20*time.Millisecond, 7*24*time.Hour)
		close(done)
	}()

	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		if n, _ := s.Depth(ctx, "alice"); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("sweeper did not delete old row within deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RunSweeper did not exit on ctx cancel")
	}
}
