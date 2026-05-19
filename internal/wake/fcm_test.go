package wake

import (
	"context"
	"errors"
	"testing"
)

type fakeFCM struct {
	calls   int
	lastTok string
	lastDry bool
	err     error
}

func (f *fakeFCM) Send(ctx context.Context, token string, payload []byte, dryRun bool) error {
	f.calls++
	f.lastTok = token
	f.lastDry = dryRun
	return f.err
}

func TestSenderDelegatesToFCM(t *testing.T) {
	fk := &fakeFCM{}
	s := Sender{FCM: fk}
	if err := s.Wake(context.Background(), "tok", []byte("hi"), false); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if fk.calls != 1 || fk.lastTok != "tok" || fk.lastDry {
		t.Fatalf("unexpected fake state: %+v", fk)
	}
}

func TestSenderPropagatesError(t *testing.T) {
	fk := &fakeFCM{err: errors.New("boom")}
	s := Sender{FCM: fk}
	if err := s.Wake(context.Background(), "tok", []byte("hi"), false); err == nil {
		t.Fatal("expected error")
	}
}

func TestSenderDryRunFlag(t *testing.T) {
	fk := &fakeFCM{}
	s := Sender{FCM: fk}
	_ = s.Wake(context.Background(), "tok", []byte("hi"), true)
	if !fk.lastDry {
		t.Fatal("expected dry-run flag forwarded")
	}
}
