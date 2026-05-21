package signaling

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func TestParseClientFrameSend(t *testing.T) {
	body := []byte(`{"type":"send","to":"abc","envelope":"aGVsbG8="}`)
	f, err := ParseClientFrame(body)
	if err != nil {
		t.Fatalf("ParseClientFrame: %v", err)
	}
	if f.Type != "send" || f.To != "abc" {
		t.Fatalf("unexpected frame: %+v", f)
	}
	env, _ := base64.StdEncoding.DecodeString(f.Envelope)
	if string(env) != "hello" {
		t.Fatalf("env decoded to %q", env)
	}
}

func TestBuildDeliverFrame(t *testing.T) {
	got := BuildDeliverFrame("alice-pub", []byte("hi"))
	var out map[string]any
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out["type"] != "deliver" || out["from"] != "alice-pub" {
		t.Fatalf("unexpected frame: %v", out)
	}
}

func TestBuildErrorFrameForPeer(t *testing.T) {
	got := BuildErrorFrameForPeer("recipient_offline", "bob-pub")
	var out map[string]string
	if err := json.Unmarshal(got, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out["type"] != "error" || out["code"] != "recipient_offline" {
		t.Fatalf("unexpected frame: %v", out)
	}
	if out["to"] != "bob-pub" {
		t.Fatalf("missing or wrong `to`: %v", out)
	}
	if out["message"] != "bob-pub" {
		t.Fatalf("expected `message` to mirror peer pubkey for backwards-compat, got %q", out["message"])
	}
}
