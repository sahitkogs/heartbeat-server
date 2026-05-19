package signaling

import (
	"encoding/base64"
	"encoding/json"
)

// ClientFrame is one inbound frame from a connected client.
type ClientFrame struct {
	Type     string `json:"type"`
	Pubkey   string `json:"pubkey,omitempty"`   // for is_online queries
	To       string `json:"to,omitempty"`       // for send
	Envelope string `json:"envelope,omitempty"` // base64
}

// ParseClientFrame deserializes a JSON frame from the client.
func ParseClientFrame(b []byte) (*ClientFrame, error) {
	var f ClientFrame
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// BuildDeliverFrame constructs a "deliver" frame the server pushes to a recipient.
func BuildDeliverFrame(fromPubkey string, envelope []byte) []byte {
	out := map[string]any{
		"type":     "deliver",
		"from":     fromPubkey,
		"envelope": base64.StdEncoding.EncodeToString(envelope),
	}
	b, _ := json.Marshal(out)
	return b
}

// BuildErrorFrame constructs an error frame.
func BuildErrorFrame(code, message string) []byte {
	out := map[string]string{
		"type":    "error",
		"code":    code,
		"message": message,
	}
	b, _ := json.Marshal(out)
	return b
}

// BuildOnlineStatusFrame constructs an online_status reply.
func BuildOnlineStatusFrame(pubkey string, online bool) []byte {
	out := map[string]any{
		"type":   "online_status",
		"pubkey": pubkey,
		"online": online,
	}
	b, _ := json.Marshal(out)
	return b
}

// decodeBase64 is a small helper that decodes envelope payloads. Errors are
// returned to the caller; downstream handlers may choose to forward an
// error frame to the client.
func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}
