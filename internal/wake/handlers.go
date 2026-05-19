package wake

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/sahitkogs/heartbeat-server/internal/phonebook"
)

// WakeRequest is the POST /v1/wake body.
type WakeRequest struct {
	RecipientPubkey string `json:"recipient_pubkey"`
	OpaquePayload   string `json:"opaque_payload"` // base64
	DryRun          bool   `json:"dry_run"`
}

// Handlers serves /v1/wake. Looks up the recipient's FCM token in the
// phonebook and dispatches via the Sender. Holds no state.
type Handlers struct {
	Book   *phonebook.Store
	Sender Sender
}

// NewHandlers constructs a Handlers.
func NewHandlers(book *phonebook.Store, s Sender) *Handlers {
	return &Handlers{Book: book, Sender: s}
}

// Wake handles POST /v1/wake.
func (h *Handlers) Wake(w http.ResponseWriter, r *http.Request) {
	var body WakeRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if body.RecipientPubkey == "" || body.OpaquePayload == "" {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	payload, err := base64.StdEncoding.DecodeString(body.OpaquePayload)
	if err != nil {
		http.Error(w, "bad payload encoding", http.StatusBadRequest)
		return
	}
	entry, err := h.Book.Lookup(r.Context(), body.RecipientPubkey)
	if err != nil {
		if errors.Is(err, phonebook.ErrNotFound) {
			http.Error(w, "recipient not registered", http.StatusNotFound)
			return
		}
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	if err := h.Sender.Wake(r.Context(), entry.FCMToken, payload, body.DryRun); err != nil {
		http.Error(w, "fcm error: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}
