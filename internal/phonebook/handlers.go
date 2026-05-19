package phonebook

import (
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/sahitkogs/heartbeat-server/internal/auth"
)

// RegisterRequest is the POST /v1/phonebook/register body.
type RegisterRequest struct {
	FCMToken string `json:"fcm_token"`
	Platform string `json:"platform"`
}

// Handlers wraps a Store with HTTP handler methods.
type Handlers struct {
	Store *Store
}

// NewHandlers constructs a Handlers.
func NewHandlers(s *Store) *Handlers {
	return &Handlers{Store: s}
}

// Register handles POST /v1/phonebook/register. The caller's pubkey is
// taken from the auth context (set by RequireSignature middleware).
func (h *Handlers) Register(w http.ResponseWriter, r *http.Request) {
	pub := auth.ClientPubkeyFromContext(r.Context())
	if pub == nil {
		http.Error(w, "no caller pubkey", http.StatusUnauthorized)
		return
	}
	var body RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	if body.FCMToken == "" || body.Platform == "" {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	if err := h.Store.Upsert(r.Context(), hex.EncodeToString(pub), body.FCMToken, body.Platform); err != nil {
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// Delete handles DELETE /v1/phonebook/entry.
func (h *Handlers) Delete(w http.ResponseWriter, r *http.Request) {
	pub := auth.ClientPubkeyFromContext(r.Context())
	if pub == nil {
		http.Error(w, "no caller pubkey", http.StatusUnauthorized)
		return
	}
	if err := h.Store.Delete(r.Context(), hex.EncodeToString(pub)); err != nil {
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
