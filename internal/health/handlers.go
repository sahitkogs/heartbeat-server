// Package health exposes a minimal liveness endpoint.
package health

import (
	"encoding/json"
	"net/http"

	"github.com/sahitkogs/heartbeat-server/internal/offline"
)

// Handler returns 200 with version info and the offline queue total when a
// queue is provided. Pass nil for q to skip the queue field (tests / local
// runs without persistence).
func Handler(version string, q *offline.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body := map[string]any{
			"ok":      true,
			"version": version,
		}
		if q != nil {
			if n, err := q.TotalDepth(r.Context()); err == nil {
				body["offline_queue_total"] = n
			}
		}
		_ = json.NewEncoder(w).Encode(body)
	}
}
