// Package health exposes a minimal liveness endpoint.
package health

import (
	"encoding/json"
	"net/http"
)

// Handler returns 200 with version info. Useful for uptime monitoring.
func Handler(version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"version": version,
		})
	}
}
