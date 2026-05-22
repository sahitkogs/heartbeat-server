package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sahitkogs/heartbeat-server/internal/auth"
	"github.com/sahitkogs/heartbeat-server/internal/health"
	"github.com/sahitkogs/heartbeat-server/internal/phonebook"
	"github.com/sahitkogs/heartbeat-server/internal/signaling"
	"github.com/sahitkogs/heartbeat-server/internal/wake"
)

const version = "0.1.4-phase10.4.1-bug6"

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "/var/lib/heartbeat/phonebook.db", "phonebook SQLite path")
	fcmCreds := flag.String("fcm-creds", os.Getenv("HB_FCM_CREDENTIALS"), "Firebase service-account JSON path")
	dryFCM := flag.Bool("fcm-disabled", false, "if true, skip Firebase init and use a stub FCM client (refuses real sends)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	book, err := phonebook.Open(*dbPath)
	if err != nil {
		log.Fatalf("open phonebook: %v", err)
	}
	defer book.Close()

	var fcm wake.FCMClient
	if *dryFCM || *fcmCreds == "" {
		fcm = stubFCM{}
		log.Println("WARNING: FCM disabled, wake calls will be no-ops")
	} else {
		c, err := wake.NewFirebaseFCM(ctx, *fcmCreds)
		if err != nil {
			log.Fatalf("init FCM: %v", err)
		}
		fcm = c
	}
	sender := wake.Sender{FCM: fcm}

	hub := signaling.NewHub()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", health.Handler(version))

	pbHandlers := phonebook.NewHandlers(book)
	mux.Handle("/v1/phonebook/register", auth.RequireSignature(http.HandlerFunc(pbHandlers.Register)))
	mux.Handle("/v1/phonebook/entry", auth.RequireSignature(http.HandlerFunc(pbHandlers.Delete)))

	wkHandlers := wake.NewHandlers(book, sender)
	mux.Handle("/v1/wake", auth.RequireSignature(http.HandlerFunc(wkHandlers.Wake)))

	sigHandlers := signaling.NewHandlers(hub)
	mux.HandleFunc("/v1/signal", sigHandlers.Signal)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("heartbeat-server %s listening on %s", version, *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shCtx)
}

// stubFCM swallows wake calls when no real Firebase credentials are configured.
type stubFCM struct{}

func (stubFCM) Send(ctx context.Context, token string, payload []byte, dryRun bool) error {
	log.Printf("stubFCM: would send (token=%s, bytes=%d, dry=%v)", redact(token), len(payload), dryRun)
	return nil
}

func redact(s string) string {
	if len(s) <= 8 {
		return "***"
	}
	return s[:4] + "..." + s[len(s)-4:]
}
