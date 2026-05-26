// Command hb-smoketest is a CLI client used to validate a running
// heartbeat-server. It speaks the same Ed25519-signed protocol the
// Flutter client will speak.
//
// Subcommands:
//
//	register       -- generate a fresh keypair, register a fake FCM token
//	wake           -- send a /v1/wake to a recipient pubkey (dry-run)
//	listen         -- open the /v1/signal WebSocket, print incoming frames
//	send           -- open the /v1/signal WebSocket, send an envelope, exit
//	offline-queue  -- end-to-end test of the server-side offline queue
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"nhooyr.io/websocket"

	"github.com/sahitkogs/heartbeat-server/internal/keys"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "register":
		cmdRegister(os.Args[2:])
	case "wake":
		cmdWake(os.Args[2:])
	case "listen":
		cmdListen(os.Args[2:])
	case "send":
		cmdSend(os.Args[2:])
	case "offline-queue":
		cmdOfflineQueue(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Println("usage: hb-smoketest <register|wake|listen|send|offline-queue> [flags]")
	os.Exit(2)
}

func signAndDo(method, url string, body []byte, kp *keys.Keypair) (*http.Response, error) {
	ts := time.Now().UTC().Format(time.RFC3339)
	sig := ed25519.Sign(kp.Private, append([]byte(ts+"\n"), body...))
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Heartbeat-Pubkey", kp.PublicHex())
	req.Header.Set("X-Heartbeat-Sig", hex.EncodeToString(sig))
	req.Header.Set("X-Heartbeat-Timestamp", ts)
	return http.DefaultClient.Do(req)
}

func cmdRegister(args []string) {
	fs := flag.NewFlagSet("register", flag.ExitOnError)
	server := fs.String("server", "http://localhost:8080", "server base URL")
	token := fs.String("token", "fake-fcm-token", "FCM token to register")
	keyFile := fs.String("key", "smoketest.key", "path to read/write the keypair (hex private)")
	_ = fs.Parse(args)

	kp := loadOrCreateKey(*keyFile)
	body, _ := json.Marshal(map[string]string{"fcm_token": *token, "platform": "android"})
	resp, err := signAndDo(http.MethodPost, *server+"/v1/phonebook/register", body, kp)
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("status=%d body=%s\npubkey=%s\n", resp.StatusCode, string(b), kp.PublicHex())
}

func cmdWake(args []string) {
	fs := flag.NewFlagSet("wake", flag.ExitOnError)
	server := fs.String("server", "http://localhost:8080", "server base URL")
	keyFile := fs.String("key", "smoketest.key", "sender keypair file")
	recipient := fs.String("to", "", "recipient pubkey hex")
	payload := fs.String("payload", "aGVsbG8=", "base64 opaque payload")
	dry := fs.Bool("dry", true, "FCM dry-run mode")
	_ = fs.Parse(args)
	if *recipient == "" {
		log.Fatal("--to is required")
	}

	kp := loadOrCreateKey(*keyFile)
	body, _ := json.Marshal(map[string]any{
		"recipient_pubkey": *recipient,
		"opaque_payload":   *payload,
		"dry_run":          *dry,
	})
	resp, err := signAndDo(http.MethodPost, *server+"/v1/wake", body, kp)
	if err != nil {
		log.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Printf("status=%d body=%s\n", resp.StatusCode, string(b))
}

func cmdListen(args []string) {
	fs := flag.NewFlagSet("listen", flag.ExitOnError)
	server := fs.String("server", "ws://localhost:8080", "WS base URL")
	keyFile := fs.String("key", "smoketest.key", "keypair file")
	_ = fs.Parse(args)
	kp := loadOrCreateKey(*keyFile)
	conn := wsConnect(*server+"/v1/signal", kp)
	defer conn.Close(websocket.StatusNormalClosure, "")
	fmt.Printf("listening as %s\n", kp.PublicHex())
	ctx := context.Background()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			log.Fatalf("read: %v", err)
		}
		fmt.Println(string(data))
	}
}

func cmdSend(args []string) {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	server := fs.String("server", "ws://localhost:8080", "WS base URL")
	keyFile := fs.String("key", "smoketest.key", "keypair file")
	to := fs.String("to", "", "recipient pubkey hex")
	text := fs.String("text", "hello from smoketest", "text payload (will be base64-encoded as the envelope)")
	_ = fs.Parse(args)
	if *to == "" {
		log.Fatal("--to is required")
	}
	kp := loadOrCreateKey(*keyFile)
	conn := wsConnect(*server+"/v1/signal", kp)
	defer conn.Close(websocket.StatusNormalClosure, "")

	frame := map[string]string{
		"type":     "send",
		"to":       *to,
		"envelope": base64.StdEncoding.EncodeToString([]byte(*text)),
	}
	b, _ := json.Marshal(frame)
	if err := conn.Write(context.Background(), websocket.MessageText, b); err != nil {
		log.Fatalf("write: %v", err)
	}
	// Wait briefly to receive any error from the server
	conn.SetReadLimit(1024)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, reply, err := conn.Read(ctx)
	if err == nil {
		fmt.Println("server replied:", string(reply))
	}
}

func wsConnect(url string, kp *keys.Keypair) *websocket.Conn {
	ts := time.Now().UTC().Format(time.RFC3339)
	sig := ed25519.Sign(kp.Private, []byte("WS-CONNECT:"+ts))
	h := http.Header{}
	h.Set("X-Heartbeat-Pubkey", kp.PublicHex())
	h.Set("X-Heartbeat-Sig", hex.EncodeToString(sig))
	h.Set("X-Heartbeat-Timestamp", ts)
	conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	return conn
}

// cmdOfflineQueue exercises the server-side offline queue end-to-end:
//
//  1. A connects then immediately disconnects (proves A is reachable + leaves
//     the hub).
//  2. B connects, sends to A (recipient now offline), reads the
//     recipient_offline error, disconnects.
//  3. A reconnects — flushOffline should push the queued envelope within a
//     few seconds.
//  4. A reconnects once more — the queue must be drained (no re-delivery).
//
// The server is run with -fcm-disabled so wakeOfflineRecipient is a no-op;
// neither A nor B needs a phonebook entry.
func cmdOfflineQueue(args []string) {
	fs := flag.NewFlagSet("offline-queue", flag.ExitOnError)
	relay := fs.String("relay", "ws://localhost:8080/v1/signal", "WS relay URL")
	_ = fs.Parse(args)

	ctx := context.Background()
	kpA, err := keys.Generate()
	if err != nil {
		log.Fatalf("[offline-queue] keygen A: %v", err)
	}
	kpB, err := keys.Generate()
	if err != nil {
		log.Fatalf("[offline-queue] keygen B: %v", err)
	}
	payload := []byte("hello-while-offline")

	// Step 1: A connects briefly to prove reachability, then disconnects.
	connA1 := wsConnect(*relay, kpA)
	_ = connA1.Close(websocket.StatusNormalClosure, "")
	log.Printf("[offline-queue] A connected+disconnected")
	time.Sleep(200 * time.Millisecond)

	// Step 2: B connects, sends to A, reads recipient_offline, disconnects.
	connB := wsConnect(*relay, kpB)
	sendFrame, _ := json.Marshal(map[string]string{
		"type":     "send",
		"to":       kpA.PublicHex(),
		"envelope": base64.StdEncoding.EncodeToString(payload),
	})
	if err := connB.Write(ctx, websocket.MessageText, sendFrame); err != nil {
		log.Fatalf("[offline-queue] B send write: %v", err)
	}
	rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
	if _, reply, rerr := connB.Read(rctx); rerr == nil {
		log.Printf("[offline-queue] B got reply: %s", string(reply))
	}
	rcancel()
	_ = connB.Close(websocket.StatusNormalClosure, "")
	time.Sleep(200 * time.Millisecond)

	// Step 3: A reconnects, expects the queued envelope within 3s.
	connA2 := wsConnect(*relay, kpA)
	rctx, rcancel = context.WithTimeout(ctx, 3*time.Second)
	_, msg, err := connA2.Read(rctx)
	rcancel()
	if err != nil {
		log.Fatalf("[offline-queue] FAIL A did not receive queued envelope: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(msg, &got); err != nil {
		log.Fatalf("[offline-queue] FAIL bad deliver JSON: %v body=%s", err, string(msg))
	}
	if got["type"] != "deliver" {
		log.Fatalf("[offline-queue] FAIL unexpected frame type=%v body=%s", got["type"], string(msg))
	}
	if got["from"] != kpB.PublicHex() {
		log.Fatalf("[offline-queue] FAIL wrong from=%v want=%s", got["from"], kpB.PublicHex())
	}
	envB64, _ := got["envelope"].(string)
	envBytes, err := base64.StdEncoding.DecodeString(envB64)
	if err != nil {
		log.Fatalf("[offline-queue] FAIL bad envelope b64: %v", err)
	}
	if string(envBytes) != string(payload) {
		log.Fatalf("[offline-queue] FAIL envelope mismatch got=%q want=%q", string(envBytes), string(payload))
	}
	log.Printf("[offline-queue] A received queued envelope OK")
	_ = connA2.Close(websocket.StatusNormalClosure, "")
	time.Sleep(300 * time.Millisecond)

	// Step 4: A reconnects again — queue must be drained (no re-delivery).
	connA3 := wsConnect(*relay, kpA)
	rctx, rcancel = context.WithTimeout(ctx, 1*time.Second)
	_, msg2, err := connA3.Read(rctx)
	rcancel()
	if err == nil {
		log.Fatalf("[offline-queue] FAIL queue not drained, got re-delivery: %s", string(msg2))
	}
	_ = connA3.Close(websocket.StatusNormalClosure, "")

	fmt.Println("PASS offline-queue")
}

func loadOrCreateKey(path string) *keys.Keypair {
	if b, err := os.ReadFile(path); err == nil {
		priv, err := hex.DecodeString(string(bytes.TrimSpace(b)))
		if err == nil && len(priv) == ed25519.PrivateKeySize {
			pk := ed25519.PrivateKey(priv)
			return &keys.Keypair{Public: pk.Public().(ed25519.PublicKey), Private: pk}
		}
	}
	kp, err := keys.Generate()
	if err != nil {
		log.Fatalf("generate: %v", err)
	}
	if err := os.WriteFile(path, []byte(hex.EncodeToString(kp.Private)), 0600); err != nil {
		log.Fatalf("write key: %v", err)
	}
	return kp
}
