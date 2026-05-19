// Command hb-smoketest is a CLI client used to validate a running
// heartbeat-server. It speaks the same Ed25519-signed protocol the
// Flutter client will speak.
//
// Subcommands:
//
//	register     -- generate a fresh keypair, register a fake FCM token
//	wake         -- send a /v1/wake to a recipient pubkey (dry-run)
//	listen       -- open the /v1/signal WebSocket, print incoming frames
//	send         -- open the /v1/signal WebSocket, send an envelope, exit
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
	default:
		usage()
	}
}

func usage() {
	fmt.Println("usage: hb-smoketest <register|wake|listen|send> [flags]")
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
