// Package wake bridges to Firebase Cloud Messaging to wake offline phones.
package wake

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

// FCMClient is the minimal interface the Sender needs from FCM.
// Real and fake implementations both satisfy this.
type FCMClient interface {
	Send(ctx context.Context, token string, payload []byte, dryRun bool) error
}

// Sender wraps an FCMClient. Other packages depend on Sender, not on a
// concrete Firebase implementation, which keeps tests dependency-free.
type Sender struct {
	FCM FCMClient
}

// Wake delivers a payload to the device identified by FCM token.
func (s Sender) Wake(ctx context.Context, token string, payload []byte, dryRun bool) error {
	if s.FCM == nil {
		return errors.New("wake: no FCM client configured")
	}
	return s.FCM.Send(ctx, token, payload, dryRun)
}

// FirebaseFCM is the production FCMClient backed by Firebase Admin SDK.
type FirebaseFCM struct {
	client *messaging.Client
}

// NewFirebaseFCM constructs a real FCM client from a service-account JSON file.
// Pass an empty path to skip Firebase init (useful in tests).
func NewFirebaseFCM(ctx context.Context, serviceAccountPath string) (*FirebaseFCM, error) {
	if serviceAccountPath == "" {
		return nil, errors.New("service account path required")
	}
	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsFile(serviceAccountPath))
	if err != nil {
		return nil, fmt.Errorf("firebase init: %w", err)
	}
	c, err := app.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("messaging client: %w", err)
	}
	return &FirebaseFCM{client: c}, nil
}

// Send delivers a data-message via FCM. payload is base64-encoded into a
// single data field; clients decode it back to bytes.
func (f *FirebaseFCM) Send(ctx context.Context, token string, payload []byte, dryRun bool) error {
	msg := &messaging.Message{
		Token: token,
		Data: map[string]string{
			"hb_payload": base64.StdEncoding.EncodeToString(payload),
		},
		Android: &messaging.AndroidConfig{
			Priority: "high",
		},
	}
	if dryRun {
		_, err := f.client.SendDryRun(ctx, msg)
		return err
	}
	_, err := f.client.Send(ctx, msg)
	return err
}
