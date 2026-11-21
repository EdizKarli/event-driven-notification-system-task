package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

func validSMS() NewNotification {
	return NewNotification{
		Recipient: "+905551234567",
		Channel:   ChannelSMS,
		Content:   "Your order has shipped",
		Priority:  PriorityNormal,
	}
}

func hasFieldError(err error, field string) bool {
	var ve *ValidationError
	if !errors.As(err, &ve) {
		return false
	}
	for _, fe := range ve.Errors {
		if fe.Field == field {
			return true
		}
	}
	return false
}

func TestValidate(t *testing.T) {
	past := testNow.Add(-time.Hour)
	future := testNow.Add(time.Hour)

	tests := []struct {
		name      string
		modify    func(n *NewNotification)
		wantField string
	}{
		{"valid sms", func(n *NewNotification) {}, ""},
		{"valid email", func(n *NewNotification) {
			n.Channel = ChannelEmail
			n.Recipient = "user@example.com"
		}, ""},
		{"valid push", func(n *NewNotification) {
			n.Channel = ChannelPush
			n.Recipient = "device-token-123"
		}, ""},
		{"valid future schedule", func(n *NewNotification) { n.ScheduledAt = &future }, ""},
		{"sms at max length", func(n *NewNotification) { n.Content = strings.Repeat("a", MaxSMSLength) }, ""},
		{"missing recipient", func(n *NewNotification) { n.Recipient = "" }, "recipient"},
		{"missing content", func(n *NewNotification) { n.Content = "" }, "content"},
		{"whitespace content", func(n *NewNotification) { n.Content = "   " }, "content"},
		{"missing channel", func(n *NewNotification) { n.Channel = "" }, "channel"},
		{"unknown channel", func(n *NewNotification) { n.Channel = "fax" }, "channel"},
		{"unknown priority", func(n *NewNotification) { n.Priority = "urgent" }, "priority"},
		{"past schedule", func(n *NewNotification) { n.ScheduledAt = &past }, "scheduled_at"},
		{"sms without plus", func(n *NewNotification) { n.Recipient = "05551234567" }, "recipient"},
		{"sms with letters", func(n *NewNotification) { n.Recipient = "+90555abc4567" }, "recipient"},
		{"sms too long", func(n *NewNotification) { n.Content = strings.Repeat("a", MaxSMSLength+1) }, "content"},
		{"invalid email", func(n *NewNotification) {
			n.Channel = ChannelEmail
			n.Recipient = "not-an-email"
		}, "recipient"},
		{"email with display name", func(n *NewNotification) {
			n.Channel = ChannelEmail
			n.Recipient = "John <john@example.com>"
		}, "recipient"},
		{"email too long", func(n *NewNotification) {
			n.Channel = ChannelEmail
			n.Recipient = "user@example.com"
			n.Content = strings.Repeat("a", MaxEmailLength+1)
		}, "content"},
		{"push token with space", func(n *NewNotification) {
			n.Channel = ChannelPush
			n.Recipient = "device token"
		}, "recipient"},
		{"push payload too large", func(n *NewNotification) {
			n.Channel = ChannelPush
			n.Recipient = "device-token-123"
			n.Content = strings.Repeat("a", MaxPushBytes+1)
		}, "content"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := validSMS()
			tt.modify(&n)
			err := n.Validate(testNow)

			if tt.wantField == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if !hasFieldError(err, tt.wantField) {
				t.Fatalf("expected error on field %q, got %v", tt.wantField, err)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	n := NewNotification{
		Recipient: "  +905551234567 ",
		Channel:   " SMS ",
		Content:   "hi",
	}
	n.Normalize()

	if n.Recipient != "+905551234567" {
		t.Errorf("recipient not trimmed: %q", n.Recipient)
	}
	if n.Channel != ChannelSMS {
		t.Errorf("channel not normalized: %q", n.Channel)
	}
	if n.Priority != PriorityNormal {
		t.Errorf("expected default priority normal, got %q", n.Priority)
	}
}

func TestValidateBatch(t *testing.T) {
	t.Run("valid batch", func(t *testing.T) {
		items := []NewNotification{validSMS(), validSMS()}
		if err := ValidateBatch(items, testNow); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		if !hasFieldError(ValidateBatch(nil, testNow), "notifications") {
			t.Fatal("expected error for empty batch")
		}
	})

	t.Run("batch too large", func(t *testing.T) {
		items := make([]NewNotification, MaxBatchSize+1)
		for i := range items {
			items[i] = validSMS()
		}
		if !hasFieldError(ValidateBatch(items, testNow), "notifications") {
			t.Fatal("expected error for oversized batch")
		}
	})

	t.Run("error points to item index", func(t *testing.T) {
		bad := validSMS()
		bad.Recipient = ""
		items := []NewNotification{validSMS(), bad}
		if !hasFieldError(ValidateBatch(items, testNow), "notifications[1].recipient") {
			t.Fatal("expected error on notifications[1].recipient")
		}
	})
}

func TestToNotification(t *testing.T) {
	t.Run("sends now when not scheduled", func(t *testing.T) {
		n := validSMS().ToNotification("id-1", testNow)
		if n.Status != StatusPending {
			t.Errorf("expected pending, got %q", n.Status)
		}
		if !n.NextAttemptAt.Equal(testNow) {
			t.Errorf("expected next attempt at now, got %v", n.NextAttemptAt)
		}
		if n.MaxAttempts != DefaultMaxAttempts {
			t.Errorf("expected %d max attempts, got %d", DefaultMaxAttempts, n.MaxAttempts)
		}
	})

	t.Run("uses scheduled time", func(t *testing.T) {
		later := testNow.Add(2 * time.Hour)
		req := validSMS()
		req.ScheduledAt = &later
		n := req.ToNotification("id-2", testNow)
		if !n.NextAttemptAt.Equal(later) {
			t.Errorf("expected next attempt at %v, got %v", later, n.NextAttemptAt)
		}
	})
}

func TestMatches(t *testing.T) {
	req := validSMS()
	existing := req.ToNotification("id-1", testNow)

	if !req.Matches(existing) {
		t.Fatal("expected identical request to match")
	}

	req.Content = "something else"
	if req.Matches(existing) {
		t.Fatal("expected different content not to match")
	}
}
