package domain

import (
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxBatchSize   = 1000
	MaxSMSLength   = 1600
	MaxEmailLength = 10000
	MaxPushBytes   = 4096
)

var e164Pattern = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

type NewNotification struct {
	Recipient   string     `json:"recipient"`
	Channel     Channel    `json:"channel"`
	Content     string     `json:"content"`
	Priority    Priority   `json:"priority"`
	ScheduledAt *time.Time `json:"scheduled_at,omitempty"`
}

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type ValidationError struct {
	Errors []FieldError `json:"errors"`
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Errors))
	for _, fe := range e.Errors {
		parts = append(parts, fe.Field+": "+fe.Message)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func (e *ValidationError) add(field, message string) {
	e.Errors = append(e.Errors, FieldError{Field: field, Message: message})
}

func (n *NewNotification) Normalize() {
	n.Recipient = strings.TrimSpace(n.Recipient)
	n.Channel = Channel(strings.ToLower(strings.TrimSpace(string(n.Channel))))
	n.Priority = Priority(strings.ToLower(strings.TrimSpace(string(n.Priority))))
	if n.Priority == "" {
		n.Priority = PriorityNormal
	}
}

func (n NewNotification) Validate(now time.Time) error {
	ve := &ValidationError{}

	if n.Recipient == "" {
		ve.add("recipient", "is required")
	}
	if strings.TrimSpace(n.Content) == "" {
		ve.add("content", "is required")
	}
	if !n.Priority.Valid() {
		ve.add("priority", "must be one of high, normal, low")
	}
	if n.ScheduledAt != nil && n.ScheduledAt.Before(now) {
		ve.add("scheduled_at", "must be in the future")
	}

	switch n.Channel {
	case ChannelSMS:
		validateSMS(n, ve)
	case ChannelEmail:
		validateEmail(n, ve)
	case ChannelPush:
		validatePush(n, ve)
	case "":
		ve.add("channel", "is required")
	default:
		ve.add("channel", "must be one of sms, email, push")
	}

	if len(ve.Errors) > 0 {
		return ve
	}
	return nil
}

func validateSMS(n NewNotification, ve *ValidationError) {
	if n.Recipient != "" && !e164Pattern.MatchString(n.Recipient) {
		ve.add("recipient", "must be a phone number in E.164 format, e.g. +905551234567")
	}
	if utf8.RuneCountInString(n.Content) > MaxSMSLength {
		ve.add("content", fmt.Sprintf("must be at most %d characters for sms", MaxSMSLength))
	}
}

func validateEmail(n NewNotification, ve *ValidationError) {
	if n.Recipient != "" {
		addr, err := mail.ParseAddress(n.Recipient)
		if err != nil || addr.Address != n.Recipient {
			ve.add("recipient", "must be a valid email address")
		}
	}
	if utf8.RuneCountInString(n.Content) > MaxEmailLength {
		ve.add("content", fmt.Sprintf("must be at most %d characters for email", MaxEmailLength))
	}
}

func validatePush(n NewNotification, ve *ValidationError) {
	if strings.ContainsAny(n.Recipient, " \t\n") {
		ve.add("recipient", "must be a device token without whitespace")
	}
	if len(n.Content) > MaxPushBytes {
		ve.add("content", fmt.Sprintf("must be at most %d bytes for push", MaxPushBytes))
	}
}

func ValidateBatch(items []NewNotification, now time.Time) error {
	ve := &ValidationError{}

	if len(items) == 0 {
		ve.add("notifications", "must contain at least one notification")
		return ve
	}
	if len(items) > MaxBatchSize {
		ve.add("notifications", fmt.Sprintf("must contain at most %d notifications", MaxBatchSize))
		return ve
	}

	for i, item := range items {
		var itemErr *ValidationError
		if errors.As(item.Validate(now), &itemErr) {
			for _, fe := range itemErr.Errors {
				ve.add(fmt.Sprintf("notifications[%d].%s", i, fe.Field), fe.Message)
			}
		}
	}

	if len(ve.Errors) > 0 {
		return ve
	}
	return nil
}

func (n NewNotification) ToNotification(id string, now time.Time) *Notification {
	nextAttemptAt := now
	if n.ScheduledAt != nil {
		nextAttemptAt = *n.ScheduledAt
	}
	return &Notification{
		ID:            id,
		Recipient:     n.Recipient,
		Channel:       n.Channel,
		Content:       n.Content,
		Priority:      n.Priority,
		Status:        StatusPending,
		MaxAttempts:   DefaultMaxAttempts,
		NextAttemptAt: nextAttemptAt,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}
