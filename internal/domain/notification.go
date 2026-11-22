package domain

import (
	"errors"
	"time"
)

const DefaultMaxAttempts = 5

var (
	ErrNotFound       = errors.New("notification not found")
	ErrBatchNotFound  = errors.New("batch not found")
	ErrNotCancellable = errors.New("notification cannot be cancelled")
	ErrDuplicate      = errors.New("idempotency key already used")
)

type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

func (p Priority) Rank() int {
	switch p {
	case PriorityHigh:
		return 0
	case PriorityLow:
		return 2
	default:
		return 1
	}
}

func PriorityFromRank(rank int) Priority {
	switch rank {
	case 0:
		return PriorityHigh
	case 2:
		return PriorityLow
	default:
		return PriorityNormal
	}
}

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusProcessing, StatusSent, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

func (s Status) CanCancel() bool {
	return s == StatusPending
}

type Notification struct {
	ID                string     `json:"id"`
	BatchID           *string    `json:"batch_id,omitempty"`
	IdempotencyKey    *string    `json:"idempotency_key,omitempty"`
	Recipient         string     `json:"recipient"`
	Channel           Channel    `json:"channel"`
	Content           string     `json:"content"`
	Priority          Priority   `json:"priority"`
	Status            Status     `json:"status"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"max_attempts"`
	NextAttemptAt     time.Time  `json:"next_attempt_at"`
	ProviderMessageID *string    `json:"provider_message_id,omitempty"`
	LastError         *string    `json:"last_error,omitempty"`
	CorrelationID     string     `json:"correlation_id,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	SentAt            *time.Time `json:"sent_at,omitempty"`
}

type Batch struct {
	ID           string         `json:"id"`
	Total        int            `json:"total"`
	StatusCounts map[Status]int `json:"status_counts"`
	CreatedAt    time.Time      `json:"created_at"`
}

type Cursor struct {
	CreatedAt time.Time
	ID        string
}

type ListFilter struct {
	Status  *Status
	Channel *Channel
	BatchID *string
	From    *time.Time
	To      *time.Time
	After   *Cursor
	Limit   int
}

type ChannelQueue struct {
	Ready      map[Priority]int `json:"ready"`
	Scheduled  int              `json:"scheduled"`
	Processing int              `json:"processing"`
}

func NewQueueDepth() map[Channel]*ChannelQueue {
	depth := make(map[Channel]*ChannelQueue, len(Channels))
	for _, ch := range Channels {
		depth[ch] = &ChannelQueue{Ready: map[Priority]int{
			PriorityHigh:   0,
			PriorityNormal: 0,
			PriorityLow:    0,
		}}
	}
	return depth
}
