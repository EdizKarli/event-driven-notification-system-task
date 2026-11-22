package worker

import (
	"testing"
	"time"
)

func TestBackoffGrowsExponentially(t *testing.T) {
	b := Backoff{Base: 5 * time.Second, Max: 5 * time.Minute, rand: func() float64 { return 1 }}

	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w {
			t.Errorf("attempt %d: expected %v, got %v", i+1, w, got)
		}
	}
}

func TestBackoffIsCapped(t *testing.T) {
	b := Backoff{Base: 5 * time.Second, Max: time.Minute, rand: func() float64 { return 1 }}

	if got := b.Delay(20); got != time.Minute {
		t.Fatalf("expected delay capped at 1m, got %v", got)
	}
}

func TestBackoffJitterRange(t *testing.T) {
	low := Backoff{Base: 10 * time.Second, Max: time.Minute, rand: func() float64 { return 0 }}
	high := Backoff{Base: 10 * time.Second, Max: time.Minute, rand: func() float64 { return 1 }}

	if got := low.Delay(1); got != 5*time.Second {
		t.Errorf("expected lowest delay 5s, got %v", got)
	}
	if got := high.Delay(1); got != 10*time.Second {
		t.Errorf("expected highest delay 10s, got %v", got)
	}

	jittered := Backoff{Base: 10 * time.Second, Max: time.Minute}
	for i := 0; i < 100; i++ {
		got := jittered.Delay(2)
		if got < 10*time.Second || got > 20*time.Second {
			t.Fatalf("delay %v outside expected range 10s-20s", got)
		}
	}
}
