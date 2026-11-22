package metrics

import (
	"testing"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

func TestCountsAndSuccessRate(t *testing.T) {
	r := New()
	for i := 0; i < 9; i++ {
		r.RecordSent(domain.ChannelSMS, 10*time.Millisecond, time.Second)
	}
	r.RecordFailed(domain.ChannelSMS)
	r.RecordRetry(domain.ChannelSMS)
	r.RecordRetry(domain.ChannelSMS)
	r.RecordSent(domain.ChannelEmail, 10*time.Millisecond, time.Second)

	snap := r.Snapshot()
	sms := snap.Channels[domain.ChannelSMS]

	if sms.Sent != 9 || sms.Failed != 1 || sms.Retried != 2 {
		t.Fatalf("unexpected sms counts: %+v", sms)
	}
	if sms.SuccessRate == nil || *sms.SuccessRate != 0.9 {
		t.Fatalf("expected sms success rate 0.9, got %v", sms.SuccessRate)
	}
	if snap.Totals.Sent != 10 || snap.Totals.Failed != 1 {
		t.Fatalf("unexpected totals: %+v", snap.Totals)
	}
	if snap.Channels[domain.ChannelPush].SuccessRate != nil {
		t.Fatal("success rate should be empty when nothing was delivered")
	}
}

func TestLatencyPercentiles(t *testing.T) {
	r := New()
	for i := 1; i <= 100; i++ {
		r.RecordSent(domain.ChannelSMS, time.Duration(i)*time.Millisecond, time.Duration(i)*time.Second)
	}

	got := r.Snapshot().Channels[domain.ChannelSMS].ProviderLatencyMs
	if got.Samples != 100 || got.Avg != 50.5 || got.P50 != 50 || got.P95 != 95 || got.P99 != 99 {
		t.Fatalf("unexpected latency summary: %+v", got)
	}

	delivery := r.Snapshot().Channels[domain.ChannelSMS].DeliveryLatencyMs
	if delivery.P50 != 50000 {
		t.Fatalf("expected delivery p50 of 50000ms, got %v", delivery.P50)
	}
}

func TestSamplesAreBounded(t *testing.T) {
	r := New()
	for i := 0; i < sampleSize+500; i++ {
		r.RecordSent(domain.ChannelSMS, time.Millisecond, time.Millisecond)
	}

	if got := r.Snapshot().Channels[domain.ChannelSMS].ProviderLatencyMs.Samples; got != sampleSize {
		t.Fatalf("expected %d samples, got %d", sampleSize, got)
	}
}
