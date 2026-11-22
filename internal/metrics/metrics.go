package metrics

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/EdizKarli/event-driven-notification-system-task/internal/domain"
)

const sampleSize = 1000

type LatencySummary struct {
	Samples int     `json:"samples"`
	Avg     float64 `json:"avg"`
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	P99     float64 `json:"p99"`
}

type ChannelStats struct {
	Sent              int64          `json:"sent"`
	Failed            int64          `json:"failed"`
	Retried           int64          `json:"retried"`
	SuccessRate       *float64       `json:"success_rate"`
	ProviderLatencyMs LatencySummary `json:"provider_latency_ms"`
	DeliveryLatencyMs LatencySummary `json:"delivery_latency_ms"`
}

type Totals struct {
	Sent        int64    `json:"sent"`
	Failed      int64    `json:"failed"`
	Retried     int64    `json:"retried"`
	SuccessRate *float64 `json:"success_rate"`
}

type Snapshot struct {
	UptimeSeconds int64                           `json:"uptime_seconds"`
	Totals        Totals                          `json:"totals"`
	Channels      map[domain.Channel]ChannelStats `json:"channels"`
}

type samples struct {
	values []float64
	next   int
}

func (s *samples) add(v float64) {
	if len(s.values) < sampleSize {
		s.values = append(s.values, v)
		return
	}
	s.values[s.next] = v
	s.next = (s.next + 1) % sampleSize
}

func (s *samples) summary() LatencySummary {
	n := len(s.values)
	if n == 0 {
		return LatencySummary{}
	}

	sorted := make([]float64, n)
	copy(sorted, s.values)
	sort.Float64s(sorted)

	var sum float64
	for _, v := range sorted {
		sum += v
	}

	return LatencySummary{
		Samples: n,
		Avg:     round(sum / float64(n)),
		P50:     round(percentile(sorted, 0.50)),
		P95:     round(percentile(sorted, 0.95)),
		P99:     round(percentile(sorted, 0.99)),
	}
}

func percentile(sorted []float64, p float64) float64 {
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	return sorted[i]
}

func round(v float64) float64 {
	return math.Round(v*100) / 100
}

func successRate(sent, failed int64) *float64 {
	if sent+failed == 0 {
		return nil
	}
	rate := round(float64(sent) / float64(sent+failed))
	return &rate
}

type channelState struct {
	sent     int64
	failed   int64
	retried  int64
	provider samples
	delivery samples
}

type Recorder struct {
	mu       sync.Mutex
	started  time.Time
	channels map[domain.Channel]*channelState
}

func New() *Recorder {
	channels := make(map[domain.Channel]*channelState, len(domain.Channels))
	for _, ch := range domain.Channels {
		channels[ch] = &channelState{}
	}
	return &Recorder{started: time.Now(), channels: channels}
}

func (r *Recorder) state(ch domain.Channel) *channelState {
	s, ok := r.channels[ch]
	if !ok {
		s = &channelState{}
		r.channels[ch] = s
	}
	return s
}

func (r *Recorder) RecordSent(ch domain.Channel, providerLatency, deliveryLatency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.state(ch)
	s.sent++
	s.provider.add(float64(providerLatency.Microseconds()) / 1000)
	s.delivery.add(float64(deliveryLatency.Microseconds()) / 1000)
}

func (r *Recorder) RecordFailed(ch domain.Channel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state(ch).failed++
}

func (r *Recorder) RecordRetry(ch domain.Channel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state(ch).retried++
}

func (r *Recorder) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	snap := Snapshot{
		UptimeSeconds: int64(time.Since(r.started).Seconds()),
		Channels:      make(map[domain.Channel]ChannelStats, len(r.channels)),
	}
	for ch, s := range r.channels {
		snap.Channels[ch] = ChannelStats{
			Sent:              s.sent,
			Failed:            s.failed,
			Retried:           s.retried,
			SuccessRate:       successRate(s.sent, s.failed),
			ProviderLatencyMs: s.provider.summary(),
			DeliveryLatencyMs: s.delivery.summary(),
		}
		snap.Totals.Sent += s.sent
		snap.Totals.Failed += s.failed
		snap.Totals.Retried += s.retried
	}
	snap.Totals.SuccessRate = successRate(snap.Totals.Sent, snap.Totals.Failed)
	return snap
}
