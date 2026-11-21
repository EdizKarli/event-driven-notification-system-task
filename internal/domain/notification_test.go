package domain

import "testing"

func TestPriorityRankRoundTrip(t *testing.T) {
	for _, p := range []Priority{PriorityHigh, PriorityNormal, PriorityLow} {
		if got := PriorityFromRank(p.Rank()); got != p {
			t.Errorf("round trip for %q gave %q", p, got)
		}
	}
	if PriorityHigh.Rank() >= PriorityNormal.Rank() || PriorityNormal.Rank() >= PriorityLow.Rank() {
		t.Error("expected high < normal < low in rank order")
	}
}

func TestStatusCanCancel(t *testing.T) {
	tests := map[Status]bool{
		StatusPending:    true,
		StatusProcessing: false,
		StatusSent:       false,
		StatusFailed:     false,
		StatusCancelled:  false,
	}
	for status, want := range tests {
		if got := status.CanCancel(); got != want {
			t.Errorf("%q.CanCancel() = %v, want %v", status, got, want)
		}
	}
}

func TestChannelValid(t *testing.T) {
	for _, c := range Channels {
		if !c.Valid() {
			t.Errorf("%q should be valid", c)
		}
	}
	if Channel("fax").Valid() {
		t.Error("fax should not be valid")
	}
}
