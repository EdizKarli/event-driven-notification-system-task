package worker

import (
	"math/rand/v2"
	"time"
)

type Backoff struct {
	Base time.Duration
	Max  time.Duration
	rand func() float64
}

func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	d := b.Base
	for i := 1; i < attempt && d < b.Max; i++ {
		d *= 2
	}
	if d > b.Max {
		d = b.Max
	}

	random := rand.Float64
	if b.rand != nil {
		random = b.rand
	}
	half := d / 2
	return half + time.Duration(random()*float64(half))
}
