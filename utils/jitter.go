package utils

import (
	"math/rand"
	"sync"
	"time"
)

type JitteredTicker struct {
	C         chan struct{}
	interval  time.Duration
	closeOnce sync.Once
	closed    chan struct{}
}

func NewJitteredTicker(interval time.Duration) *JitteredTicker {
	t := &JitteredTicker{
		C:        make(chan struct{}, 1),
		interval: interval,
		closed:   make(chan struct{}),
	}
	go t.tick()
	return t
}

func (t *JitteredTicker) tick() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	timer := time.NewTimer(Jitter(r, t.interval))
	defer timer.Stop()

	for {
		select {
		case <-t.closed:
			return
		case <-timer.C:
			t.C <- struct{}{}
			timer.Reset(Jitter(r, t.interval))
		}
	}
}

func (t *JitteredTicker) Stop() {
	t.closeOnce.Do(func() { close(t.closed) })
}

func Jitter(r *rand.Rand, t time.Duration) time.Duration {
	nanos := r.NormFloat64()*float64(t/4) + float64(t)
	if nanos <= 0 {
		nanos = 1
	}
	return time.Duration(nanos)
}
