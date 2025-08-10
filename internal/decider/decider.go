package decider

import (
	"errors"
	"sync"
	"time"
)

type Provider string
const (
	ProviderDefault Provider = "default"
	ProviderFallback Provider = "fallback"
)

type Decider struct {
	mu sync.RWMutex
	lat map[Provider]time.Duration
	err map[Provider]int
	cnt map[Provider]int
}

func New() *Decider {
	return &Decider{
		lat: map[Provider]time.Duration{ProviderDefault: 50 * time.Millisecond, ProviderFallback: 60 * time.Millisecond},
		err: map[Provider]int{ProviderDefault:0, ProviderFallback:0},
		cnt: map[Provider]int{ProviderDefault:0, ProviderFallback:0},
	}
}

func (d *Decider) Choose() string {
	d.mu.RLock(); defer d.mu.RUnlock()
	if d.cnt[ProviderDefault] > 20 && float64(d.err[ProviderDefault])/float64(d.cnt[ProviderDefault]) > 0.2 {
		return string(ProviderFallback)
	}
	return string(ProviderDefault)
}

func (d *Decider) Observe(p Provider, dur time.Duration, err error) {
	d.mu.Lock(); defer d.mu.Unlock()
	d.cnt[p]++
	if err != nil { d.err[p]++ }
	
	prev := d.lat[p]
	alpha := 0.3
	d.lat[p] = time.Duration(alpha*float64(dur) + (1-alpha)*float64(prev))
}

var ErrCBOpen = errors.New("circuit open")
