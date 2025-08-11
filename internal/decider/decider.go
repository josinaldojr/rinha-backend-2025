package decider

import (
	"math/rand/v2"
	"sync"
	"time"
)

type Provider string

const (
	ProviderDefault  Provider = "default"
	ProviderFallback Provider = "fallback"
)

type state struct {
	latEWMA     time.Duration
	errs        int
	total       int
	cbOpenUntil time.Time
	failing     bool
	minRespMs   int
	updatedAt   time.Time
}

type Decider struct {
	mu           sync.RWMutex
	s            map[Provider]*state
	marginMs     int
	epsilon      float64
	cbFailRate   float64
	cbMinSamples int
	cbTimeout    time.Duration
}

func New() *Decider {
	return &Decider{
		s: map[Provider]*state{
			ProviderDefault:  {latEWMA: 50 * time.Millisecond},
			ProviderFallback: {latEWMA: 60 * time.Millisecond},
		},
		marginMs:     150,
		epsilon:      0.01,
		cbFailRate:   0.25,
		cbMinSamples: 40,
		cbTimeout:    2 * time.Second,
	}
}

func (d *Decider) Choose() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if rand.Float64() < d.epsilon {
		if rand.IntN(2) == 0 {
			return string(ProviderDefault)
		}
		return string(ProviderFallback)
	}

	now := time.Now()
	def := d.s[ProviderDefault]
	fb := d.s[ProviderFallback]

	defBlocked := def.cbOpenUntil.After(now) || def.failing
	fbBlocked := fb.cbOpenUntil.After(now) || fb.failing

	if defBlocked && !fbBlocked {
		return string(ProviderFallback)
	}
	if fbBlocked && !defBlocked {
		return string(ProviderDefault)
	}
	if defBlocked && fbBlocked {
		if def.latEWMA <= fb.latEWMA {
			return string(ProviderDefault)
		}
		return string(ProviderFallback)
	}

	margin := time.Duration(d.marginMs) * time.Millisecond
	if def.latEWMA <= fb.latEWMA+margin {
		return string(ProviderDefault)
	}
	return string(ProviderFallback)
}

func (d *Decider) Observe(p Provider, dur time.Duration, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.ensureState(p)
	st.total++
	if err != nil {
		st.errs++
	}
	const alpha = 0.3
	st.latEWMA = time.Duration(alpha*float64(dur) + (1-alpha)*float64(st.latEWMA))

	if st.total >= d.cbMinSamples {
		rate := float64(st.errs) / float64(st.total)
		if rate >= d.cbFailRate {
			st.cbOpenUntil = time.Now().Add(d.cbTimeout)
			st.errs = 0
			st.total = 0
		}
	}
}

func (d *Decider) UpdateHealth(p Provider, failing bool, minRespMs int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.ensureState(p)
	st.failing = failing
	st.minRespMs = minRespMs
	st.updatedAt = time.Now()
}

func (d *Decider) ensureState(p Provider) *state {
	if st, ok := d.s[p]; ok {
		return st
	}
	st := &state{latEWMA: 80 * time.Millisecond}
	d.s[p] = st
	return st
}