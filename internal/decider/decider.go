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
	latEWMA     time.Duration // latência suavizada (EWMA)
	errs        int           // erros recentes acumulados
	total       int           // amostras recentes acumuladas
	cbOpenUntil time.Time     // até quando o CB permanece aberto
	failing     bool          // sinal do health-check (endpoint externo)
	minRespMs   int           // sinal do health-check (endpoint externo)
	updatedAt   time.Time     // última atualização de health
}

type Decider struct {
	mu           sync.RWMutex
	s            map[Provider]*state
	marginMs     int           // tolerância de latência para manter default
	epsilon      float64       // probabilidade de exploração
	cbFailRate   float64       // limiar de taxa de erro para abrir CB
	cbMinSamples int           // nº mínimo de amostras para avaliar CB
	cbTimeout    time.Duration // duração do CB aberto (half-open após)
}

func New() *Decider {
	return &Decider{
		s: map[Provider]*state{
			ProviderDefault:  {latEWMA: 50 * time.Millisecond},
			ProviderFallback: {latEWMA: 60 * time.Millisecond},
		},
		marginMs:     50,        // default pode ser até 50ms mais lento que fallback
		epsilon:      0.03,      // 3% de exploração
		cbFailRate:   0.20,      // abre CB com >=20% de erro
		cbMinSamples: 20,        // em pelo menos 20 amostras
		cbTimeout:    2 * time.Second,
	}
}

// Choose retorna o provider recomendado no momento.
// Estratégia: prioriza default; considera CB/health; usa ε-greedy; tolera pequena diferença de latência.
func (d *Decider) Choose() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// ε-greedy: pequena exploração para detectar recuperação sem "colar" em uma decisão
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

	// Se um está bloqueado/falhando e o outro não, escolha o que está livre
	if defBlocked && !fbBlocked {
		return string(ProviderFallback)
	}
	if fbBlocked && !defBlocked {
		return string(ProviderDefault)
	}
	// Ambos ruins: escolha o de menor latência estimada (minimiza p99)
	if defBlocked && fbBlocked {
		if def.latEWMA <= fb.latEWMA {
			return string(ProviderDefault)
		}
		return string(ProviderFallback)
	}

	// Ambos disponíveis: prioriza default salvo se estiver muito pior
	margin := time.Duration(d.marginMs) * time.Millisecond
	if def.latEWMA <= fb.latEWMA+margin {
		return string(ProviderDefault)
	}
	return string(ProviderFallback)
}

// Observe registra uma observação de latência/erro para o provider.
// Atualiza EWMA de latência e decide abertura do Circuit Breaker por taxa de erro.
func (d *Decider) Observe(p Provider, dur time.Duration, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	st := d.ensureState(p)
	st.total++
	if err != nil {
		st.errs++
	}

	// EWMA simples
	const alpha = 0.3
	st.latEWMA = time.Duration(alpha*float64(dur) + (1-alpha)*float64(st.latEWMA))

	// Circuit Breaker: abre se taxa de erro recente excede o limiar e há amostras suficientes
	if st.total >= d.cbMinSamples {
		rate := float64(st.errs) / float64(st.total)
		if rate >= d.cbFailRate {
			st.cbOpenUntil = time.Now().Add(d.cbTimeout)
			// Reduz contadores para permitir medição de recuperação depois
			st.errs = 0
			st.total = 0
		}
	}
}

// UpdateHealth injeta sinais do health-check no decisor.
// failing=true abrevia a decisão (trata como indisponível); minRespMs pode ser usado para heurísticas futuras.
func (d *Decider) UpdateHealth(p Provider, failing bool, minRespMs int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.ensureState(p)
	st.failing = failing
	st.minRespMs = minRespMs
	st.updatedAt = time.Now()
}

// ensureState garante que o mapa de estado tem uma entrada para o provider.
func (d *Decider) ensureState(p Provider) *state {
	if st, ok := d.s[p]; ok {
		return st
	}
	st := &state{latEWMA: 80 * time.Millisecond}
	d.s[p] = st
	return st
}