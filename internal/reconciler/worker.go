package reconciler

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/josinaldojr/rinha-backend-2025/internal/processors"
	"github.com/josinaldojr/rinha-backend-2025/internal/repo"
)

const (
    maxProbeBatch    = 512           // 256 -> 512
    failAfter        = 5 * time.Second // 8s -> 5s (falha mais cedo se realmente não existe)
    probeHTTPTimeout = 400 * time.Millisecond // 200ms -> 400ms (menos falso negativo)
    loopEvery        = 50 * time.Millisecond   // 100ms -> 50ms
)

func Start(ctx context.Context, db repo.DB, proc *processors.Client) {
	go func() {
		t := time.NewTicker(loopEvery)
		defer t.Stop()
		httpc := &http.Client{Timeout: probeHTTPTimeout}

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				ids, provs, times, err := db.ListInFlightWithTime(ctx, maxProbeBatch)
				if err != nil || len(ids) == 0 {
					continue
				}
				now := time.Now()
				for i, id := range ids {
					pv := processors.Provider(provs[i])
					if ok := probe(ctx, httpc, proc, pv, id); ok {
						_ = db.MarkProcessed(ctx, id)
						continue
					}
					if now.Sub(times[i]) >= failAfter {
						_ = db.MarkFailed(ctx, id)
					}
				}
			}
		}
	}()
}

func probe(ctx context.Context, httpc *http.Client, proc *processors.Client, pv processors.Provider, id uuid.UUID) bool {
	base := procBaseURL(proc, pv)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/payments/%s", base, id.String()), nil)
	resp, err := httpc.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func procBaseURL(proc *processors.Client, pv processors.Provider) string {
	if pv == processors.ProviderFallback {
		return proc.FallbackBase()
	}
	return proc.DefaultBase()
}
