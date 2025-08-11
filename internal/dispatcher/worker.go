package dispatcher

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/josinaldojr/rinha-backend-2025/internal/decider"
	"github.com/josinaldojr/rinha-backend-2025/internal/processors"
	"github.com/josinaldojr/rinha-backend-2025/internal/repo"
)

const (
	dispatchLoopEvery = 20 * time.Millisecond // 10ms -> 20ms
	dispatchBatchSize = 64                    // 128 -> 64
)

func Start(ctx context.Context, db repo.DB, proc *processors.Client, d *decider.Decider) {
	go func() {
		t := time.NewTicker(dispatchLoopEvery)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				items, err := db.ClaimPendingBatch(ctx, dispatchBatchSize)
				if err != nil || len(items) == 0 {
					continue
				}
				for _, it := range items {
					sentAt := time.Now().UTC()
					prov := processors.Provider(d.Choose())

					start := time.Now()
					err := proc.Pay(ctx, prov, it.CorrelationID, it.Amount, sentAt)
					d.Observe(decider.Provider(prov), time.Since(start), err)

					status := repo.StatusPending
					if err == nil {
						status = repo.StatusProcessed
					}
					// finaliza com provider escolhido e mesmo sentAt
					_ = db.Finish(ctx, it.CorrelationID, repo.Provider(prov), status, sentAt)
				}
			}
		}
	}()
}

func quickConfirm(ctx context.Context, proc *processors.Client, prov processors.Provider, id uuid.UUID) bool {
	httpc := &http.Client{Timeout: 400 * time.Millisecond} // um pouco mais folgado
	base := proc.DefaultBase()
	if prov == processors.ProviderFallback {
		base = proc.FallbackBase()
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/payments/%s", base, id.String()), nil)
	resp, err := httpc.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}
