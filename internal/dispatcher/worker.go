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
	dispatchLoopEvery = 20 * time.Millisecond // controla a pressão no DB
	dispatchBatchSize = 64                    // lotes menores = menos picos de UPDATE

	delayedConfirmAfter = 150 * time.Millisecond // segunda confirmação logo depois do timeout
	confirmHTTPTimeout  = 400 * time.Millisecond // timeout do GET /payments/{id}
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

					if err == nil {
						// caminho feliz: fecha imediato
						_ = db.Finish(ctx, it.CorrelationID, repo.Provider(prov), repo.StatusProcessed, sentAt)
						continue
					}

					// 1) confirmação imediata
					if quickConfirm(ctx, proc, prov, it.CorrelationID) {
						_ = db.Finish(ctx, it.CorrelationID, repo.Provider(prov), repo.StatusProcessed, sentAt)
						continue
					}

					// 2) confirmação "um instante depois"
					delayedCtx, cancel := context.WithTimeout(ctx, confirmHTTPTimeout+50*time.Millisecond)
					timer := time.NewTimer(delayedConfirmAfter)
					select {
					case <-delayedCtx.Done():
						timer.Stop()
					case <-timer.C:
						if quickConfirm(delayedCtx, proc, prov, it.CorrelationID) {
							cancel()
							_ = db.Finish(ctx, it.CorrelationID, repo.Provider(prov), repo.StatusProcessed, sentAt)
							continue
						}
						cancel()
					}

					// ainda não achou? mantém PENDING para o reconciler decidir
					_ = db.Finish(ctx, it.CorrelationID, repo.Provider(prov), repo.StatusPending, sentAt)
				}
			}
		}
	}()
}

// quickConfirm faz uma verificação única no provider logo após erro/timeout do Pay.
// Se o provider já tiver persistido a transação, retornamos true e marcamos PROCESSED.
func quickConfirm(ctx context.Context, proc *processors.Client, prov processors.Provider, id uuid.UUID) bool {
	httpc := &http.Client{Timeout: confirmHTTPTimeout}
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
