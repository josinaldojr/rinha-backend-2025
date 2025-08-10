package decider

import (
	"context"
	"time"

	"github.com/josinaldojr/rinha-backend-2025/internal/processors"
	"github.com/josinaldojr/rinha-backend-2025/internal/repo"
)

const lockKey int64 = 987654321

func StartHealthWorker(ctx context.Context, db repo.DB, proc *processors.Client, d *Decider) {
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				ok, err := db.TryGlobalLock(ctx, lockKey)
				if err != nil || !ok {
					continue
				}

				func() {
					defer db.UnlockGlobal(ctx, lockKey)
					ctx2, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
					defer cancel()

					if hi, err := proc.Health(ctx2, processors.ProviderDefault); err == nil {
						d.UpdateHealth(ProviderDefault, hi.Failing, hi.MinResponseMs)
					}
					if hi, err := proc.Health(ctx2, processors.ProviderFallback); err == nil {
						d.UpdateHealth(ProviderFallback, hi.Failing, hi.MinResponseMs)
					}
				}()
			}
		}
	}()
}
