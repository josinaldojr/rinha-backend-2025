package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/josinaldojr/rinha2025-api/internal/decider"
	"github.com/josinaldojr/rinha2025-api/internal/processors"
	"github.com/josinaldojr/rinha2025-api/internal/repo"
)

type Handler struct {
	db   repo.DB
	proc *processors.Client
	dec  *decider.Decider
}

func New(db repo.DB, proc *processors.Client, d *decider.Decider) *Handler {
	return &Handler{db: db, proc: proc, dec: d}
}

type paymentIn struct {
	CorrelationID uuid.UUID       `json:"correlationId"`
	Amount        decimal.Decimal `json:"amount"`
}

func (h *Handler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	var in paymentIn
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if in.CorrelationID == uuid.Nil || !in.Amount.IsPositive() {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 300*time.Millisecond)
	defer cancel()

	already, err := h.db.EnsureUnique(ctx, in.CorrelationID, in.Amount)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	if already {
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "OK", "idempotent": true})
		return
	}

	provider := h.dec.Choose()

	started := time.Now()
	err = h.proc.Pay(ctx, processors.Provider(provider), in.CorrelationID, in.Amount)
	h.dec.Observe(decider.Provider(provider), time.Since(started), err)

	status := repo.StatusFailed
	if err == nil {
		status = repo.StatusProcessed
	}

	_ = h.db.Finish(ctx, in.CorrelationID, repo.Provider(provider), status)

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"provider": provider, "status": string(status),
	})
}

func (h *Handler) Summary(w http.ResponseWriter, r *http.Request) {
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	from, to := repo.ParseISO(fromStr), repo.ParseISO(toStr)

	defCnt, defAmt, _ := h.db.Summary(r.Context(), repo.ProviderDefault, from, to)
	fbCnt, fbAmt, _ := h.db.Summary(r.Context(), repo.ProviderFallback, from, to)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"default":  map[string]any{"totalRequests": defCnt, "totalAmount": defAmt},
		"fallback": map[string]any{"totalRequests": fbCnt, "totalAmount": fbAmt},
	})
}
