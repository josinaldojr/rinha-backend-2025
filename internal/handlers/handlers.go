package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/josinaldojr/rinha-backend-2025/internal/repo"
	"github.com/shopspring/decimal"
)

type Handler struct {
	db repo.DB
}

func New(db repo.DB) *Handler {
	return &Handler{db: db}
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

	// caminho super curto: só enfileira e responde
	ctx, cancel := context.WithTimeout(r.Context(), 450*time.Millisecond) // 250 -> 450
	defer cancel()

	already, err := h.db.EnsureUnique(ctx, in.CorrelationID, in.Amount)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if already {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "OK", "idempotent": true})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   "QUEUED",
		"queuedAt": time.Now().UTC(),
	})
}

func (h *Handler) Summary(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
  defer cancel()

	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")
	from, to := repo.ParseISO(fromStr), repo.ParseISO(toStr)

	defCnt, defAmt, _ := h.db.Summary(ctx, repo.ProviderDefault, from, to)
	fbCnt, fbAmt, _ := h.db.Summary(ctx, repo.ProviderFallback, from, to)

	_ = json.NewEncoder(w).Encode(map[string]any{
		"default":  map[string]any{"totalRequests": defCnt, "totalAmount": defAmt},
		"fallback": map[string]any{"totalRequests": fbCnt, "totalAmount": fbAmt},
	})
}
