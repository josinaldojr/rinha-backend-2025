package repo

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// Tipos usados pelos handlers/decider
type Provider string

const (
	ProviderDefault  Provider = "default"
	ProviderFallback Provider = "fallback"
)

type Status string

const (
	StatusPending     Status = "PENDING"
	StatusDispatching Status = "DISPATCHING"
	StatusProcessed   Status = "PROCESSED"
	StatusFailed      Status = "FAILED"
)

// Item de batch para o dispatcher
type BatchItem struct {
	ID            uuid.UUID
	CorrelationID uuid.UUID
	Amount        decimal.Decimal
}

// Contrato usado nos handlers e workers
type DB interface {
	Close(ctx context.Context)

	// Hot path (handler)
	EnsureUnique(ctx context.Context, correlationID uuid.UUID, amount decimal.Decimal) (already bool, err error)

	// Finalização após chamada ao processor
	Finish(ctx context.Context, correlationID uuid.UUID, provider Provider, status Status, requestedAt time.Time) error

	// Summary
	Summary(ctx context.Context, provider Provider, from, to *time.Time) (count int64, total decimal.Decimal, err error)

	// Health worker (advisory lock)
	TryGlobalLock(ctx context.Context, key int64) (bool, error)
	UnlockGlobal(ctx context.Context, key int64) error

	// Dispatcher: pega lote PENDING -> marca como DISPATCHING e retorna os itens
	ClaimPendingBatch(ctx context.Context, limit int) ([]BatchItem, error)

	// Reconciliação
	ListInFlightWithTime(ctx context.Context, limit int) ([]uuid.UUID, []Provider, []time.Time, error)
	MarkProcessed(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID) error
}

type PgxDB struct{ pool *pgxpool.Pool }

// Open cria o pool (usado no main.go)
func Open(ctx context.Context, url string) (*PgxDB, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	// Menos fsync por transação; bom para latência em ambiente de teste
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = "rinha-api"
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PgxDB{pool: pool}, nil
}

func (p *PgxDB) Close(ctx context.Context) { p.pool.Close() }

// EnsureUnique: insere placeholder (PENDING) e detecta duplicidade por correlation_id.
func (p *PgxDB) EnsureUnique(ctx context.Context, correlationID uuid.UUID, amount decimal.Decimal) (bool, error) {
	var dummy int
	err := p.pool.QueryRow(ctx, `
		INSERT INTO payments (correlation_id, amount, provider, status, requested_at)
		VALUES ($1, $2, 'default', 'PENDING', now())
		ON CONFLICT (correlation_id) DO NOTHING
		RETURNING 1
	`, correlationID, amount).Scan(&dummy)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Não inseriu porque já existia (idempotente)
			return true, nil
		}
		return false, err
	}
	// Inseriu agora -> não existia ainda
	return false, nil
}

// Finish: atualiza provider/status e requested_at com o timestamp usado no processor.
func (p *PgxDB) Finish(ctx context.Context, correlationID uuid.UUID, provider Provider, status Status, requestedAt time.Time) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE payments
		   SET provider=$1, status=$2, requested_at=$4
		 WHERE correlation_id=$3
	`, provider, status, correlationID, requestedAt)
	return err
}

// Summary agrega contagem e soma por provider/status=PROCESSED, com filtros opcionais from/to.
func (p *PgxDB) Summary(ctx context.Context, provider Provider, from, to *time.Time) (int64, decimal.Decimal, error) {
	q := `SELECT count(*), COALESCE(sum(amount), 0) FROM payments WHERE provider=$1 AND status='PROCESSED'`
	args := []any{provider}

	if from != nil {
		q += " AND requested_at >= $2"
		args = append(args, *from)
	}
	if to != nil {
		if from != nil {
			q += " AND requested_at <= $3"
		} else {
			q += " AND requested_at <= $2"
		}
		args = append(args, *to)
	}

	var count int64
	var totalStr string
	if err := p.pool.QueryRow(ctx, q, args...).Scan(&count, &totalStr); err != nil {
		return 0, decimal.Zero, err
	}
	total, _ := decimal.NewFromString(totalStr)
	return count, total, nil
}

// Advisory lock para o health worker
func (p *PgxDB) TryGlobalLock(ctx context.Context, key int64) (bool, error) {
	var ok bool
	err := p.pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&ok)
	return ok, err
}

func (p *PgxDB) UnlockGlobal(ctx context.Context, key int64) error {
	var ok bool
	return p.pool.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, key).Scan(&ok)
}

// ClaimPendingBatch: bloqueia e marca PENDING -> DISPATCHING em um único statement.
// Retorna o lote para processamento fora da transação (sem segurar lock).
func (p *PgxDB) ClaimPendingBatch(ctx context.Context, limit int) ([]BatchItem, error) {
	rows, err := p.pool.Query(ctx, `
		WITH cte AS (
		  SELECT id
		       , correlation_id
		       , amount
		    FROM payments
		   WHERE status = 'PENDING'
		   ORDER BY requested_at
		   FOR UPDATE SKIP LOCKED
		   LIMIT $1
		),
		upd AS (
		  UPDATE payments p
		     SET status = 'DISPATCHING'
		    FROM cte
		   WHERE p.id = cte.id
		)
		SELECT cte.id, cte.correlation_id, cte.amount
		  FROM cte
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []BatchItem
	for rows.Next() {
		var id, cid uuid.UUID
		var amtStr string
		if err := rows.Scan(&id, &cid, &amtStr); err != nil {
			return nil, err
		}
		amt, _ := decimal.NewFromString(amtStr)
		out = append(out, BatchItem{ID: id, CorrelationID: cid, Amount: amt})
	}
	return out, rows.Err()
}

// Reconciliação: busca itens em voo (PENDING e DISPATCHING) do mais antigo.
func (p *PgxDB) ListInFlightWithTime(ctx context.Context, limit int) ([]uuid.UUID, []Provider, []time.Time, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT correlation_id, provider, requested_at
		  FROM payments
		 WHERE status IN ('PENDING','DISPATCHING')
		 ORDER BY requested_at ASC
		 LIMIT $1
	`, limit)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	var provs []Provider
	var times []time.Time
	for rows.Next() {
		var id uuid.UUID
		var pv Provider
		var ts time.Time
		if err := rows.Scan(&id, &pv, &ts); err != nil {
			return nil, nil, nil, err
		}
		ids, provs, times = append(ids, id), append(provs, pv), append(times, ts)
	}
	return ids, provs, times, rows.Err()
}

func (p *PgxDB) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	_, err := p.pool.Exec(ctx, `UPDATE payments SET status='PROCESSED' WHERE correlation_id=$1`, id)
	return err
}

func (p *PgxDB) MarkFailed(ctx context.Context, id uuid.UUID) error {
	_, err := p.pool.Exec(ctx, `UPDATE payments SET status='FAILED' WHERE correlation_id=$1`, id)
	return err
}

// ParseISO: helper para parsear timestamps ISO UTC do /payments-summary?from&to
func ParseISO(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil
	}
	return &t
}
