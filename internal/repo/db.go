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

// Tipos usados pelo handlers/decider
type Provider string
const (
	ProviderDefault  Provider = "default"
	ProviderFallback Provider = "fallback"
)

type Status string
const (
	StatusProcessed Status = "PROCESSED"
	StatusFailed    Status = "FAILED"
)

// Contrato usado nos handlers
type DB interface {
	Close(ctx context.Context)
	EnsureUnique(ctx context.Context, correlationID uuid.UUID, amount decimal.Decimal) (already bool, err error)
	Finish(ctx context.Context, correlationID uuid.UUID, provider Provider, status Status) error
	Summary(ctx context.Context, provider Provider, from, to *time.Time) (count int64, total decimal.Decimal, err error)
	TryGlobalLock(ctx context.Context, key int64) (bool, error)
	UnlockGlobal(ctx context.Context, key int64) error
}

type PgxDB struct{ pool *pgxpool.Pool }

// Open cria o pool (usado no main.go)
func Open(ctx context.Context, url string) (*PgxDB, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil { return nil, err }
	return &PgxDB{pool: pool}, nil
}

func (p *PgxDB) Close(ctx context.Context) { p.pool.Close() }

// EnsureUnique faz um insert "placeholder" (FAILED) e detecta duplicidade por correlation_id.
// Retorna already=true se já existia antes (idempotência).
func (p *PgxDB) EnsureUnique(ctx context.Context, correlationID uuid.UUID, amount decimal.Decimal) (bool, error) {
	var dummy int
	err := p.pool.QueryRow(ctx, `
		INSERT INTO payments (correlation_id, amount, provider, status, requested_at)
		VALUES ($1, $2, 'default', 'FAILED', now())
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

// Finish atualiza provider/status após a tentativa de envio ao processor.
func (p *PgxDB) Finish(ctx context.Context, correlationID uuid.UUID, provider Provider, status Status) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE payments SET provider=$1, status=$2 WHERE correlation_id=$3
	`, provider, status, correlationID)
	return err
}

// Summary agrega contagem e soma por provider/status=PROCESSED, com filtros opcionais from/to.
func (p *PgxDB) Summary(ctx context.Context, provider Provider, from, to *time.Time) (int64, decimal.Decimal, error) {
	q := `SELECT count(*), COALESCE(sum(amount), 0) FROM payments WHERE provider=$1 AND status='PROCESSED'`
	args := []any{provider}

	// Filtros de período (em requested_at)
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

// TryGlobalLock/UnlockGlobal: advisory lock para coordenar o health-check (1 nó por cluster).
func (p *PgxDB) TryGlobalLock(ctx context.Context, key int64) (bool, error) {
	var ok bool
	err := p.pool.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&ok)
	return ok, err
}

func (p *PgxDB) UnlockGlobal(ctx context.Context, key int64) error {
	var ok bool
	return p.pool.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, key).Scan(&ok)
}

// ParseISO: helper para parsear timestamps ISO UTC do /payments-summary?from&to
func ParseISO(s string) *time.Time {
	if s == "" { return nil }
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil { return nil }
	return &t
}