package repo

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type Provider string
const (
	ProviderDefault Provider = "default"
	ProviderFallback Provider = "fallback"
)

type Status string
const (
	StatusProcessed Status = "PROCESSED"
	StatusFailed    Status = "FAILED"
)

type DB interface {
	Close(ctx context.Context)
	EnsureUnique(ctx context.Context, correlationID uuid.UUID, amount decimal.Decimal) (already bool, err error)
	Finish(ctx context.Context, correlationID uuid.UUID, provider Provider, status Status) error
	Summary(ctx context.Context, provider Provider, from, to *time.Time) (count int64, total decimal.Decimal, err error)
}

type PgxDB struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*PgxDB, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil { return nil, err }
	return &PgxDB{pool: pool}, nil
}

func (p *PgxDB) Close(ctx context.Context) { p.pool.Close() }

func (p *PgxDB) EnsureUnique(ctx context.Context, correlationID uuid.UUID, amount decimal.Decimal) (bool, error) {
	_, err := p.pool.Exec(ctx, `
		insert into payments (correlation_id, amount, provider, status, requested_at)
		values ($1, $2, 'default', 'FAILED', now())
		on conflict (correlation_id) do nothing;
	`, correlationID, amount)
	if err != nil { return false, err }
	var exists bool
	err = p.pool.QueryRow(ctx, `select exists(select 1 from payments where correlation_id=$1)`, correlationID).Scan(&exists)
	return exists && true, err
}

func (p *PgxDB) Finish(ctx context.Context, correlationID uuid.UUID, provider Provider, status Status) error {
	_, err := p.pool.Exec(ctx, `
		update payments set provider=$1, status=$2 where correlation_id=$3
	`, provider, status, correlationID)
	return err
}

func ParseISO(s string) *time.Time {
	if s == "" { return nil }
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil { return nil }
	return &t
}

func (p *PgxDB) Summary(ctx context.Context, provider Provider, from, to *time.Time) (int64, decimal.Decimal, error) {
	q := `select count(*), coalesce(sum(amount),0) from payments where provider=$1 and status='PROCESSED'`
	args := []any{provider}
	if from != nil { q += " and requested_at >= $2"; args = append(args, *from) }
	if to != nil {
		if from != nil { q += " and requested_at <= $3" } else { q += " and requested_at <= $2" }
		args = append(args, *to)
	}
	var count int64; var totalStr string
	err := p.pool.QueryRow(ctx, q, args...).Scan(&count, &totalStr)
	if err != nil { return 0, decimal.Zero, err }
	total, _ := decimal.NewFromString(totalStr)
	return count, total, nil
}
