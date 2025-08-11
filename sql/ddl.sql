create extension if not exists pgcrypto;

create table if not exists payments (
  id uuid primary key default gen_random_uuid(),
  correlation_id uuid not null unique,
  amount numeric(18,2) not null,
  provider text not null check (provider in ('default','fallback')),
  status text not null check (status in ('PENDING','DISPATCHING','PROCESSED','FAILED')), -- add DISPATCHING
  requested_at timestamptz not null default now()
);

create index if not exists idx_payments_provider_requested_at
  on payments(provider, requested_at);

-- chave para fila: varredura por status e tempo
create index if not exists idx_payments_status_requested_at
  on payments(status, requested_at);

-- rode uma vez (após dropar/recriar a tabela, crie de novo)
create index if not exists idx_payments_summary_proc
  on payments (provider, requested_at)
  where status = 'PROCESSED';

