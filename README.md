# Rinha 2025 – Backend Skeleton (Go)

Serviço em Go que implementa:
- `POST /payments`
- `GET /payments-summary`

## Stack
- Go 1.22 (chi, pgx, uuid, decimal)
- PostgreSQL 16
- nginx (load balancer)
- Docker Compose (porta 9999, limites de 1.5 CPU / 350MB)

## Variáveis
- `DATABASE_URL` (default: postgres://rinha:rinha@postgres:5432/rinha?sslmode=disable)
- `PP_DEFAULT_URL` (default: http://payment-processor-default:8080)
- `PP_FALLBACK_URL` (default: http://payment-processor-fallback:8080)

## Subir local
1. Suba os Payment Processors do repositório da Rinha (cria rede `payment-processor`).
2. `docker compose up --build`

## Endpoints
- `POST /payments`
  - body: `{ "correlationId": "uuid", "amount": 19.90 }`
  - 202 Accepted com `{ "provider": "...", "status": "..." }`
- `GET /payments-summary?from=&to=`
  - retorno no formato exigido pela prova.
