# Multi-stage build
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build     go build -o /out/api ./cmd/api

FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=build /out/api /api
EXPOSE 9999
USER 65534:65534
ENTRYPOINT ["/api"]
