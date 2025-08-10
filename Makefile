.PHONY: build docker push run
IMAGE=ghcr.io/josinaldojr/rinha2025-api:latest

build:
	GOEXPERIMENT=loopvar go build -o bin/api ./cmd/api

docker:
	docker build -t $(IMAGE) .

push:
	docker push $(IMAGE)

run:
	docker compose up --build
