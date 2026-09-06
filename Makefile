COMPOSE ?= docker compose

.PHONY: build test race vet fmt tidy run docker up down logs clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/signald ./cmd/signald

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

run:
	go run ./cmd/signald

docker:
	docker build -t signald:latest .

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f signald

clean:
	rm -rf bin coverage.out