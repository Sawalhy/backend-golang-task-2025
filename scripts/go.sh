#!/usr/bin/env bash
# Runs the Go toolchain inside Docker. macOS/Linux twin of go.ps1, which is
# PowerShell and does not run here.
#
# Go is not installed on this machine; Docker is, and the project needs it anyway
# for Postgres, RabbitMQ and Redis. Named volumes persist the module and build
# caches, so only the first run pays download cost.
#
#   ./scripts/go.sh build ./...
#   ./scripts/go.sh test ./... -race
#   ./scripts/go.sh mod tidy
#
# If the compose stack is up, the container joins its network, so service names
# resolve exactly as they do between the app containers:
#
#   ./scripts/go.sh run ./cmd/loadtest -url http://api:8080 -n 1000 -c 200
#
# Install Go locally and this script becomes unnecessary — `go` works directly.

set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

net_args=(--add-host=host.docker.internal:host-gateway)
if docker network ls --filter "name=order-processing_default" --format '{{.Name}}' \
     | grep -q '^order-processing_default$'; then
  net_args+=(--network order-processing_default)
fi

exec docker run --rm \
  -v "${repo}:/src" \
  -v "gotask-modcache:/go/pkg/mod" \
  -v "gotask-buildcache:/root/.cache/go-build" \
  -w /src \
  -e GOFLAGS=-buildvcs=false \
  -e TEST_DATABASE_URL="${TEST_DATABASE_URL:-}" \
  -e TEST_RABBITMQ_URL="${TEST_RABBITMQ_URL:-}" \
  -e TEST_REDIS_ADDR="${TEST_REDIS_ADDR:-}" \
  "${net_args[@]}" \
  golang:1.25 go "$@"
