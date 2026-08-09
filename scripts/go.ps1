# Runs the Go toolchain inside Docker.
#
# Go is not installed on this machine; Docker is, and the project needs it anyway
# for Postgres, RabbitMQ and Redis. Named volumes persist the module and build
# caches, so only the first run pays download cost.
#
#   .\scripts\go.ps1 build ./...
#   .\scripts\go.ps1 test ./... -race
#   .\scripts\go.ps1 mod tidy
#
# If the compose stack is up, the container joins its network, so service names
# resolve exactly as they do between the app containers:
#
#   .\scripts\go.ps1 run ./cmd/loadtest -url http://api:8080 -n 1000 -c 200
#
# Install Go locally and this script becomes unnecessary — `go` works directly.

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot

# Attach to the compose network when it exists so `postgres`, `api` and
# `rabbitmq` resolve by name. host.docker.internal is added regardless, for
# reaching services published on the Windows host.
$netArgs = @("--add-host=host.docker.internal:host-gateway")
$network = docker network ls --filter "name=order-processing_default" --format "{{.Name}}" 2>$null
if ($network -match "order-processing_default") {
    $netArgs += @("--network", "order-processing_default")
}

docker run --rm `
  -v "${repo}:/src" `
  -v "gotask-modcache:/go/pkg/mod" `
  -v "gotask-buildcache:/root/.cache/go-build" `
  -w /src `
  -e GOFLAGS=-buildvcs=false `
  -e TEST_DATABASE_URL="$env:TEST_DATABASE_URL" `
  -e TEST_RABBITMQ_URL="$env:TEST_RABBITMQ_URL" `
  -e TEST_REDIS_ADDR="$env:TEST_REDIS_ADDR" `
  @netArgs `
  golang:1.25 go @args

exit $LASTEXITCODE
