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
# Install Go locally and this script becomes unnecessary — `go` works directly.

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot

docker run --rm `
  -v "${repo}:/src" `
  -v "gotask-modcache:/go/pkg/mod" `
  -v "gotask-buildcache:/root/.cache/go-build" `
  -w /src `
  -e GOFLAGS=-buildvcs=false `
  golang:1.25 go @args

exit $LASTEXITCODE
