# One image, several entry points. cmd/api, cmd/worker and cmd/relay all ship in
# the same artifact and are selected by the container command, so there is one
# thing to build, scan and promote rather than three that can drift apart.

# --- build ------------------------------------------------------------------
FROM golang:1.25 AS build

WORKDIR /src

# Dependencies first, in their own layer. Source changes are frequent and
# go.mod changes are rare, so this keeps the module download cached across
# almost every rebuild.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a static binary, which is what lets the final stage be
# a distroless image with no libc at all.
# -trimpath strips local filesystem paths out of the binary; -w -s drop DWARF
# and the symbol table, roughly halving the size.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-w -s" -o /out/api     ./cmd/api  && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-w -s" -o /out/worker  ./cmd/worker && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-w -s" -o /out/relay   ./cmd/relay && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-w -s" -o /out/migrate ./cmd/migrate && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-w -s" -o /out/seed    ./cmd/seed

# --- runtime ----------------------------------------------------------------
# Distroless: no shell, no package manager, no busybox. An attacker who achieves
# RCE lands in a container with nothing to pivot with. It also runs as nonroot by
# default rather than needing a USER line to fix a bad default.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=build /out/ /app/
# Migrations are read at runtime by cmd/migrate, so they have to be in the image.
COPY --from=build /src/migrations /app/migrations

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/app/api"]
