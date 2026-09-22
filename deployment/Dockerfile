# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM golang:1.24-alpine AS build

WORKDIR /src

# Dependencies are copied and downloaded before the source so that a change to
# application code reuses the cached module layer instead of re-downloading the
# whole graph on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO is disabled so the result is a static binary that runs on scratch.
# -trimpath keeps build machine paths out of the binary, and the ldflags strip
# the symbol table and DWARF data, which cuts the image by several megabytes.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /out/goqueue \
      ./cmd

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

COPY --from=build /out/goqueue /app/goqueue
COPY --from=build /src/config /app/config

# distroless/nonroot runs as uid 65532 with no shell and no package manager,
# which removes most of what an attacker would reach for after a compromise.
USER nonroot:nonroot

# 8080 public API, 9090 private metrics/dashboard.
EXPOSE 8080 9090

ENV GOQUEUE_CONFIG_PATH=/app/config/config.local.yaml

ENTRYPOINT ["/app/goqueue"]
