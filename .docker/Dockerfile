# --- BUILD STAGE ---
FROM golang:1.25-bookworm AS builder

ARG TARGETARCH

# Required for DuckDB stuff
RUN apt-get update && apt-get install -y build-essential

WORKDIR /app
COPY . .

RUN CGO_ENABLED=1 GOOS=linux GOARCH=$TARGETARCH go build -o /app/main .

# --- RUN STAGE ---
FROM debian:bookworm-slim

# Required for DuckDB stuff
RUN apt-get update && apt-get install -y libstdc++6 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/main /app
COPY --from=builder /etc/ssl/certs/ /etc/ssl/certs/

RUN chmod +x /app

ENTRYPOINT ["/app"]