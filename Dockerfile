# === Stage 1: Build ===
FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/mediaservice ./cmd/mediaservice

# === Stage 2: Final ===
FROM alpine:3.19

RUN apk add --no-cache ffmpeg

WORKDIR /app

COPY --from=builder /app/mediaservice /usr/local/bin/mediaservice

COPY --from=builder /app/migrations ./migrations

# gRPC-порт
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/mediaservice"]