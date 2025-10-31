# Multi-stage Dockerfile for Go services

# Build stage
FROM golang:1.24-alpine as builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the applications
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ingest ./cmd/ingest
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o strategy_runner ./cmd/strategy_runner
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o strategy_screen ./cmd/strategy_screen

# Production stage
FROM alpine:3.18 as production

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata curl

# Create non-root user
RUN addgroup -g 1001 -S backtest && \
    adduser -u 1001 -S backtest -G backtest

# Set working directory
WORKDIR /app

# Copy binaries from builder stage
COPY --from=builder /app/main /app/main
COPY --from=builder /app/ingest /app/ingest
COPY --from=builder /app/strategy_runner /app/strategy_runner
COPY --from=builder /app/strategy_screen /app/strategy_screen

# Create data directory
RUN mkdir -p /app/data && chown -R backtest:backtest /app

# Switch to non-root user
USER backtest

# Expose ports
EXPOSE 8080 9091

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the application
CMD ["./main"]

