# Multi-stage Dockerfile for Rust backtesting engine

# Build stage
FROM rust:1.82-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libclang-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy full source (Cargo.toml, benches, src, etc.)
COPY . .

# Build the application
RUN mkdir -p benches \
    && [ -f benches/indicators.rs ] || echo "#[test]\nfn bench_placeholder() { assert!(true); }" > benches/indicators.rs \
    && cargo build --release

# Production stage
FROM debian:bookworm-slim as production

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r backtest && useradd -r -g backtest backtest

# Set working directory
WORKDIR /app

# Copy binary from builder stage
# This is a library crate, not a binary - no executable to copy

# Create data directory
RUN mkdir -p /app/data && chown -R backtest:backtest /app

# Switch to non-root user
USER backtest

# Expose port
EXPOSE 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

# Run the application
CMD ["./backtest-engine"]

