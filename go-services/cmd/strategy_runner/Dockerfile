# Strategy Runner Dockerfile
FROM golang:1.21-alpine AS builder

# Install dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the strategy runner
RUN cd cmd/strategy_runner && go build -o strategy_runner .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create app directory
WORKDIR /app

# Copy the binary
COPY --from=builder /app/cmd/strategy_runner/strategy_runner .

# Set executable permissions
RUN chmod +x strategy_runner

# Default command
ENTRYPOINT ["./strategy_runner"]



