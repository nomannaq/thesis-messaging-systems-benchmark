FROM golang:tip-alpine

WORKDIR /app

# Set GOTOOLCHAIN to auto to allow using newer Go version
ENV GOTOOLCHAIN=auto

# Alpine needs these for Go's CGO (required for Kafka client)
RUN apk add --no-cache gcc musl-dev

# Copy go.mod (with dependencies) and go.sum
COPY go.mod* go.sum* ./

# Copy the source code
COPY *.go ./
