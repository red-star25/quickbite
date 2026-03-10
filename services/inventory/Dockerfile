# 1) Build stage: compiles the Go library.
FROM golang:1.25 AS builder
# Set the working directory in the container to /app.
WORKDIR /app
# Copy proto and inventory module (build context is repo root).
COPY proto ./proto
# Copy go.mod and go.sum files for inventory module.
COPY services/inventory/go.mod services/inventory/go.sum ./services/inventory/
# Copy services/inventory directory to the container.
COPY services/inventory ./services/inventory
# Set the working directory in the container to /app/services/inventory.
WORKDIR /app/services/inventory
# Downloads the dependencies.
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /inventory ./cmd/inventory

# 2) Run stage: runs image with the compiled binary.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
# Copy the built file from stage 1 (builder) and copy it into this final image. So now the final image contains basically /inventory (your executable)
COPY --from=builder /inventory /inventory
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/inventory"]