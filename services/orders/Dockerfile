# 1) Build stage: compiles the Go library.
# Start from the image that has the Go compiler. Name this stage as "builder".
FROM golang:1.25 AS builder
# Set the working directory in the container to /app.
WORKDIR /app
# Copy proto and orders module (build context is repo root).
COPY proto ./proto
COPY services/orders/go.mod services/orders/go.sum ./services/orders/
COPY services/orders ./services/orders
WORKDIR /app/services/orders
# Downloads the dependencies.
RUN go mod download
# Compile your go app into a single executable file.
# GOOS=linux because containers run Linux.
# CGO_ENABLED=0 makes a "static" binary that doesn't need extra system libraries.
RUN CGO_ENABLED=0 GOOS=linux go build -o /orders ./cmd/orders

# 2) Run stage: runs image with the compiled binary.
# gcr.io/distroless/static:nonroot -> minimal Linux image without any additional packages.
# nonroot -> run as non-root user.
FROM gcr.io/distroless/static:nonroot
# Set the working directory in the container to /.
WORKDIR /
# Copt the built file from stage 1 (builder) and copy it into this final image. So now the final image contains basically /orders (your executable)
COPY --from=builder /orders /orders
# run the program as non-root user and not as admin/root for better security. If someone gains access to the container, they won't have root access.
USER nonroot:nonroot
# This is just a label saying: this container will listen on port 8080. You still need -p 8080:8080 to map the container port to the host port.
EXPOSE 8080
# When the container starts, runs "/orders"
ENTRYPOINT ["/orders"]