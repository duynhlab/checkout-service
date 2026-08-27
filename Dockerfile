# Build stage
# --platform pins the builder to the BUILD host so a multi-arch build
# cross-compiles instead of running this whole stage under emulation.
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.26.7-alpine AS builder
ARG TARGETOS TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH}" go build -o /app/checkout-service ./cmd/main.go

# Final stage
FROM alpine:latest

RUN apk --no-cache upgrade && apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/checkout-service .

EXPOSE 8080

ENTRYPOINT ["./checkout-service"]
