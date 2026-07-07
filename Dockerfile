# Build stage
FROM docker.io/library/golang:1.26.4-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/checkout-service ./cmd/main.go

# Final stage
FROM alpine:latest

RUN apk --no-cache upgrade && apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/checkout-service .

EXPOSE 8080

ENTRYPOINT ["./checkout-service"]
