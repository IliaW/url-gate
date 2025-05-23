FROM golang:1.24.1-alpine AS builder

ENV GOOS=linux
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o main .

FROM alpine:3.20.3 AS installer
WORKDIR /app
COPY --from=builder /build/main /app/
COPY --from=builder /build/config.yaml /app/config.yaml
ENTRYPOINT ["/app/main"]
