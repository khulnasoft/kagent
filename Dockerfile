FROM golang:1.23.5-alpine AS builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o kagent github.com/khulnasoft/kagent/cmd/kagent

FROM alpine:3.21.2

COPY --from=builder /app/kagent /app/

ENTRYPOINT ["/app/kagent"]
