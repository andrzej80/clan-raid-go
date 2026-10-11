FROM golang:1.27-alpine AS builder

RUN apk add --no-cache git

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/server ./cmd/api


FROM alpine:3.20 AS production

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/server ./server

EXPOSE 9090

CMD ["./server"]