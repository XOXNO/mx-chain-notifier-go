FROM golang:1.23.6 AS builder

MAINTAINER MultiversX

WORKDIR /multiversx

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /multiversx/cmd/notifier

RUN go mod tidy
RUN go build -o notifier

# ===== SECOND STAGE ======
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y openssl ca-certificates
COPY --from=builder /multiversx/cmd/notifier /multiversx

EXPOSE 8080 22111 80 443 5000

WORKDIR /multiversx

CMD ["./notifier", "--publisher-type", "servicebus"]
