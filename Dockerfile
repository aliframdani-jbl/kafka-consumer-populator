FROM golang:1.22 as builder

WORKDIR /app

RUN apt-get update && apt-get install -y librdkafka-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o kafka-producer

FROM debian:latest

RUN apt-get update && apt-get install -y librdkafka1

WORKDIR /

COPY --from=builder /app/kafka-producer .

ENTRYPOINT ["/kafka-producer"]
