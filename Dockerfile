FROM golang:1.25-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo docker)" \
    -o /apex ./cmd/apex

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /apex /apex

EXPOSE 8080 9090 9091

ENTRYPOINT ["/apex", "start"]
