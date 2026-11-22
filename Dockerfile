FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY api ./api
RUN CGO_ENABLED=0 go build -o /out/geostream-ingest ./cmd/ingest && \
    CGO_ENABLED=0 go build -o /out/geostream-api ./cmd/api

FROM alpine:3.21
WORKDIR /app
COPY --from=builder /out/geostream-ingest /usr/local/bin/geostream-ingest
COPY --from=builder /out/geostream-api /usr/local/bin/geostream-api
COPY fixtures ./fixtures
EXPOSE 8200 8201
ENTRYPOINT ["/usr/local/bin/geostream-ingest"]

