# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/heartbeat-server ./cmd/heartbeat-server
# Pre-create the data dir owned by the nonroot UID so a Docker-initialized
# named volume (docker-compose `hb-data`) inherits nonroot ownership and the
# SQLite driver can create phonebook.db / offline-queue.db. Distroless has no
# shell to chown at runtime; this is the only window.
RUN mkdir -p /out/var-lib-heartbeat && chown 65532:65532 /out/var-lib-heartbeat

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/heartbeat-server /heartbeat-server
COPY --from=build --chown=65532:65532 /out/var-lib-heartbeat /var/lib/heartbeat
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/heartbeat-server"]
