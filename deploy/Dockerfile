# syntax=docker/dockerfile:1.7

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/heartbeat-server ./cmd/heartbeat-server

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/heartbeat-server /heartbeat-server
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/heartbeat-server"]
