# Multi-stage build for server or sweeper (TARGET=server|sweeper).
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGET=server
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/${TARGET}

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/app /app
COPY migrations /migrations
EXPOSE 8080 8081
USER nonroot:nonroot
ENTRYPOINT ["/app"]
