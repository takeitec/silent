FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY internal ./internal
COPY cmd ./cmd
RUN go build -o /out/peer ./cmd/peer

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/peer /usr/local/bin/peer
ENTRYPOINT ["/usr/local/bin/peer"]