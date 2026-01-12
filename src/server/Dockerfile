FROM golang:1.25.5 as builder
WORKDIR /server
ADD . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build

FROM scratch
COPY --from=builder /server/server .
ENTRYPOINT ["./server"]
