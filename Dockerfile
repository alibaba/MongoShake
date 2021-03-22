FROM golang:alpine3.12 as golang
RUN apk add bash make git zip tzdata ca-certificates gcc musl-dev
WORKDIR /app
COPY . .
RUN make linux

FROM alpine:3.12
# Dependencies
RUN apk --no-cache add tzdata ca-certificates musl
# where application lives
WORKDIR /app
# Copy the products
COPY --from=golang /app/bin .
# metrics
EXPOSE 9100
ENTRYPOINT ["/app/collector"]