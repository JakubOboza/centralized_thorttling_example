FROM golang:1.12-alpine

WORKDIR /app

RUN apk update && apk upgrade && \
    apk add --no-cache git

COPY ./ ./

RUN go build smoothy.go

CMD ["./smoothy"]