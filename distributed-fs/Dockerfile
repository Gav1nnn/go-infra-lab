FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/fs .

FROM alpine:3.22

WORKDIR /app
COPY --from=build /out/fs /usr/local/bin/fs
ENTRYPOINT ["fs"]
