FROM golang:1.17-alpine

WORKDIR /app

COPY ./app/go.mod ./
COPY ./app/go.sum ./
RUN go mod download

COPY ./app* ./

RUN go build ./main/server.go

EXPOSE 8080
EXPOSE 8082
EXPOSE 8081

CMD [ "./server" ]