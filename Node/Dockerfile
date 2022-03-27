FROM golang:1.17-alpine


WORKDIR /

COPY ./* ./
# COPY ./src* ./src
# COPY ./go.sum ./
RUN go mod download

#COPY ./app* ./

RUN go build ./server.go 

EXPOSE 5555

CMD [ "./server" ]