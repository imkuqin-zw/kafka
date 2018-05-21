FROM golang:latest

WORKDIR $GOPATH/src/github.com/imkuqin-zw/kafka
COPY example example/
COPY kafka.go kafka.go
COPY glide.lock glide.lock
COPY glide.yaml glide.yaml
copy vendor vendor/

RUN cd example \
    && go build producer.go && cp producer $GOPATH/bin \
    && go build consumer.go && cp consumer $GOPATH/bin


