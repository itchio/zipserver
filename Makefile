
.PHONY: install test

build:
	go build -o bin/zipserver

install:
	go install github.com/itchio/zipserver

test:
	go test -v github.com/itchio/zipserver/zipserver
