
.PHONY: install test

install:
	go install github.com/itchio/zipserver

test:
	go test -v github.com/itchio/zipserver/zipserver
