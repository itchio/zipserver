
.PHONY: install test

install:
	go install github.com/leafo/zipserver

test:
	go test -v github.com/leafo/zipserver/zipserver
