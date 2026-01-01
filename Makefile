
.PHONY: build install test

VERSION ?= $(shell git describe --tags --exact-match 2>/dev/null || echo "dev")
COMMIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

LDFLAGS := -X main.Version=$(VERSION) \
           -X main.CommitSHA=$(COMMIT_SHA) \
           -X main.BuildTime=$(BUILD_TIME)

build:
	go build -ldflags "$(LDFLAGS)" -o bin/zipserver

install:
	go install -ldflags "$(LDFLAGS)" .

test:
	go test -v ./zipserver
