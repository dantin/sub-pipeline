# enable BASH-specific features
SHELL := /bin/bash

SOURCE_DIR := $(shell pwd)
DIST_DIR := $(SOURCE_DIR)/bin/dist
GOFILES != find . -name '*.go'

.PHONY: build
build: importer

.PHONY: importer
importer: $(GOFILES) go.mod go.sum
	@echo "Building importer binary..."
	@cd cmd/importer; CGO_ENABLED=0 go build \
		-o $@
	@cd cmd/importer; mv $@ ${SOURCE_DIR}/bin/
