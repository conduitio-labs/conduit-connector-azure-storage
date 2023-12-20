.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-azure-storage.version=${VERSION}'" -o conduit-connector-azure-storage cmd/connector/main.go

test:
	docker compose -f test/docker-compose.yml -p tests up --quiet-pull -d --wait
	go test --tags=unit,integration $(GOTEST_FLAGS) -race ./...; ret=$$?; \
	  	docker compose -f test/docker-compose.yml -p tests down; \
	  	if [ $$ret -ne 0 ]; then exit $$ret; fi

lint:
	golangci-lint run

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy
