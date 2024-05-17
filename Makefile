VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-azure-storage.version=${VERSION}'" -o conduit-connector-azure-storage cmd/connector/main.go

.PHONY: test
test:
	docker compose -f test/docker-compose.yml -p tests up --quiet-pull -d --wait
	go test --tags=unit,integration $(GOTEST_FLAGS) -race ./...; ret=$$?; \
	  	docker compose -f test/docker-compose.yml -p tests down; \
	  	if [ $$ret -ne 0 ]; then exit $$ret; fi

.PHONY: lint
lint:
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
