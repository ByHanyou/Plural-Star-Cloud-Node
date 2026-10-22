BIN := plural-star-cloud-node
PKG := ./cmd/node
DIST := dist
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/ByHanyou/Plural-Star-Cloud-Node/internal/version.Version=$(VERSION)

.PHONY: build gencard run test vet tidy clean dist

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $(BIN) $(PKG)

gencard:
	CGO_ENABLED=0 go build -o gencard ./cmd/gencard

run: build
	./$(BIN) --config config.yaml

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -f $(BIN) $(BIN)-* $(BIN).exe gencard
	rm -rf $(DIST)

dist:
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux   GOARCH=amd64        go build -ldflags "$(LDFLAGS)" -o $(DIST)/$(BIN)-linux-amd64   $(PKG)
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm64        go build -ldflags "$(LDFLAGS)" -o $(DIST)/$(BIN)-linux-arm64   $(PKG)
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm GOARM=7  go build -ldflags "$(LDFLAGS)" -o $(DIST)/$(BIN)-linux-armv7   $(PKG)
	CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64        go build -ldflags "$(LDFLAGS)" -o $(DIST)/$(BIN)-macos-arm64   $(PKG)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64        go build -ldflags "$(LDFLAGS)" -o $(DIST)/$(BIN).exe           $(PKG)
