# Plural Star Cloud Node — build helpers
BIN := plural-star-cloud-node
PKG := ./cmd/node
DIST := dist

.PHONY: build run test vet tidy clean dist

build:
	CGO_ENABLED=0 go build -o $(BIN) $(PKG)

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
	rm -f $(BIN) $(BIN)-* $(BIN).exe
	rm -rf $(DIST)

# Cross-compile all release targets (fully static, CGO disabled).
dist:
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux   GOARCH=amd64        go build -o $(DIST)/$(BIN)-linux-amd64   $(PKG)
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm64        go build -o $(DIST)/$(BIN)-linux-arm64   $(PKG)
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm GOARM=7  go build -o $(DIST)/$(BIN)-linux-armv7   $(PKG)
	CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64        go build -o $(DIST)/$(BIN)-macos-arm64   $(PKG)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64        go build -o $(DIST)/$(BIN).exe           $(PKG)
