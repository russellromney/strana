.PHONY: setup-lbug test build clean help e2e e2e-py e2e-python e2e-js e2e-go e2e-rust e2e-all bench

LBUG_VERSION := v0.14.1
LBUG_ARCHIVE := liblbug-osx-universal.tar.gz

help: ## Show this help
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-15s %s\n", $$1, $$2}'

setup-lbug: ## Download prebuilt LadybugDB shared library
	@mkdir -p lbug-lib
	@if [ ! -f lbug-lib/liblbug.dylib ]; then \
		echo "Downloading LadybugDB $(LBUG_VERSION) prebuilt library..."; \
		gh release download $(LBUG_VERSION) --repo LadybugDB/ladybug --pattern '$(LBUG_ARCHIVE)' --dir /tmp --clobber; \
		tar -xzf /tmp/$(LBUG_ARCHIVE) -C lbug-lib/; \
		echo "Done. lbug-lib/ ready."; \
	else \
		echo "lbug-lib/liblbug.dylib already exists."; \
	fi

test: ## Run all tests
	DYLD_LIBRARY_PATH=$(CURDIR)/lbug-lib ~/.cargo/bin/cargo test

build: ## Build the project
	~/.cargo/bin/cargo build

e2e: e2e-py ## Run Python e2e tests (default)

e2e-py: build ## Run end-to-end tests via neo4j Python driver
	cd tests/e2e && pip install -q -r requirements.txt && \
	DYLD_LIBRARY_PATH=$(CURDIR)/lbug-lib \
	GRAPHD_BINARY=$(CURDIR)/target/debug/graphd \
	STRANA_ROOT=$(CURDIR) \
	python3 test_e2e.py -v

e2e-python: build ## Run end-to-end tests via neo4j Python driver (standalone)
	cd tests/e2e && pip install -q neo4j && \
	python3 test_python.py

e2e-js: build ## Run end-to-end tests via neo4j JavaScript driver
	cd tests/e2e && npm install && \
	DYLD_LIBRARY_PATH=$(CURDIR)/lbug-lib \
	GRAPHD_BINARY=$(CURDIR)/target/debug/graphd \
	STRANA_ROOT=$(CURDIR) \
	node test_js.js

e2e-go: build ## Run end-to-end tests via neo4j Go driver
	cd tests/e2e && \
	DYLD_LIBRARY_PATH=$(CURDIR)/lbug-lib \
	GRAPHD_BINARY=$(CURDIR)/target/debug/graphd \
	STRANA_ROOT=$(CURDIR) \
	go run test_go.go

e2e-rust: build ## Run end-to-end tests via neo4j Rust driver
	cd tests/e2e && \
	DYLD_LIBRARY_PATH=$(CURDIR)/lbug-lib \
	GRAPHD_BINARY=$(CURDIR)/target/debug/graphd \
	STRANA_ROOT=$(CURDIR) \
	cargo run --bin test_rust

e2e-all: build ## Run all driver compatibility tests (Python, JavaScript, Go, Rust)
	@echo "Running Python driver tests (integration)..."
	@$(MAKE) e2e-py
	@echo "\nRunning Python driver tests (standalone)..."
	@$(MAKE) e2e-python
	@echo "\nRunning JavaScript driver tests..."
	@$(MAKE) e2e-js
	@echo "\nRunning Go driver tests..."
	@$(MAKE) e2e-go
	@echo "\nRunning Rust driver tests..."
	@$(MAKE) e2e-rust
	@echo "\n✓ All driver compatibility tests completed"

clean: ## Clean build artifacts
	~/.cargo/bin/cargo clean

bench: ## Run benchmarks
	DYLD_LIBRARY_PATH=$(CURDIR)/lbug-lib ~/.cargo/bin/cargo bench
