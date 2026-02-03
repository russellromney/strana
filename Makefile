.PHONY: setup-lbug test build clean help e2e bench

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

e2e: build ## Run end-to-end tests via neo4j Python driver
	cd tests/e2e && pip install -q -r requirements.txt && \
	DYLD_LIBRARY_PATH=$(CURDIR)/lbug-lib \
	GRAPHD_BINARY=$(CURDIR)/target/debug/graphd \
	STRANA_ROOT=$(CURDIR) \
	python3 test_e2e.py -v

clean: ## Clean build artifacts
	~/.cargo/bin/cargo clean

bench: ## Run benchmarks
	DYLD_LIBRARY_PATH=$(CURDIR)/lbug-lib ~/.cargo/bin/cargo bench
