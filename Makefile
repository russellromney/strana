.PHONY: setup-kuzu test build clean help

KUZU_VERSION := v0.11.3
KUZU_ARCHIVE := libkuzu-osx-universal.tar.gz

help: ## Show this help
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-15s %s\n", $$1, $$2}'

setup-kuzu: ## Download prebuilt Kuzu shared library
	@mkdir -p kuzu-lib
	@if [ ! -f kuzu-lib/libkuzu.dylib ]; then \
		echo "Downloading Kuzu $(KUZU_VERSION) prebuilt library..."; \
		gh release download $(KUZU_VERSION) --repo kuzudb/kuzu --pattern '$(KUZU_ARCHIVE)' --dir /tmp --clobber; \
		tar -xzf /tmp/$(KUZU_ARCHIVE) -C kuzu-lib/; \
		echo "Done. kuzu-lib/ ready."; \
	else \
		echo "kuzu-lib/libkuzu.dylib already exists."; \
	fi

test: ## Run all tests
	DYLD_LIBRARY_PATH=$(CURDIR)/kuzu-lib ~/.cargo/bin/cargo test

build: ## Build the project
	~/.cargo/bin/cargo build

clean: ## Clean build artifacts
	~/.cargo/bin/cargo clean
