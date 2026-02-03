use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;

/// A token entry with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenEntry {
    /// SHA-256 hash of the token (hex-encoded).
    pub hash: String,
    /// Human-readable label for this token.
    #[serde(default)]
    pub label: String,
}

/// Token file format: array of token entries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenFile {
    pub tokens: Vec<TokenEntry>,
}

/// Thread-safe store of hashed tokens.
pub struct TokenStore {
    /// Map from SHA-256 hash (hex) -> label.
    tokens: RwLock<HashMap<String, String>>,
}

impl TokenStore {
    /// Create an empty token store (no auth — all tokens accepted).
    pub fn open() -> Self {
        Self {
            tokens: RwLock::new(HashMap::new()),
        }
    }

    /// Create a token store with a single plaintext token.
    pub fn from_token(plaintext: &str) -> Self {
        let store = Self::open();
        store.add_token(plaintext, "cli");
        store
    }

    /// Load tokens from a JSON file.
    ///
    /// File format:
    /// ```json
    /// {
    ///   "tokens": [
    ///     { "hash": "sha256hex...", "label": "my-app" }
    ///   ]
    /// }
    /// ```
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let contents =
            std::fs::read_to_string(path).map_err(|e| format!("Failed to read token file: {e}"))?;
        let file: TokenFile =
            serde_json::from_str(&contents).map_err(|e| format!("Invalid token file: {e}"))?;

        let mut tokens = HashMap::new();
        for entry in file.tokens {
            tokens.insert(entry.hash, entry.label);
        }

        Ok(Self {
            tokens: RwLock::new(tokens),
        })
    }

    /// Returns true if the store has no tokens (open access).
    pub fn is_empty(&self) -> bool {
        self.tokens.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    /// Hash a plaintext token and add it to the store.
    pub fn add_token(&self, plaintext: &str, label: &str) {
        let hash = hash_token(plaintext);
        self.tokens
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(hash, label.to_string());
    }

    /// Validate a plaintext token. Returns the label if valid.
    /// If the store is empty, all tokens are accepted (returns "open").
    pub fn validate(&self, plaintext: &str) -> Result<String, ()> {
        let tokens = self.tokens.read().unwrap_or_else(|e| e.into_inner());
        if tokens.is_empty() {
            return Ok("open".to_string());
        }
        let hash = hash_token(plaintext);
        tokens.get(&hash).cloned().ok_or(())
    }
}

/// SHA-256 hash a plaintext token, returning hex-encoded string.
pub fn hash_token(plaintext: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(plaintext.as_bytes());
    hex::encode(hasher.finalize())
}

/// Generate a random token: "graphd_" + 32 hex chars (128 bits).
pub fn generate_token() -> String {
    use rand::Rng;
    let bytes: [u8; 16] = rand::rng().random();
    format!("graphd_{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_deterministic() {
        assert_eq!(hash_token("hello"), hash_token("hello"));
        assert_ne!(hash_token("hello"), hash_token("world"));
    }

    #[test]
    fn test_store_single_token() {
        let store = TokenStore::from_token("secret123");
        assert!(store.validate("secret123").is_ok());
        assert!(store.validate("wrong").is_err());
    }

    #[test]
    fn test_store_empty_allows_all() {
        let store = TokenStore::open();
        assert!(store.validate("anything").is_ok());
        assert!(store.is_empty());
    }

    #[test]
    fn test_store_add_multiple() {
        let store = TokenStore::open();
        store.add_token("token_a", "app-a");
        store.add_token("token_b", "app-b");

        assert_eq!(store.validate("token_a").unwrap(), "app-a");
        assert_eq!(store.validate("token_b").unwrap(), "app-b");
        assert!(store.validate("token_c").is_err());
    }

    #[test]
    fn test_generate_token_format() {
        let token = generate_token();
        assert!(token.starts_with("graphd_"));
        assert_eq!(token.len(), 7 + 32); // "graphd_" + 32 hex chars
    }

    #[test]
    fn test_load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");

        let hash = hash_token("my-secret");
        let contents = serde_json::json!({
            "tokens": [
                { "hash": hash, "label": "test-app" }
            ]
        });
        std::fs::write(&path, contents.to_string()).unwrap();

        let store = TokenStore::from_file(&path).unwrap();
        assert_eq!(store.validate("my-secret").unwrap(), "test-app");
        assert!(store.validate("wrong").is_err());
    }
}
