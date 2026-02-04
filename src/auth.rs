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

// ─── Rate limiter ───

use std::net::IpAddr;
use std::time::{Duration, Instant};

struct FailureRecord {
    count: u32,
    last_failure: Instant,
}

/// In-process auth rate limiter with per-IP exponential backoff.
///
/// After `threshold` failed attempts within `window`, introduces an exponential
/// delay before responding to subsequent auth failures. This prevents brute-force
/// credential guessing.
pub struct RateLimiter {
    attempts: RwLock<HashMap<IpAddr, FailureRecord>>,
    /// Failed attempts before backoff kicks in.
    threshold: u32,
    /// Initial backoff delay.
    base_delay: Duration,
    /// Maximum backoff delay.
    max_delay: Duration,
    /// Failure records expire after this duration of inactivity.
    window: Duration,
}

impl RateLimiter {
    pub fn new(threshold: u32, base_delay: Duration, max_delay: Duration, window: Duration) -> Self {
        Self {
            attempts: RwLock::new(HashMap::new()),
            threshold,
            base_delay,
            max_delay,
            window,
        }
    }

    /// Returns the delay to apply before responding to this IP.
    /// Call this BEFORE checking credentials.
    pub fn check_delay(&self, ip: IpAddr) -> Duration {
        let attempts = self.attempts.read().unwrap_or_else(|e| e.into_inner());
        match attempts.get(&ip) {
            Some(record) => {
                if record.last_failure.elapsed() > self.window {
                    return Duration::ZERO;
                }
                if record.count < self.threshold {
                    return Duration::ZERO;
                }
                let exponent = record.count.saturating_sub(self.threshold);
                let delay = self.base_delay.saturating_mul(1u32 << exponent.min(20));
                delay.min(self.max_delay)
            }
            None => Duration::ZERO,
        }
    }

    /// Record a failed auth attempt for an IP.
    pub fn record_failure(&self, ip: IpAddr) {
        let mut attempts = self.attempts.write().unwrap_or_else(|e| e.into_inner());
        let record = attempts.entry(ip).or_insert(FailureRecord {
            count: 0,
            last_failure: Instant::now(),
        });
        // Reset if window expired.
        if record.last_failure.elapsed() > self.window {
            record.count = 0;
        }
        record.count = record.count.saturating_add(1);
        record.last_failure = Instant::now();
    }

    /// Clear failure count on successful auth.
    pub fn record_success(&self, ip: IpAddr) {
        let mut attempts = self.attempts.write().unwrap_or_else(|e| e.into_inner());
        attempts.remove(&ip);
    }

    /// Remove expired entries. Call periodically to bound memory usage.
    pub fn cleanup(&self) {
        let mut attempts = self.attempts.write().unwrap_or_else(|e| e.into_inner());
        attempts.retain(|_, record| record.last_failure.elapsed() <= self.window);
    }
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

    // ─── RateLimiter tests ───

    #[test]
    fn rate_limiter_no_delay_under_threshold() {
        let limiter = RateLimiter::new(
            5,
            Duration::from_millis(100),
            Duration::from_secs(30),
            Duration::from_secs(3600),
        );
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        for _ in 0..4 {
            limiter.record_failure(ip);
        }
        assert_eq!(limiter.check_delay(ip), Duration::ZERO);
    }

    #[test]
    fn rate_limiter_delay_at_threshold() {
        let limiter = RateLimiter::new(
            3,
            Duration::from_millis(100),
            Duration::from_secs(30),
            Duration::from_secs(3600),
        );
        let ip: IpAddr = "10.0.0.2".parse().unwrap();

        for _ in 0..3 {
            limiter.record_failure(ip);
        }
        // At threshold: 2^0 * 100ms = 100ms
        assert_eq!(limiter.check_delay(ip), Duration::from_millis(100));
    }

    #[test]
    fn rate_limiter_exponential_backoff() {
        let limiter = RateLimiter::new(
            2,
            Duration::from_millis(100),
            Duration::from_secs(30),
            Duration::from_secs(3600),
        );
        let ip: IpAddr = "10.0.0.3".parse().unwrap();

        // 2 failures = at threshold: 2^0 * 100ms = 100ms
        limiter.record_failure(ip);
        limiter.record_failure(ip);
        assert_eq!(limiter.check_delay(ip), Duration::from_millis(100));

        // 3rd failure: 2^1 * 100ms = 200ms
        limiter.record_failure(ip);
        assert_eq!(limiter.check_delay(ip), Duration::from_millis(200));

        // 4th failure: 2^2 * 100ms = 400ms
        limiter.record_failure(ip);
        assert_eq!(limiter.check_delay(ip), Duration::from_millis(400));
    }

    #[test]
    fn rate_limiter_caps_at_max() {
        let limiter = RateLimiter::new(
            1,
            Duration::from_millis(100),
            Duration::from_millis(500),
            Duration::from_secs(3600),
        );
        let ip: IpAddr = "10.0.0.4".parse().unwrap();

        for _ in 0..20 {
            limiter.record_failure(ip);
        }
        // Should cap at max_delay (500ms)
        assert_eq!(limiter.check_delay(ip), Duration::from_millis(500));
    }

    #[test]
    fn rate_limiter_success_clears() {
        let limiter = RateLimiter::new(
            2,
            Duration::from_millis(100),
            Duration::from_secs(30),
            Duration::from_secs(3600),
        );
        let ip: IpAddr = "10.0.0.5".parse().unwrap();

        limiter.record_failure(ip);
        limiter.record_failure(ip);
        limiter.record_failure(ip);
        assert!(limiter.check_delay(ip) > Duration::ZERO);

        limiter.record_success(ip);
        assert_eq!(limiter.check_delay(ip), Duration::ZERO);
    }

    #[test]
    fn rate_limiter_separate_ips() {
        let limiter = RateLimiter::new(
            2,
            Duration::from_millis(100),
            Duration::from_secs(30),
            Duration::from_secs(3600),
        );
        let ip_a: IpAddr = "10.0.0.6".parse().unwrap();
        let ip_b: IpAddr = "10.0.0.7".parse().unwrap();

        for _ in 0..5 {
            limiter.record_failure(ip_a);
        }
        // ip_a is throttled, ip_b is not
        assert!(limiter.check_delay(ip_a) > Duration::ZERO);
        assert_eq!(limiter.check_delay(ip_b), Duration::ZERO);
    }

    #[test]
    fn rate_limiter_cleanup_removes_expired() {
        let limiter = RateLimiter::new(
            2,
            Duration::from_millis(100),
            Duration::from_secs(30),
            Duration::from_millis(1), // 1ms window — expires almost immediately
        );
        let ip: IpAddr = "10.0.0.8".parse().unwrap();

        limiter.record_failure(ip);
        limiter.record_failure(ip);
        limiter.record_failure(ip);

        // Wait for window to expire.
        std::thread::sleep(Duration::from_millis(5));

        limiter.cleanup();
        assert_eq!(limiter.check_delay(ip), Duration::ZERO);
    }
}
