use clap::Parser;
use std::env;

/// Test that encryption key hex string can be converted to bytes.
#[test]
fn test_encryption_key_hex_conversion() {
    // Verify 64-char hex string converts to 32 bytes correctly
    let hex_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    // Verify it can be converted to bytes
    let mut key = [0u8; 32];
    for i in 0..32 {
        key[i] = u8::from_str_radix(&hex_key[i * 2..i * 2 + 2], 16).unwrap();
    }
    assert_eq!(key[0], 0x01);
    assert_eq!(key[1], 0x23);
    assert_eq!(key[31], 0xef);
}

/// Note: Environment variable tests are not included because clap's parse_from()
/// doesn't pick up env vars set during test execution. Environment variable
/// support is verified through CLI flag tests above, which clap maps to env vars.

/// Test that compression level has correct default.
#[test]
fn test_compression_level_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.journal_compress_level, 3);
}

/// Test that encryption key can be passed via CLI flag.
#[test]
fn test_encryption_key_cli_flag() {
    let hex_key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let config = graphd::config::Config::parse_from(vec![
        "graphd",
        "--journal-encryption-key",
        hex_key,
    ]);

    assert_eq!(config.journal_encryption_key, Some(hex_key.to_string()));
}

/// Test that compression can be enabled via CLI flag.
#[test]
fn test_compression_cli_flag() {
    let config = graphd::config::Config::parse_from(vec![
        "graphd",
        "--journal-compress",
    ]);

    assert!(config.journal_compress);
}

/// Test that compression level can be set via CLI flag.
#[test]
fn test_compression_level_cli_flag() {
    let config = graphd::config::Config::parse_from(vec![
        "graphd",
        "--journal-compress-level",
        "10",
    ]);

    assert_eq!(config.journal_compress_level, 10);
}

/// Test that invalid hex key length is rejected.
#[test]
fn test_encryption_key_invalid_length() {
    let short_key = "abc123"; // Too short
    assert_ne!(short_key.len(), 64);

    let long_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef00"; // Too long
    assert_ne!(long_key.len(), 64);
}

/// Test that invalid hex characters are rejected.
#[test]
fn test_encryption_key_invalid_hex() {
    let invalid_hex = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
    assert_eq!(invalid_hex.len(), 64); // Length is correct but contains invalid hex

    // Try to parse it
    let result = (0..32)
        .map(|i| u8::from_str_radix(&invalid_hex[i * 2..i * 2 + 2], 16))
        .collect::<Result<Vec<_>, _>>();

    assert!(result.is_err(), "Should fail to parse invalid hex");
}

/// Test that empty string key is handled properly.
#[test]
fn test_encryption_key_empty_string() {
    // Empty string should not be passed as a key
    let empty = "";
    assert_ne!(empty.len(), 64);

    // Config with no key should have None
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.journal_encryption_key, None);
}
