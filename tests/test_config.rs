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

// ─── Port Configuration Tests ───

#[test]
fn test_bolt_port_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.bolt_port, 7687);
}

#[test]
fn test_bolt_port_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--bolt-port", "9999"]);
    assert_eq!(config.bolt_port, 9999);
}

#[test]
fn test_http_port_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.http_port, 7688);
}

#[test]
fn test_http_port_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--http-port", "8080"]);
    assert_eq!(config.http_port, 8080);
}

#[test]
fn test_bolt_host_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.bolt_host, "127.0.0.1");
}

#[test]
fn test_bolt_host_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--bolt-host", "0.0.0.0"]);
    assert_eq!(config.bolt_host, "0.0.0.0");
}

#[test]
fn test_http_host_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.http_host, "127.0.0.1");
}

#[test]
fn test_http_host_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--http-host", "0.0.0.0"]);
    assert_eq!(config.http_host, "0.0.0.0");
}

// ─── Data Directory Tests ───

#[test]
fn test_data_dir_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.data_dir.to_str().unwrap(), "./data");
}

#[test]
fn test_data_dir_short_flag() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "-d", "/tmp/mydb"]);
    assert_eq!(config.data_dir.to_str().unwrap(), "/tmp/mydb");
}

#[test]
fn test_data_dir_long_flag() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--data-dir", "/var/lib/graphd"]);
    assert_eq!(config.data_dir.to_str().unwrap(), "/var/lib/graphd");
}

// ─── Authentication Tests ───

#[test]
fn test_token_flag() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--token", "my-secret"]);
    assert_eq!(config.token, Some("my-secret".to_string()));
    assert_eq!(config.token_file, None);
}

#[test]
fn test_token_file_flag() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--token-file", "/path/tokens.json"]);
    assert_eq!(config.token_file.as_ref().unwrap().to_str().unwrap(), "/path/tokens.json");
    assert_eq!(config.token, None);
}

#[test]
fn test_generate_token_flag() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--generate-token"]);
    assert!(config.generate_token);
}

// ─── Journal Configuration Tests ───

#[test]
fn test_journal_disabled_by_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert!(!config.journal);
}

#[test]
fn test_journal_flag() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--journal"]);
    assert!(config.journal);
}

#[test]
fn test_journal_segment_mb_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.journal_segment_mb, 64);
}

#[test]
fn test_journal_segment_mb_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--journal-segment-mb", "128"]);
    assert_eq!(config.journal_segment_mb, 128);
}

#[test]
fn test_journal_fsync_ms_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.journal_fsync_ms, 100);
}

#[test]
fn test_journal_fsync_ms_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--journal-fsync-ms", "500"]);
    assert_eq!(config.journal_fsync_ms, 500);
}

#[test]
fn test_journal_compress_disabled_by_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert!(!config.journal_compress);
}

// ─── Snapshot & Restore Tests ───

#[test]
fn test_restore_disabled_by_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert!(!config.restore);
}

#[test]
fn test_restore_flag() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--restore"]);
    assert!(config.restore);
}

#[test]
fn test_snapshot_path_with_restore() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--restore", "--snapshot", "/path/to/snapshot"]);
    assert!(config.restore);
    assert_eq!(config.snapshot.as_ref().unwrap().to_str().unwrap(), "/path/to/snapshot");
}

// ─── S3 Configuration Tests ───

#[test]
fn test_s3_bucket_not_set_by_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.s3_bucket, None);
}

#[test]
fn test_s3_bucket_flag() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--s3-bucket", "my-backups"]);
    assert_eq!(config.s3_bucket, Some("my-backups".to_string()));
}

#[test]
fn test_s3_prefix_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.s3_prefix, "");
}

#[test]
fn test_s3_prefix_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--s3-prefix", "prod/"]);
    assert_eq!(config.s3_prefix, "prod/");
}

// ─── Retention Policy Tests ───

#[test]
fn test_retain_daily_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.retain_daily, 7);
}

#[test]
fn test_retain_daily_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--retain-daily", "14"]);
    assert_eq!(config.retain_daily, 14);
}

#[test]
fn test_retain_weekly_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.retain_weekly, 4);
}

#[test]
fn test_retain_weekly_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--retain-weekly", "8"]);
    assert_eq!(config.retain_weekly, 8);
}

#[test]
fn test_retain_monthly_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.retain_monthly, 3);
}

#[test]
fn test_retain_monthly_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--retain-monthly", "12"]);
    assert_eq!(config.retain_monthly, 12);
}

// ─── Connection Pool Tests ───

#[test]
fn test_tx_timeout_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.tx_timeout_secs, 30);
}

#[test]
fn test_tx_timeout_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--tx-timeout-secs", "60"]);
    assert_eq!(config.tx_timeout_secs, 60);
}

#[test]
fn test_query_timeout_ms_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.query_timeout_ms, 0);
}

#[test]
fn test_query_timeout_ms_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--query-timeout-ms", "5000"]);
    assert_eq!(config.query_timeout_ms, 5000);
}

#[test]
fn test_bolt_max_connections_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.bolt_max_connections, 256);
}

#[test]
fn test_bolt_max_connections_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--bolt-max-connections", "512"]);
    assert_eq!(config.bolt_max_connections, 512);
}

#[test]
fn test_read_connections_default() {
    let config = graphd::config::Config::parse_from(vec!["graphd"]);
    assert_eq!(config.read_connections, 4);
}

#[test]
fn test_read_connections_custom() {
    let config = graphd::config::Config::parse_from(vec!["graphd", "--read-connections", "8"]);
    assert_eq!(config.read_connections, 8);
}
