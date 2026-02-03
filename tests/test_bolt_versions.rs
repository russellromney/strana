/// Comprehensive tests for Bolt protocol version-specific features.
///
/// These tests verify that version-specific features are correctly implemented
/// for each supported Bolt protocol version (4.4, 5.0-5.7).

use graphd::bolt::{add_connection_hints, failure_message_versioned, BoltVersion};
use bolt_proto::Value;
use std::collections::HashMap;

// ── Connection Hints Tests ───

#[test]
fn test_no_hints_for_bolt_4_4() {
    let mut metadata = HashMap::new();
    add_connection_hints(BoltVersion::V4_4, &mut metadata);

    // Bolt 4.4 should not have any hints
    assert!(!metadata.contains_key("hints"));
}

#[test]
fn test_no_hints_for_bolt_5_0_to_5_3() {
    for minor in 0..=3 {
        let mut metadata = HashMap::new();
        add_connection_hints(BoltVersion::V5(minor), &mut metadata);

        // Bolt 5.0-5.3 should not have any hints
        assert!(
            !metadata.contains_key("hints"),
            "Bolt 5.{} should not have hints",
            minor
        );
    }
}

#[test]
fn test_telemetry_hint_for_bolt_5_4_plus() {
    for minor in 4..=7 {
        let mut metadata = HashMap::new();
        add_connection_hints(BoltVersion::V5(minor), &mut metadata);

        // Bolt 5.4+ should have telemetry.enabled hint
        assert!(
            metadata.contains_key("hints"),
            "Bolt 5.{} should have hints",
            minor
        );

        if let Value::Map(hints) = metadata.get("hints").unwrap() {
            assert_eq!(
                hints.get("telemetry.enabled"),
                Some(&Value::from(true)),
                "Bolt 5.{} should have telemetry.enabled=true",
                minor
            );
        } else {
            panic!("hints should be a map");
        }
    }
}

// ── Error Format Tests ───

#[test]
fn test_legacy_error_format_bolt_4_4() {
    let chunks = failure_message_versioned(
        BoltVersion::V4_4,
        "Neo.ClientError.Security.Unauthorized",
        "Invalid credentials"
    );

    // Should have generated chunks
    assert!(!chunks.is_empty());

    // Parse the FAILURE message to verify format
    // (This is a simplified test - in practice you'd deserialize the chunks)
    let bytes = chunks.iter().flat_map(|c| c.iter()).copied().collect::<Vec<_>>();
    let as_string = String::from_utf8_lossy(&bytes);

    // Legacy format should contain "code" field
    assert!(as_string.contains("code"), "Legacy format should have 'code' field");
    assert!(as_string.contains("message"), "Legacy format should have 'message' field");

    // Should NOT contain GQL fields
    assert!(!as_string.contains("neo4j_code"), "Legacy format should not have 'neo4j_code'");
    assert!(!as_string.contains("gql_status"), "Legacy format should not have 'gql_status'");
}

#[test]
fn test_legacy_error_format_bolt_5_0_to_5_6() {
    for minor in 0..=6 {
        let chunks = failure_message_versioned(
            BoltVersion::V5(minor),
            "Neo.ClientError.Security.Unauthorized",
            "Invalid credentials"
        );

        assert!(!chunks.is_empty(), "Bolt 5.{} should generate error chunks", minor);

        let bytes = chunks.iter().flat_map(|c| c.iter()).copied().collect::<Vec<_>>();
        let as_string = String::from_utf8_lossy(&bytes);

        // Bolt 5.0-5.6 uses legacy format
        assert!(
            as_string.contains("code"),
            "Bolt 5.{} should use legacy 'code' field",
            minor
        );
        assert!(
            !as_string.contains("gql_status"),
            "Bolt 5.{} should not have 'gql_status' field",
            minor
        );
    }
}

#[test]
fn test_gql_error_format_bolt_5_7() {
    let chunks = failure_message_versioned(
        BoltVersion::V5(7),
        "Neo.ClientError.Security.Unauthorized",
        "Invalid credentials"
    );

    assert!(!chunks.is_empty());

    let bytes = chunks.iter().flat_map(|c| c.iter()).copied().collect::<Vec<_>>();
    let as_string = String::from_utf8_lossy(&bytes);

    // Bolt 5.7 should use GQL format
    assert!(as_string.contains("neo4j_code"), "Bolt 5.7 should have 'neo4j_code' field");
    assert!(as_string.contains("gql_status"), "Bolt 5.7 should have 'gql_status' field");
    assert!(as_string.contains("description"), "Bolt 5.7 should have 'description' field");
    assert!(as_string.contains("diagnostic_record"), "Bolt 5.7 should have 'diagnostic_record' field");

    // Should NOT contain legacy field
    assert!(!as_string.contains("\"code\""), "Bolt 5.7 should not have legacy 'code' field");
}

#[test]
fn test_gql_status_mapping_security_errors() {
    let chunks = failure_message_versioned(
        BoltVersion::V5(7),
        "Neo.ClientError.Security.Unauthorized",
        "Access denied"
    );

    let bytes = chunks.iter().flat_map(|c| c.iter()).copied().collect::<Vec<_>>();
    let as_string = String::from_utf8_lossy(&bytes);

    // Security errors should map to 42000 (Syntax error or access rule violation)
    assert!(as_string.contains("42000"), "Security errors should map to GQL status 42000");
}

#[test]
fn test_gql_status_mapping_transient_errors() {
    let chunks = failure_message_versioned(
        BoltVersion::V5(7),
        "Neo.TransientError.Transaction.DeadlockDetected",
        "Deadlock detected"
    );

    let bytes = chunks.iter().flat_map(|c| c.iter()).copied().collect::<Vec<_>>();
    let as_string = String::from_utf8_lossy(&bytes);

    // Transient errors should map to 40000 (Transaction rollback)
    assert!(as_string.contains("40000"), "Transient errors should map to GQL status 40000");
}

// ── Version Negotiation Edge Cases ───

#[test]
fn test_version_negotiation_prefers_5_7_over_5_0() {
    // Client proposes [5.7, 5.0, 4.4, 0.0]
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x00, 0x07, 0x05]); // 5.7
    versions[4..8].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]); // 5.0
    versions[8..12].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4

    let version = graphd::bolt::negotiate_bolt_version(&versions);
    assert_eq!(version, Some(BoltVersion::V5(7)), "Should prefer 5.7");
}

#[test]
fn test_version_negotiation_with_range_spans_multiple_versions() {
    // Client proposes 5.7 with range 7 (accepts 5.7 down to 5.0)
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x07, 0x07, 0x05]); // 5.7 with range 7

    let version = graphd::bolt::negotiate_bolt_version(&versions);
    assert_eq!(version, Some(BoltVersion::V5(7)), "Should match 5.7 from range");
}

#[test]
fn test_version_negotiation_range_excludes_supported_version() {
    // Client proposes 5.3 with range 0 (only accepts 5.3)
    // We support 5.0-5.7, so 5.3 should match
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x00, 0x03, 0x05]); // 5.3 no range

    let version = graphd::bolt::negotiate_bolt_version(&versions);
    assert_eq!(version, Some(BoltVersion::V5(3)), "Should match 5.3");
}

#[test]
fn test_version_negotiation_skips_bolt_5_5() {
    // Note: Bolt 5.5 was intentionally skipped in the spec
    // If client somehow proposes 5.5, we should still accept it (we support 5.0-5.7)
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x00, 0x05, 0x05]); // 5.5

    let version = graphd::bolt::negotiate_bolt_version(&versions);
    // We technically support it even though Neo4j skipped it
    assert_eq!(version, Some(BoltVersion::V5(5)), "Should accept 5.5 (we support full range)");
}

// ── Version Limiting Tests (for testing) ───

use graphd::bolt::negotiate_bolt_version_with_limit;

#[test]
fn test_version_limit_to_4_4() {
    // Client proposes [5.7, 5.0, 4.4, 0.0]
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x00, 0x07, 0x05]); // 5.7
    versions[4..8].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]); // 5.0
    versions[8..12].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4

    let version = negotiate_bolt_version_with_limit(&versions, Some(BoltVersion::V4_4));
    assert_eq!(version, Some(BoltVersion::V4_4), "Should limit to 4.4");
}

#[test]
fn test_version_limit_to_5_0() {
    // Client proposes [5.7, 5.0, 4.4, 0.0]
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x00, 0x07, 0x05]); // 5.7
    versions[4..8].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]); // 5.0
    versions[8..12].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4

    let version = negotiate_bolt_version_with_limit(&versions, Some(BoltVersion::V5(0)));
    assert_eq!(version, Some(BoltVersion::V5(0)), "Should limit to 5.0");
}

#[test]
fn test_version_limit_to_5_4() {
    // Client proposes [5.7, 5.4, 5.0, 4.4]
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x00, 0x07, 0x05]); // 5.7
    versions[4..8].copy_from_slice(&[0x00, 0x00, 0x04, 0x05]); // 5.4
    versions[8..12].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]); // 5.0
    versions[12..16].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]); // 4.4

    let version = negotiate_bolt_version_with_limit(&versions, Some(BoltVersion::V5(4)));
    assert_eq!(version, Some(BoltVersion::V5(4)), "Should limit to 5.4");
}

#[test]
fn test_version_limit_with_range() {
    // Client proposes 5.7 with range 7 (accepts 5.7 down to 5.0)
    // Limit to 5.2 - should match 5.2
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x07, 0x07, 0x05]); // 5.7 with range 7

    let version = negotiate_bolt_version_with_limit(&versions, Some(BoltVersion::V5(2)));
    assert_eq!(version, Some(BoltVersion::V5(2)), "Should limit range to 5.2");
}

#[test]
fn test_version_limit_below_client_minimum() {
    // Client only proposes 5.7 (no range)
    // Limit to 5.4 - should fail (client doesn't accept 5.4)
    let mut versions = [0u8; 16];
    versions[0..4].copy_from_slice(&[0x00, 0x00, 0x07, 0x05]); // 5.7 no range

    let version = negotiate_bolt_version_with_limit(&versions, Some(BoltVersion::V5(4)));
    assert_eq!(version, None, "Should fail when limit is below client minimum");
}
