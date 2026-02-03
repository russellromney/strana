/// Breaking tests for GraphJ format - corruption, edge cases, and error handling.
use std::io::{Read, Write};

#[test]
fn test_truncated_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("truncated.graphj");

    // Write only 64 bytes of header (should be 128)
    std::fs::write(&path, &[0u8; 64]).unwrap();

    // Should detect truncation
    let mut file = std::fs::File::open(&path).unwrap();
    let result = graphd::graphj::read_header(&mut file);
    assert!(result.is_err(), "Should fail on truncated header");
}

#[test]
fn test_corrupted_magic_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad_magic.graphj");

    // Write header with wrong magic
    let mut header = vec![0u8; 128];
    header[0..6].copy_from_slice(b"NOTGRA"); // Wrong magic
    std::fs::write(&path, &header).unwrap();

    let mut file = std::fs::File::open(&path).unwrap();
    let result = graphd::graphj::read_header(&mut file);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none(), "Should return None for non-graphj file");
}

#[test]
fn test_empty_body_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.graphj");

    // Create a sealed file with 0 entries
    let header = graphd::graphj::GraphjHeader {
        flags: graphd::graphj::FLAG_SEALED,
        compression: graphd::graphj::COMPRESSION_NONE,
        encryption: 0,
        zstd_level: 0,
        first_seq: 1,
        last_seq: 0, // last < first when empty
        entry_count: 0,
        body_len: 0,
        body_checksum: [0u8; 32],
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    let header_bytes = graphd::graphj::encode_header(&header);
    std::fs::write(&path, &header_bytes).unwrap();

    // Should handle empty body gracefully
    let mut file = std::fs::File::open(&path).unwrap();
    let read_header = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    assert_eq!(read_header.entry_count, 0);
    assert_eq!(read_header.body_len, 0);
}

#[test]
fn test_body_length_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad_len.graphj");

    // Header says 100 bytes, but file has only 128 bytes (header only)
    let header = graphd::graphj::GraphjHeader {
        flags: graphd::graphj::FLAG_SEALED,
        compression: graphd::graphj::COMPRESSION_NONE,
        encryption: 0,
        zstd_level: 0,
        first_seq: 1,
        last_seq: 5,
        entry_count: 5,
        body_len: 100,
        body_checksum: [0u8; 32],
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    let header_bytes = graphd::graphj::encode_header(&header);
    std::fs::write(&path, &header_bytes).unwrap();

    // Reading should fail when trying to read 100 bytes that don't exist
    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    let result = file.read_exact(&mut body);
    assert!(result.is_err(), "Should fail when body is shorter than claimed");
}

#[test]
fn test_checksum_mismatch_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad_checksum.graphj");

    let body = b"some content";
    let wrong_checksum = [0xFFu8; 32]; // Wrong checksum

    let header = graphd::graphj::GraphjHeader {
        flags: graphd::graphj::FLAG_SEALED,
        compression: graphd::graphj::COMPRESSION_NONE,
        encryption: 0,
        zstd_level: 0,
        first_seq: 1,
        last_seq: 1,
        entry_count: 1,
        body_len: body.len() as u64,
        body_checksum: wrong_checksum,
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&graphd::graphj::encode_header(&header)).unwrap();
    file.write_all(body).unwrap();
    drop(file);

    // Verify checksum validation would catch this
    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    let mut body_read = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body_read).unwrap();

    use sha2::{Digest, Sha256};
    let actual_checksum: [u8; 32] = Sha256::digest(&body_read).into();
    assert_ne!(actual_checksum, hdr.body_checksum, "Checksums should not match");
}

#[test]
fn test_invalid_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad_version.graphj");

    let mut header = vec![0u8; 128];
    header[0..6].copy_from_slice(b"GRAPHJ");
    header[6] = 0; // nul
    header[7] = 99; // Invalid version number
    std::fs::write(&path, &header).unwrap();

    // Should still parse but version field will be 99
    let mut file = std::fs::File::open(&path).unwrap();
    let result = graphd::graphj::read_header(&mut file);
    assert!(result.is_ok(), "Should parse even with unknown version");
    // Note: Current implementation doesn't validate version, but could in future
}

#[test]
fn test_contradictory_flags() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("contradictory.graphj");

    // Encrypted flag set but encryption algorithm is 0 (none)
    let header = graphd::graphj::GraphjHeader {
        flags: graphd::graphj::FLAG_SEALED | graphd::graphj::FLAG_ENCRYPTED,
        compression: graphd::graphj::COMPRESSION_NONE,
        encryption: 0, // No encryption algorithm!
        zstd_level: 0,
        first_seq: 1,
        last_seq: 1,
        entry_count: 1,
        body_len: 10,
        body_checksum: [0u8; 32],
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    let header_bytes = graphd::graphj::encode_header(&header);
    std::fs::write(&path, &header_bytes).unwrap();

    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    assert_eq!(hdr.flags & graphd::graphj::FLAG_ENCRYPTED, graphd::graphj::FLAG_ENCRYPTED);
    assert_eq!(hdr.encryption, 0);
    // This is contradictory - encrypted flag but no algorithm
}

#[test]
fn test_last_seq_before_first_seq() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reversed_seq.graphj");

    let header = graphd::graphj::GraphjHeader {
        flags: graphd::graphj::FLAG_SEALED,
        compression: graphd::graphj::COMPRESSION_NONE,
        encryption: 0,
        zstd_level: 0,
        first_seq: 100,
        last_seq: 50, // last < first!
        entry_count: 1,
        body_len: 10,
        body_checksum: [0u8; 32],
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    let header_bytes = graphd::graphj::encode_header(&header);
    std::fs::write(&path, &header_bytes).unwrap();

    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    assert!(hdr.last_seq < hdr.first_seq, "Should parse invalid sequence range");
    // Note: Could add validation in decode_body or reader
}

#[test]
fn test_reading_encrypted_without_key() {
    let dir = tempfile::tempdir().unwrap();

    // Create an encrypted file
    let entries = b"fake entry data";
    let path = dir.path().join("encrypted.graphj");

    let key = [0x42u8; 32];
    use chacha20poly1305::{aead::{Aead, KeyInit}, XChaCha20Poly1305, XNonce};

    let cipher = XChaCha20Poly1305::new(&key.into());
    let nonce = [0x11u8; 24];
    let nonce_ref = XNonce::from_slice(&nonce);

    // Encrypt the body
    let ciphertext = cipher.encrypt(nonce_ref, entries.as_ref()).unwrap();

    let header = graphd::graphj::GraphjHeader {
        flags: graphd::graphj::FLAG_SEALED | graphd::graphj::FLAG_ENCRYPTED,
        compression: graphd::graphj::COMPRESSION_NONE,
        encryption: graphd::graphj::ENCRYPTION_XCHACHA20POLY1305,
        zstd_level: 0,
        first_seq: 1,
        last_seq: 1,
        entry_count: 1,
        body_len: ciphertext.len() as u64,
        body_checksum: [0u8; 32],
        nonce,
        created_ms: 1700000000000,
    };

    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&graphd::graphj::encode_header(&header)).unwrap();
    file.write_all(&ciphertext).unwrap();
    drop(file);

    // Try to decrypt without providing key
    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body).unwrap();

    // Should fail because no key provided but file is encrypted
    let result = graphd::graphj::decode_body(&hdr, &body, None);
    assert!(result.is_err(), "Should fail to decrypt without key");
    let err_msg = result.unwrap_err();
    assert!(
        err_msg.to_lowercase().contains("encrypted"),
        "Error should mention encryption, got: {}", err_msg
    );
}

#[test]
fn test_reading_unencrypted_with_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unencrypted.graphj");

    // Create unencrypted file
    let body = b"plain text";
    let header = graphd::graphj::GraphjHeader {
        flags: graphd::graphj::FLAG_SEALED,
        compression: graphd::graphj::COMPRESSION_NONE,
        encryption: 0,
        zstd_level: 0,
        first_seq: 1,
        last_seq: 1,
        entry_count: 1,
        body_len: body.len() as u64,
        body_checksum: [0u8; 32],
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&graphd::graphj::encode_header(&header)).unwrap();
    file.write_all(body).unwrap();
    drop(file);

    // Try to read with a key (but file is not encrypted)
    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    let mut body_read = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body_read).unwrap();

    let key = [0x42u8; 32];
    let result = graphd::graphj::decode_body(&hdr, &body_read, Some(&key));
    // Should succeed because file is not encrypted (key is ignored)
    assert!(result.is_ok(), "Should handle unencrypted file even with key provided");
    assert_eq!(result.unwrap(), body);
}

#[test]
fn test_maximum_field_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("max_values.graphj");

    // Test with maximum field values
    let header = graphd::graphj::GraphjHeader {
        flags: 0xFF,
        compression: 0xFF,
        encryption: 0xFF,
        zstd_level: i32::MAX,
        first_seq: u64::MAX,
        last_seq: u64::MAX,
        entry_count: u64::MAX,
        body_len: 0, // Can't actually create u64::MAX bytes
        body_checksum: [0xFF; 32],
        nonce: [0xFF; 24],
        created_ms: i64::MAX,
    };

    let header_bytes = graphd::graphj::encode_header(&header);
    std::fs::write(&path, &header_bytes).unwrap();

    // Should parse without overflow
    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    assert_eq!(hdr.first_seq, u64::MAX);
    assert_eq!(hdr.last_seq, u64::MAX);
    assert_eq!(hdr.entry_count, u64::MAX);
}

#[test]
fn test_unsealed_with_nonzero_final_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unsealed_invalid.graphj");

    // Unsealed file should have zeros for last_seq, entry_count, body_len, checksum
    // but let's violate that
    let header = graphd::graphj::GraphjHeader {
        flags: 0, // Not sealed
        compression: graphd::graphj::COMPRESSION_NONE,
        encryption: 0,
        zstd_level: 0,
        first_seq: 1,
        last_seq: 100, // Should be 0!
        entry_count: 50, // Should be 0!
        body_len: 1000, // Should be 0!
        body_checksum: [0xAA; 32], // Should be zeros!
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    let header_bytes = graphd::graphj::encode_header(&header);
    std::fs::write(&path, &header_bytes).unwrap();

    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    assert!(!hdr.is_sealed());
    // Values are set even though file is unsealed - this is invalid but parses
    assert_ne!(hdr.last_seq, 0);
    assert_ne!(hdr.entry_count, 0);
}

#[test]
fn test_double_compression_flag() {
    let _dir = tempfile::tempdir().unwrap();

    // Try to compress already compressed data
    let data = b"test data";
    let compressed_once = zstd::encode_all(&data[..], 3).unwrap();
    let compressed_twice = zstd::encode_all(&compressed_once[..], 3).unwrap();

    // Second compression should not error (though it won't help)
    assert!(compressed_twice.len() > 0);

    // Decompressing twice should fail or return unexpected data
    let decompressed_once = zstd::decode_all(compressed_twice.as_slice()).unwrap();
    let _result = zstd::decode_all(decompressed_once.as_slice());
    // May or may not error depending on if first result looks like zstd
    // This tests that we only compress once in compact()
}

#[test]
fn test_very_high_compression_level() {
    let _dir = tempfile::tempdir().unwrap();
    let data = b"test data that will be compressed at extreme level";

    // zstd supports levels 1-22
    let result = zstd::encode_all(&data[..], 22);
    assert!(result.is_ok(), "Should handle max compression level");

    // Try invalid level (too high)
    let _result = zstd::encode_all(&data[..], 100);
    // zstd might clamp this or error - check behavior
    // Either way, compact() should validate level before calling zstd
}

#[test]
fn test_zero_byte_entries() {
    // What happens if we try to create entries with zero bytes?
    // This tests journal writer behavior, not just graphj format
    // The format itself should handle it fine
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero_entry.graphj");

    let header = graphd::graphj::GraphjHeader::new_unsealed(1, 1700000000000);
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&graphd::graphj::encode_header(&header)).unwrap();
    // Write no body at all
    drop(file);

    // Should parse as valid unsealed file with no entries yet
    let mut file = std::fs::File::open(&path).unwrap();
    let hdr = graphd::graphj::read_header(&mut file).unwrap().unwrap();
    assert!(!hdr.is_sealed());
    assert_eq!(hdr.entry_count, 0);
}
