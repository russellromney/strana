use super::*;
use tempfile::tempdir;

#[test]
fn test_header_roundtrip() {
    let header = GraphjHeader {
        flags: FLAG_SEALED | FLAG_COMPRESSED,
        compression: COMPRESSION_ZSTD,
        encryption: ENCRYPTION_NONE,
        zstd_level: 3,
        first_seq: 1,
        last_seq: 100,
        entry_count: 50,
        body_len: 4096,
        body_checksum: [0xAB; 32],
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    let encoded = encode_header(&header);
    assert_eq!(encoded.len(), HEADER_SIZE);
    assert_eq!(&encoded[0..6], MAGIC);
    assert_eq!(encoded[6], NUL);
    assert_eq!(encoded[7], VERSION);

    let mut cursor = io::Cursor::new(&encoded[..]);
    let decoded = read_header(&mut cursor).unwrap().unwrap();

    assert_eq!(decoded.flags, header.flags);
    assert_eq!(decoded.compression, header.compression);
    assert_eq!(decoded.encryption, header.encryption);
    assert_eq!(decoded.zstd_level, header.zstd_level);
    assert_eq!(decoded.first_seq, header.first_seq);
    assert_eq!(decoded.last_seq, header.last_seq);
    assert_eq!(decoded.entry_count, header.entry_count);
    assert_eq!(decoded.body_len, header.body_len);
    assert_eq!(decoded.body_checksum, header.body_checksum);
    assert_eq!(decoded.nonce, header.nonce);
    assert_eq!(decoded.created_ms, header.created_ms);
}

#[test]
fn test_magic_detection_legacy() {
    // Simulate a legacy .wal file: starts with CRC32C bytes, not "GRAPHJ".
    let fake_wal = [0x12, 0x34, 0x56, 0x78, 0x00, 0x00, 0x00, 0x01];
    let mut padded = [0u8; HEADER_SIZE];
    padded[..8].copy_from_slice(&fake_wal);

    let mut cursor = io::Cursor::new(&padded[..]);
    let result = read_header(&mut cursor).unwrap();
    assert!(result.is_none(), "Legacy .wal data should not match magic");
}

#[test]
fn test_unsealed_header() {
    let header = GraphjHeader::new_unsealed(42, 1700000000000);
    assert!(!header.is_sealed());
    assert!(!header.is_compressed());
    assert!(!header.is_encrypted());
    assert_eq!(header.first_seq, 42);
    assert_eq!(header.last_seq, 0);
    assert_eq!(header.entry_count, 0);
}

#[test]
fn test_flag_checks() {
    let mut header = GraphjHeader::new_unsealed(1, 0);
    assert!(!header.is_sealed());
    assert!(!header.is_compressed());
    assert!(!header.is_encrypted());

    header.flags = FLAG_SEALED | FLAG_COMPRESSED | FLAG_ENCRYPTED;
    assert!(header.is_sealed());
    assert!(header.is_compressed());
    assert!(header.is_encrypted());
}

/// Helper: create a fake journal entry in the existing binary format.
fn make_raw_entry(seq: u64, prev_hash: &[u8; 32], query: &str) -> Vec<u8> {
    use prost::Message;
    let proto_entry = crate::graphd::JournalEntry {
        sequence: seq,
        query: query.to_string(),
        params: vec![],
        timestamp_ms: 1700000000000,
    };
    let payload = proto_entry.encode_to_vec();

    let total = 48 + payload.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&[0u8; 4]); // CRC32C placeholder
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(prev_hash);
    buf.extend_from_slice(&payload);
    let crc = crc32c::crc32c(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    buf
}

fn make_test_entries(count: u64) -> Vec<u8> {
    let mut entries = Vec::new();
    let mut prev_hash = [0u8; 32];
    for i in 1..=count {
        let entry = make_raw_entry(i, &prev_hash, &format!("CREATE (n:Test{{id: {i}}})"));
        // Compute chain hash for next entry.
        let payload_start = 48;
        let payload = &entry[payload_start..];
        let mut hasher = Sha256::new();
        hasher.update(prev_hash);
        hasher.update(payload);
        prev_hash = hasher.finalize().into();
        entries.extend_from_slice(&entry);
    }
    entries
}

#[test]
fn test_seal_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.graphj");

    // Write an unsealed .graphj file with 3 entries.
    let entries = make_test_entries(3);
    let header = GraphjHeader::new_unsealed(1, 1700000000000);
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&encode_header(&header)).unwrap();
    file.write_all(&entries).unwrap();
    file.sync_all().unwrap();
    drop(file);

    // Seal it.
    let sealed = seal_file(&path).unwrap();
    assert!(sealed.is_sealed());
    assert_eq!(sealed.first_seq, 1);
    assert_eq!(sealed.last_seq, 3);
    assert_eq!(sealed.entry_count, 3);
    assert_eq!(sealed.body_len, entries.len() as u64);

    // Verify checksum.
    let expected_checksum: [u8; 32] = Sha256::digest(&entries).into();
    assert_eq!(sealed.body_checksum, expected_checksum);
}

#[test]
fn test_compact_uncompressed() {
    let dir = tempdir().unwrap();

    // Create two .graphj files with entries.
    let entries1 = make_test_entries(3);
    let path1 = dir.path().join("journal-0000000000000001.graphj");
    let h1 = GraphjHeader::new_unsealed(1, 1700000000000);
    let mut f1 = std::fs::File::create(&path1).unwrap();
    f1.write_all(&encode_header(&h1)).unwrap();
    f1.write_all(&entries1).unwrap();
    drop(f1);

    let path2 = dir.path().join("journal-0000000000000004.graphj");
    let mut prev_hash = [0u8; 32];
    // Compute hash through first 3 entries.
    {
        let mut cursor = io::Cursor::new(&entries1);
        for _ in 0..3 {
            let mut hdr = [0u8; 48];
            cursor.read_exact(&mut hdr).unwrap();
            let plen = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as usize;
            let mut payload = vec![0u8; plen];
            cursor.read_exact(&mut payload).unwrap();
            let mut hasher = Sha256::new();
            hasher.update(&hdr[16..48]); // prev_hash from entry
            hasher.update(&payload);
            prev_hash = hasher.finalize().into();
        }
    }
    let entry4 = make_raw_entry(4, &prev_hash, "CREATE (n:Test{id: 4})");
    let h2 = GraphjHeader::new_unsealed(4, 1700000000000);
    let mut f2 = std::fs::File::create(&path2).unwrap();
    f2.write_all(&encode_header(&h2)).unwrap();
    f2.write_all(&entry4).unwrap();
    drop(f2);

    // Compact.
    let output = dir.path().join("compacted.graphj");
    let result = compact(&[&path1, &path2], &output, false, 0, None).unwrap();

    assert!(result.is_sealed());
    assert!(!result.is_compressed());
    assert!(!result.is_encrypted());
    assert_eq!(result.first_seq, 1);
    assert_eq!(result.last_seq, 4);
    assert_eq!(result.entry_count, 4);
}

#[test]
fn test_compact_compressed() {
    let dir = tempdir().unwrap();

    let entries = make_test_entries(5);
    let path = dir.path().join("input.graphj");
    let h = GraphjHeader::new_unsealed(1, 1700000000000);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&encode_header(&h)).unwrap();
    f.write_all(&entries).unwrap();
    drop(f);

    let output = dir.path().join("compressed.graphj");
    let result = compact(&[&path], &output, true, 3, None).unwrap();

    assert!(result.is_sealed());
    assert!(result.is_compressed());
    assert!(!result.is_encrypted());
    assert_eq!(result.entry_count, 5);

    // Body should be smaller than raw (or at least different).
    assert!(result.body_len > 0);

    // Decode and verify.
    let mut file = std::fs::File::open(&output).unwrap();
    let hdr = read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body).unwrap();
    let decoded = decode_body(&hdr, &body, None).unwrap();
    assert_eq!(decoded, entries);
}

#[test]
fn test_compact_encrypted() {
    let dir = tempdir().unwrap();

    let entries = make_test_entries(3);
    let path = dir.path().join("input.graphj");
    let h = GraphjHeader::new_unsealed(1, 1700000000000);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&encode_header(&h)).unwrap();
    f.write_all(&entries).unwrap();
    drop(f);

    let key = [0x42u8; 32];
    let output = dir.path().join("encrypted.graphj");
    let result = compact(&[&path], &output, false, 0, Some(&key)).unwrap();

    assert!(result.is_sealed());
    assert!(!result.is_compressed());
    assert!(result.is_encrypted());

    // Raw body should not equal entries (it's encrypted).
    let mut file = std::fs::File::open(&output).unwrap();
    let hdr = read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body).unwrap();
    assert_ne!(&body[..entries.len().min(body.len())], &entries[..entries.len().min(body.len())]);

    // Decrypt and verify.
    let decoded = decode_body(&hdr, &body, Some(&key)).unwrap();
    assert_eq!(decoded, entries);
}

#[test]
fn test_compact_compressed_and_encrypted() {
    let dir = tempdir().unwrap();

    let entries = make_test_entries(5);
    let path = dir.path().join("input.graphj");
    let h = GraphjHeader::new_unsealed(1, 1700000000000);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&encode_header(&h)).unwrap();
    f.write_all(&entries).unwrap();
    drop(f);

    let key = [0xAB; 32];
    let output = dir.path().join("both.graphj");
    let result = compact(&[&path], &output, true, 3, Some(&key)).unwrap();

    assert!(result.is_sealed());
    assert!(result.is_compressed());
    assert!(result.is_encrypted());

    // Full roundtrip.
    let mut file = std::fs::File::open(&output).unwrap();
    let hdr = read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body).unwrap();
    let decoded = decode_body(&hdr, &body, Some(&key)).unwrap();
    assert_eq!(decoded, entries);
}

#[test]
fn test_body_checksum_tamper() {
    let dir = tempdir().unwrap();

    let entries = make_test_entries(2);
    let path = dir.path().join("input.graphj");
    let h = GraphjHeader::new_unsealed(1, 1700000000000);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&encode_header(&h)).unwrap();
    f.write_all(&entries).unwrap();
    drop(f);

    let output = dir.path().join("sealed.graphj");
    compact(&[&path], &output, false, 0, None).unwrap();

    // Tamper with body byte.
    let mut data = std::fs::read(&output).unwrap();
    if data.len() > HEADER_SIZE + 10 {
        data[HEADER_SIZE + 10] ^= 0xFF;
    }
    std::fs::write(&output, &data).unwrap();

    // Read and verify checksum fails.
    let mut file = std::fs::File::open(&output).unwrap();
    let hdr = read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body).unwrap();
    let actual_checksum: [u8; 32] = Sha256::digest(&body).into();
    assert_ne!(actual_checksum, hdr.body_checksum, "Tampered body should not match checksum");
}

#[test]
fn test_aad_tamper_encrypted() {
    let dir = tempdir().unwrap();

    let entries = make_test_entries(2);
    let path = dir.path().join("input.graphj");
    let h = GraphjHeader::new_unsealed(1, 1700000000000);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&encode_header(&h)).unwrap();
    f.write_all(&entries).unwrap();
    drop(f);

    let key = [0x99u8; 32];
    let output = dir.path().join("encrypted.graphj");
    compact(&[&path], &output, false, 0, Some(&key)).unwrap();

    // Tamper with a header field in the AAD range (e.g., first_seq at offset 16).
    let mut data = std::fs::read(&output).unwrap();
    data[16] ^= 0xFF;
    std::fs::write(&output, &data).unwrap();

    // Decryption should fail because AAD was tampered.
    let mut file = std::fs::File::open(&output).unwrap();
    let hdr = read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body).unwrap();
    let result = decode_body(&hdr, &body, Some(&key));
    assert!(result.is_err(), "Decryption should fail after AAD tamper");
}

#[test]
fn test_wrong_key_fails() {
    let dir = tempdir().unwrap();

    let entries = make_test_entries(2);
    let path = dir.path().join("input.graphj");
    let h = GraphjHeader::new_unsealed(1, 1700000000000);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&encode_header(&h)).unwrap();
    f.write_all(&entries).unwrap();
    drop(f);

    let key = [0x42u8; 32];
    let output = dir.path().join("encrypted.graphj");
    compact(&[&path], &output, false, 0, Some(&key)).unwrap();

    // Try decrypting with wrong key.
    let wrong_key = [0x43u8; 32];
    let mut file = std::fs::File::open(&output).unwrap();
    let hdr = read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body).unwrap();
    let result = decode_body(&hdr, &body, Some(&wrong_key));
    assert!(result.is_err(), "Wrong key should fail decryption");
}

#[test]
fn test_compact_legacy_wal() {
    let dir = tempdir().unwrap();

    // Create a legacy .wal file (no header, raw entries).
    let entries = make_test_entries(3);
    let wal_path = dir.path().join("journal-0000000000000001.wal");
    std::fs::write(&wal_path, &entries).unwrap();

    let output = dir.path().join("compacted.graphj");
    let result = compact(&[&wal_path], &output, false, 0, None).unwrap();

    assert!(result.is_sealed());
    assert_eq!(result.first_seq, 1);
    assert_eq!(result.last_seq, 3);
    assert_eq!(result.entry_count, 3);

    // Verify body matches.
    let mut file = std::fs::File::open(&output).unwrap();
    let hdr = read_header(&mut file).unwrap().unwrap();
    let mut body = vec![0u8; hdr.body_len as usize];
    file.read_exact(&mut body).unwrap();
    assert_eq!(body, entries);
}

#[test]
fn test_reader_with_encrypted_sealed_file() {
    use crate::journal::{JournalReader, PendingEntry, JournalCommand, JournalState, spawn_journal_writer};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let journal_dir = dir.path().to_path_buf();
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    // Write 3 journal entries.
    let tx = spawn_journal_writer(journal_dir.clone(), 1024 * 1024, 100, state.clone());
    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Node {{id: {i}}})"),
            params: vec![],
        }))
        .unwrap();
    }

    // Flush to ensure all entries are written.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    drop(tx);

    // Wait for writer thread to exit and seal the segment.
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Read the sealed segment file.
    let segments: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("graphj"))
        .collect();
    assert_eq!(segments.len(), 1, "Should have 1 segment");

    // Now encrypt the sealed segment by compacting it.
    let key = [0x42u8; 32];
    let encrypted_path = dir.path().join("encrypted.graphj");
    compact(&[&segments[0]], &encrypted_path, false, 0, Some(&key)).unwrap();

    // Verify the encrypted file has the ENCRYPTED flag set.
    let mut file = std::fs::File::open(&encrypted_path).unwrap();
    let hdr = read_header(&mut file).unwrap().unwrap();
    assert_eq!(hdr.flags & FLAG_ENCRYPTED, FLAG_ENCRYPTED);
    assert_eq!(hdr.entry_count, 3);
    drop(file);

    // Now read the encrypted file with JournalReader using the correct key.
    // Replace the original segment with the encrypted one.
    std::fs::remove_file(&segments[0]).unwrap();
    std::fs::copy(&encrypted_path, &segments[0]).unwrap();

    // Read with correct key.
    let reader = JournalReader::from_sequence_with_key(
        &journal_dir,
        1,
        [0u8; 32],
        Some(key),
    )
    .unwrap();

    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 3);
    assert!(entries[0].entry.query.contains("id: 1"));
    assert!(entries[1].entry.query.contains("id: 2"));
    assert!(entries[2].entry.query.contains("id: 3"));

    // Verify reading with wrong key fails.
    let wrong_key = [0x99u8; 32];
    let reader_wrong = JournalReader::from_sequence_with_key(
        &journal_dir,
        1,
        [0u8; 32],
        Some(wrong_key),
    );
    // Should fail to decrypt when iterating.
    assert!(reader_wrong.is_ok(), "Reader should open but fail during iteration");
    let mut iter = reader_wrong.unwrap();
    assert!(iter.next().unwrap().is_err(), "Decryption should fail with wrong key");
}

#[test]
fn test_reader_mixed_encrypted_unencrypted_segments() {
    use crate::journal::{spawn_journal_writer, JournalCommand, JournalReader, JournalState, PendingEntry};
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let journal_dir = dir.path().to_path_buf();
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    // Write first segment (will be encrypted later).
    let tx = spawn_journal_writer(journal_dir.clone(), 200, 100, state.clone());
    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:A {id: 1})".to_string(),
        params: vec![],
    }))
    .unwrap();
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    drop(tx);
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Encrypt the first segment.
    let segments: Vec<std::path::PathBuf> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("graphj"))
        .collect();
    assert_eq!(segments.len(), 1);

    let key = [0x11u8; 32];
    let encrypted_path = dir.path().join("encrypted1.graphj");
    compact(&[&segments[0]], &encrypted_path, false, 0, Some(&key)).unwrap();
    std::fs::remove_file(&segments[0]).unwrap();
    std::fs::rename(&encrypted_path, &segments[0]).unwrap();

    // Write second segment (leave unencrypted).
    let state2 = Arc::new(JournalState::with_sequence_and_hash(
        state.sequence.load(std::sync::atomic::Ordering::SeqCst),
        *state.chain_hash.lock().unwrap(),
    ));
    let tx2 = spawn_journal_writer(journal_dir.clone(), 200, 100, state2.clone());
    tx2.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:B {id: 2})".to_string(),
        params: vec![],
    }))
    .unwrap();
    let (ack_tx2, ack_rx2) = std::sync::mpsc::sync_channel(1);
    tx2.send(JournalCommand::Flush(ack_tx2)).unwrap();
    ack_rx2.recv().unwrap();
    drop(tx2);
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Reader with encryption key should read both segments (one encrypted, one not).
    let reader = JournalReader::from_sequence_with_key(&journal_dir, 1, [0u8; 32], Some(key))
        .unwrap();

    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries[0].entry.query.contains("id: 1"));
    assert!(entries[1].entry.query.contains("id: 2"));
}
