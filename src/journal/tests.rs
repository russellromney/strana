use super::*;

#[test]
fn test_is_mutation_create() {
    assert!(is_mutation("CREATE NODE TABLE Foo(id INT64, PRIMARY KEY(id))"));
    assert!(is_mutation("CREATE (:Foo {id: 1})"));
}

#[test]
fn test_is_mutation_merge() {
    assert!(is_mutation("MERGE (n:Foo {id: 1})"));
}

#[test]
fn test_is_mutation_delete() {
    assert!(is_mutation("MATCH (n:Foo) DELETE n"));
}

#[test]
fn test_is_mutation_drop() {
    assert!(is_mutation("DROP TABLE Foo"));
}

#[test]
fn test_is_mutation_alter() {
    assert!(is_mutation("ALTER TABLE Foo ADD col STRING"));
}

#[test]
fn test_is_mutation_copy() {
    assert!(is_mutation("COPY Foo FROM 'data.csv'"));
}

#[test]
fn test_is_mutation_set() {
    assert!(is_mutation("MATCH (n:Foo) SET n.name = 'bar'"));
}

#[test]
fn test_is_mutation_case_insensitive() {
    assert!(is_mutation("create node table Foo(id INT64, PRIMARY KEY(id))"));
    assert!(is_mutation("match (n:Foo) delete n"));
    assert!(is_mutation("Match (n:Foo) Set n.x = 1"));
}

#[test]
fn test_not_mutation_read() {
    assert!(!is_mutation("MATCH (n:Foo) RETURN n"));
    assert!(!is_mutation("RETURN 1"));
    assert!(!is_mutation("RETURN 'hello'"));
}

#[test]
fn test_not_mutation_substring() {
    // "CREATES" should not match "CREATE"
    assert!(!is_mutation("RETURN 'CREATES'"));
    // "DELETING" should not match "DELETE"
    assert!(!is_mutation("RETURN 'DELETING'"));
    // "OFFSET" should not match "SET" because SET is preceded by alpha
    assert!(!is_mutation("RETURN 'OFFSET'"));
}

#[test]
fn test_not_mutation_embedded_in_word() {
    // RESET contains SET but SET is preceded by 'E' (alpha) so no match
    assert!(!is_mutation("RETURN 'RESET'"));
    // SETTING starts with SET followed by alpha
    assert!(!is_mutation("RETURN 'SETTING'"));
}

// ─── New mutation keywords (INSTALL, UNINSTALL, IMPORT, ATTACH, DETACH) ───

#[test]
fn test_is_mutation_install() {
    assert!(is_mutation("INSTALL extension_name"));
    assert!(is_mutation("install myext"));
    assert!(is_mutation("Install SomeExtension"));
}

#[test]
fn test_is_mutation_uninstall() {
    assert!(is_mutation("UNINSTALL extension_name"));
    assert!(is_mutation("uninstall myext"));
}

#[test]
fn test_is_mutation_import() {
    assert!(is_mutation("IMPORT DATABASE '/path/to/db'"));
    assert!(is_mutation("import database '/tmp/backup'"));
}

#[test]
fn test_is_mutation_attach() {
    assert!(is_mutation("ATTACH DATABASE '/path/to/other.db'"));
    assert!(is_mutation("attach database '/tmp/other'"));
}

#[test]
fn test_is_mutation_detach() {
    assert!(is_mutation("DETACH DATABASE other_db"));
    assert!(is_mutation("detach database mydb"));
}

#[test]
fn test_uninstall_not_caught_by_install_boundary() {
    // UNINSTALL must be caught by its own keyword, not by INSTALL
    // (word boundary: 'N' before INSTALL is alphanumeric → INSTALL alone won't match)
    assert!(is_mutation("UNINSTALL ext"));
}

#[test]
fn test_not_mutation_new_keywords_substring() {
    // Suffixed forms should not match
    assert!(!is_mutation("RETURN 'INSTALLED'"));
    assert!(!is_mutation("RETURN 'IMPORTING'"));
    assert!(!is_mutation("RETURN 'ATTACHED'"));
    assert!(!is_mutation("RETURN 'DETACHED'"));
    assert!(!is_mutation("RETURN 'DETACHING'"));
}

#[test]
fn test_not_mutation_new_keywords_prefixed() {
    // Prefixed forms should not match (alpha before keyword)
    assert!(!is_mutation("RETURN 'REIMPORT'"));
    assert!(!is_mutation("RETURN 'REATTACH'"));
}

// ─── Comprehensive replica safety: every ladybug mutation type ───

/// This test documents every mutation type that ladybug supports
/// and verifies is_mutation catches all of them. If ladybug adds
/// new mutation types, add them here.
#[test]
fn test_replica_safety_all_mutation_types() {
    // ── Data modification clauses ──
    assert!(is_mutation("CREATE (:Person {name: 'Alice'})"));
    assert!(is_mutation("CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))"));
    assert!(is_mutation("CREATE REL TABLE KNOWS(FROM Person TO Person)"));
    assert!(is_mutation("MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]->(b)"));
    assert!(is_mutation("MERGE (n:Person {name: 'Bob'})"));
    assert!(is_mutation("MATCH (n:Person) SET n.age = 30"));
    assert!(is_mutation("MATCH (n:Person) DELETE n"));
    assert!(is_mutation("MATCH (n:Person) DETACH DELETE n"));

    // ── DDL ──
    assert!(is_mutation("DROP TABLE Person"));
    assert!(is_mutation("DROP TABLE IF EXISTS Person"));
    assert!(is_mutation("ALTER TABLE Person ADD age INT64"));
    assert!(is_mutation("ALTER TABLE Person DROP col"));
    assert!(is_mutation("ALTER TABLE Person RENAME TO People"));
    assert!(is_mutation("CREATE SEQUENCE my_seq"));
    assert!(is_mutation("DROP SEQUENCE my_seq"));
    assert!(is_mutation("CREATE TYPE my_type AS STRUCT(x INT64)"));
    assert!(is_mutation("CREATE MACRO my_func(x) AS x + 1"));
    assert!(is_mutation("DROP MACRO my_func"));
    assert!(is_mutation("CREATE GRAPH my_graph"));
    assert!(is_mutation("DROP GRAPH my_graph"));

    // ── Data import/export ──
    assert!(is_mutation("COPY Person FROM 'people.csv'"));
    assert!(is_mutation("COPY Person FROM 'people.parquet'"));

    // ── Extension management ──
    assert!(is_mutation("INSTALL httpfs"));
    assert!(is_mutation("UNINSTALL httpfs"));

    // ── Database management ──
    assert!(is_mutation("IMPORT DATABASE '/path/to/export'"));
    assert!(is_mutation("ATTACH DATABASE '/other/db' AS other"));
    assert!(is_mutation("DETACH DATABASE other"));

    // ── Read-only operations (must NOT trigger) ──
    assert!(!is_mutation("MATCH (n:Person) RETURN n"));
    assert!(!is_mutation("MATCH (n:Person) RETURN n.name ORDER BY n.name"));
    assert!(!is_mutation("MATCH (n:Person) RETURN count(n)"));
    assert!(!is_mutation("RETURN 1"));
    assert!(!is_mutation("RETURN 'hello world'"));
    assert!(!is_mutation("UNWIND [1, 2, 3] AS x RETURN x"));
    assert!(!is_mutation("CALL current_setting('threads')"));
    assert!(!is_mutation("EXPLAIN MATCH (n) RETURN n"));
    assert!(!is_mutation("EXPORT DATABASE '/path/to/export'"));
    assert!(!is_mutation("LOAD EXTENSION httpfs"));
    assert!(!is_mutation("USE DATABASE other"));
    assert!(!is_mutation("USE GRAPH my_graph"));
}

#[test]
fn test_param_values_to_map_entries_empty() {
    assert!(param_values_to_map_entries(&[]).is_empty());
}

#[test]
fn test_param_values_to_map_entries_values() {
    let params = vec![
        ("name".into(), ParamValue::String("Alice".into())),
        ("age".into(), ParamValue::Int(30)),
        ("active".into(), ParamValue::Bool(true)),
    ];
    let entries = param_values_to_map_entries(&params);
    assert_eq!(entries.len(), 3);
    let find = |k: &str| entries.iter().find(|e| e.key == k).unwrap();
    match find("name").value.as_ref().unwrap().value.as_ref().unwrap() {
        proto::graph_value::Value::StringValue(s) => assert_eq!(s, "Alice"),
        other => panic!("Expected StringValue, got {other:?}"),
    }
    match find("age").value.as_ref().unwrap().value.as_ref().unwrap() {
        proto::graph_value::Value::IntValue(i) => assert_eq!(*i, 30),
        other => panic!("Expected IntValue, got {other:?}"),
    }
}

#[test]
fn test_map_entries_to_param_values_empty() {
    assert!(map_entries_to_param_values(&[]).is_empty());
}

#[test]
fn test_param_conversion_roundtrip() {
    let params = vec![
        ("x".into(), ParamValue::Int(42)),
        ("y".into(), ParamValue::String("hello".into())),
        ("z".into(), ParamValue::Bool(true)),
    ];
    let entries = param_values_to_map_entries(&params);
    let back = map_entries_to_param_values(&entries);
    assert_eq!(params, back);
}

#[test]
fn test_encode_decode_roundtrip() {
    let prev_hash = [0u8; 32];
    let entry = proto::JournalEntry {
        sequence: 1,
        query: "CREATE (:Foo {id: 1})".into(),
        params: vec![],
        timestamp_ms: 1234567890,
    };
    let payload = entry.encode_to_vec();
    let buf = encode_entry(1, &prev_hash, &payload);

    let mut reader = std::io::Cursor::new(&buf);
    let decoded = read_entry_from(&mut reader).unwrap().unwrap();
    assert_eq!(decoded.sequence, 1);
    assert_eq!(decoded.prev_hash, prev_hash);
    let decoded_entry = proto::JournalEntry::decode(decoded.payload.as_slice()).unwrap();
    assert_eq!(decoded_entry.query, "CREATE (:Foo {id: 1})");
    assert_eq!(decoded_entry.timestamp_ms, 1234567890);
}

#[test]
fn test_crc32c_corruption_detected() {
    let prev_hash = [0u8; 32];
    let entry = proto::JournalEntry {
        sequence: 1,
        query: "test".into(),
        params: vec![],
        timestamp_ms: 0,
    };
    let payload = entry.encode_to_vec();
    let mut buf = encode_entry(1, &prev_hash, &payload);

    // Corrupt one byte in the payload area.
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;

    let mut reader = std::io::Cursor::new(&buf);
    let result = read_entry_from(&mut reader);
    assert!(result.is_err() || result.unwrap().is_none());
}

#[test]
fn test_write_read_roundtrip_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    let tx = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state.clone());

    // Write 3 entries.
    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:T {{id: {i}}})"),
            params: vec![],
        }))
        .unwrap();
    }

    // Flush and shutdown.
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Read back.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].sequence, 1);
    assert_eq!(entries[1].sequence, 2);
    assert_eq!(entries[2].sequence, 3);
    assert_eq!(entries[0].entry.query, "CREATE (:T {id: 1})");
    assert_eq!(entries[2].entry.query, "CREATE (:T {id: 3})");
}

#[test]
fn test_chain_hash_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    let tx = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state.clone());

    tx.send(JournalCommand::Write(PendingEntry {
        query: "q1".into(),
        params: vec![],
    }))
    .unwrap();
    tx.send(JournalCommand::Write(PendingEntry {
        query: "q2".into(),
        params: vec![],
    }))
    .unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    // Each entry should have a different chain hash.
    assert_ne!(entries[0].chain_hash, entries[1].chain_hash);
    // Chain hashes should not be all zeros.
    assert_ne!(entries[0].chain_hash, [0u8; 32]);
}

#[test]
fn test_segment_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    // Very small segment size to force rotation.
    let tx = spawn_journal_writer(journal_dir.clone(), 100, 100, state.clone());

    for _ in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Big {{data: '{}'}})", "x".repeat(50)),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Should have multiple segment files.
    let segments: Vec<_> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "graphj"))
        .collect();
    assert!(
        segments.len() > 1,
        "Expected multiple segments, got {}",
        segments.len()
    );

    // All entries should still be readable across segments.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 5);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence, i as u64 + 1);
    }
}

#[test]
fn test_segment_naming() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    let tx = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state.clone());

    tx.send(JournalCommand::Write(PendingEntry {
        query: "q1".into(),
        params: vec![],
    }))
    .unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    let files: Vec<String> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0], "journal-0000000000000001.graphj");
}

#[test]
fn test_empty_journal_read() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert!(entries.is_empty());
}

#[test]
fn test_read_from_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    let tx = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state.clone());

    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("q{i}"),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Read starting from sequence 3.
    let reader = JournalReader::from_sequence(&journal_dir, 3, [0u8; 32]).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].sequence, 3);
    assert_eq!(entries[0].entry.query, "q3");
    assert_eq!(entries[2].sequence, 5);
}

#[test]
fn test_journal_with_params() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    let tx = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state.clone());

    tx.send(JournalCommand::Write(PendingEntry {
        query: "CREATE (:Foo {name: $_uuid_0})".into(),
        params: vec![
            ("_uuid_0".into(), ParamValue::String("abc-123".into())),
            ("user_param".into(), ParamValue::Int(42)),
        ],
    }))
    .unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 1);
    let params = map_entries_to_param_values(&entries[0].entry.params);
    let find = |k: &str| params.iter().find(|(key, _)| key == k).unwrap().1.clone();
    assert_eq!(find("_uuid_0"), ParamValue::String("abc-123".into()));
    assert_eq!(find("user_param"), ParamValue::Int(42));
}

#[test]
fn test_state_sequence_progresses() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    let tx = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state.clone());

    assert_eq!(state.sequence.load(Ordering::SeqCst), 0);

    tx.send(JournalCommand::Write(PendingEntry {
        query: "q1".into(),
        params: vec![],
    }))
    .unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    assert_eq!(state.sequence.load(Ordering::SeqCst), 1);

    tx.send(JournalCommand::Write(PendingEntry {
        query: "q2".into(),
        params: vec![],
    }))
    .unwrap();

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();

    assert_eq!(state.sequence.load(Ordering::SeqCst), 2);

    tx.send(JournalCommand::Shutdown).unwrap();
}

#[test]
fn test_recover_state_empty_journal() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    std::fs::create_dir_all(&journal_dir).unwrap();

    let (seq, hash) = recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 0);
    assert_eq!(hash, [0u8; 32]);
}

#[test]
fn test_recover_state_nonexistent_dir() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("nope");

    let (seq, hash) = recover_journal_state(&journal_dir).unwrap();
    assert_eq!(seq, 0);
    assert_eq!(hash, [0u8; 32]);
}

#[test]
fn test_recover_state_from_existing_journal() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    let tx = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state.clone());

    // Write 3 entries.
    for i in 1..=3 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:T {{id: {i}}})"),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Record expected state.
    let expected_seq = state.sequence.load(Ordering::SeqCst);
    let expected_hash = *state.chain_hash.lock().unwrap_or_else(|e| e.into_inner());

    // Recover and compare.
    let (rec_seq, rec_hash) = recover_journal_state(&journal_dir).unwrap();
    assert_eq!(rec_seq, expected_seq);
    assert_eq!(rec_hash, expected_hash);
    assert_eq!(rec_seq, 3);
    assert_ne!(rec_hash, [0u8; 32]);
}

#[test]
fn test_recover_state_multiple_segments() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    // Small segment size to force rotation.
    let tx = spawn_journal_writer(journal_dir.clone(), 100, 100, state.clone());

    for _ in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:Big {{data: '{}'}})", "x".repeat(50)),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Should have multiple segments.
    let seg_count = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "graphj"))
        .count();
    assert!(seg_count > 1, "Expected multiple segments, got {seg_count}");

    let expected_seq = state.sequence.load(Ordering::SeqCst);
    let expected_hash = *state.chain_hash.lock().unwrap_or_else(|e| e.into_inner());

    let (rec_seq, rec_hash) = recover_journal_state(&journal_dir).unwrap();
    assert_eq!(rec_seq, expected_seq);
    assert_eq!(rec_hash, expected_hash);
}

#[test]
fn test_journal_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");

    // Phase 1: Write entries, shutdown.
    let state1 = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));
    let tx1 = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state1.clone());

    for i in 1..=3 {
        tx1.send(JournalCommand::Write(PendingEntry {
            query: format!("q{i}"),
            params: vec![],
        }))
        .unwrap();
    }
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx1.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx1.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Phase 2: Recover state, spawn new writer, write more entries.
    let (rec_seq, rec_hash) = recover_journal_state(&journal_dir).unwrap();
    assert_eq!(rec_seq, 3);

    let state2 = Arc::new(JournalState::with_sequence_and_hash(rec_seq, rec_hash));
    let tx2 = spawn_journal_writer(journal_dir.clone(), 64 * 1024 * 1024, 100, state2.clone());

    for i in 4..=6 {
        tx2.send(JournalCommand::Write(PendingEntry {
            query: format!("q{i}"),
            params: vec![],
        }))
        .unwrap();
    }
    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx2.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx2.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Phase 3: Read all entries — chain must validate end-to-end.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), 6);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence, i as u64 + 1);
        assert_eq!(e.entry.query, format!("q{}", i + 1));
    }
}

#[test]
fn test_param_array_roundtrip() {
    let params = vec![
        ("tags".into(), ParamValue::List(vec![
            ParamValue::String("a".into()),
            ParamValue::String("b".into()),
            ParamValue::String("c".into()),
        ])),
        ("embedding".into(), ParamValue::List(vec![
            ParamValue::Float(0.1),
            ParamValue::Float(0.2),
            ParamValue::Float(0.3),
        ])),
        ("ids".into(), ParamValue::List(vec![
            ParamValue::Int(1),
            ParamValue::Int(2),
            ParamValue::Int(3),
        ])),
    ];
    let entries = param_values_to_map_entries(&params);
    let back = map_entries_to_param_values(&entries);
    assert_eq!(params, back);
}

#[test]
fn test_param_nested_array_roundtrip() {
    let params = vec![
        ("matrix".into(), ParamValue::List(vec![
            ParamValue::List(vec![ParamValue::Int(1), ParamValue::Int(2)]),
            ParamValue::List(vec![ParamValue::Int(3), ParamValue::Int(4)]),
        ])),
    ];
    let entries = param_values_to_map_entries(&params);
    let back = map_entries_to_param_values(&entries);
    assert_eq!(params, back);
}

#[test]
fn test_read_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let journal_dir = dir.path().join("journal");
    let state = Arc::new(JournalState::with_sequence_and_hash(0, [0u8; 32]));

    // Small segment size to force rotation across multiple segments.
    let tx = spawn_journal_writer(journal_dir.clone(), 100, 100, state.clone());

    for i in 1..=5 {
        tx.send(JournalCommand::Write(PendingEntry {
            query: format!("CREATE (:T {{id: {i}, data: '{}'}})", "x".repeat(50)),
            params: vec![],
        }))
        .unwrap();
    }

    let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(0);
    tx.send(JournalCommand::Flush(ack_tx)).unwrap();
    ack_rx.recv().unwrap();
    tx.send(JournalCommand::Shutdown).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Read all entries and record the chain hash at seq 3.
    let reader = JournalReader::open(&journal_dir).unwrap();
    let all: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(all.len(), 5);
    let hash_at_3 = all[2].chain_hash; // chain_hash after seq 3

    // Simulate compaction: delete segments fully before seq 3.
    // A segment is safe to delete if the NEXT segment starts at <= min_seq.
    let mut segments: Vec<(std::path::PathBuf, u64)> = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let path = e.path();
            let name = path.file_stem()?.to_string_lossy().to_string();
            let seq_str = name.strip_prefix("journal-")?;
            let seq: u64 = seq_str.parse().ok()?;
            Some((path, seq))
        })
        .collect();
    segments.sort_by_key(|(_, seq)| *seq);

    let min_seq = 3u64;
    for i in 0..segments.len().saturating_sub(1) {
        if segments[i + 1].1 <= min_seq {
            std::fs::remove_file(&segments[i].0).unwrap();
        }
    }

    // Verify some segments were deleted.
    let remaining_count = std::fs::read_dir(&journal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .count();
    assert!(remaining_count < segments.len(), "Expected compaction to delete segments");

    // Now read from seq 4 using hash_at_3.
    // Pre-start entries in remaining segments skip chain validation, then
    // the first entry at/after start_seq validates the chain connection.
    let reader = JournalReader::from_sequence(&journal_dir, 4, hash_at_3).unwrap();
    let entries: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(entries.len(), 2); // entries 4 and 5
    assert_eq!(entries[0].sequence, 4);
    assert_eq!(entries[1].sequence, 5);
}
