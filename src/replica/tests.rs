use super::*;

#[test]
fn test_parse_source_s3() {
    let src = parse_source("s3://my-bucket/my/prefix").unwrap();
    match src {
        ReplicaSource::S3 { bucket, prefix } => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(prefix, "my/prefix");
        }
        _ => panic!("Expected S3 source"),
    }
}

#[test]
fn test_parse_source_s3_no_prefix() {
    let src = parse_source("s3://my-bucket").unwrap();
    match src {
        ReplicaSource::S3 { bucket, prefix } => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(prefix, "");
        }
        _ => panic!("Expected S3 source"),
    }
}

#[test]
fn test_parse_source_file() {
    let src = parse_source("file:///tmp/replica-source").unwrap();
    match src {
        ReplicaSource::File { path } => {
            assert_eq!(path, PathBuf::from("/tmp/replica-source"));
        }
        _ => panic!("Expected File source"),
    }
}

#[test]
fn test_parse_duration() {
    assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
    assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
    assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
    assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
    assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
}

#[test]
fn test_parse_snapshot_seq() {
    assert_eq!(
        parse_snapshot_seq_from_key("snapshots/0000000000000100.tar.zst"),
        Some(100)
    );
    assert_eq!(
        parse_snapshot_seq_from_filename("0000000000000100.tar.zst"),
        Some(100)
    );
}

#[test]
fn test_parse_journal_seq() {
    assert_eq!(
        parse_journal_seq_from_key("journal/journal-0000000000001000.graphj"),
        Some(1000)
    );
    assert_eq!(
        parse_journal_seq_from_filename("journal-0000000000001000.graphj"),
        Some(1000)
    );
}

#[test]
fn test_is_segment_sealed_returns_false_for_unsealed() {
    use crate::graphj;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal-0000000000000001.graphj");

    // Write an unsealed header (flags = 0).
    let header = graphj::GraphjHeader::new_unsealed(1, 0);
    std::fs::write(&path, graphj::encode_header(&header)).unwrap();

    assert!(!is_segment_sealed(&path).unwrap());
}

#[test]
fn test_is_segment_sealed_returns_true_for_sealed() {
    use crate::graphj;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal-0000000000000001.graphj");

    // Write a sealed header (FLAG_SEALED set).
    let mut header = graphj::GraphjHeader::new_unsealed(1, 0);
    header.flags = graphj::FLAG_SEALED;
    header.last_seq = 10;
    header.entry_count = 10;
    std::fs::write(&path, graphj::encode_header(&header)).unwrap();

    assert!(is_segment_sealed(&path).unwrap());
}

#[test]
fn test_file_sync_skips_unsealed_segments() {
    // Regression test: unsealed segments must never be copied because
    // the primary may be mid-write, producing a partial final entry.
    use crate::graphj;
    let dir = tempfile::tempdir().unwrap();

    // Create an unsealed segment (seq=1) — simulates active primary segment.
    let unsealed_path = dir.path().join("journal-0000000000000001.graphj");
    let header = graphj::GraphjHeader::new_unsealed(1, 0);
    std::fs::write(&unsealed_path, graphj::encode_header(&header)).unwrap();

    // Create a sealed segment (seq=50) — safe to copy.
    let sealed_path = dir.path().join("journal-0000000000000050.graphj");
    let mut sealed_header = graphj::GraphjHeader::new_unsealed(50, 0);
    sealed_header.flags = graphj::FLAG_SEALED;
    sealed_header.last_seq = 99;
    sealed_header.entry_count = 50;
    std::fs::write(&sealed_path, graphj::encode_header(&sealed_header)).unwrap();

    // Create a sealed segment already replayed (seq=1, but sealed) — should skip.
    let old_sealed_path = dir.path().join("journal-0000000000000000.graphj");
    let mut old_header = graphj::GraphjHeader::new_unsealed(0, 0);
    old_header.flags = graphj::FLAG_SEALED;
    old_header.last_seq = 0;
    old_header.entry_count = 1;
    std::fs::write(&old_sealed_path, graphj::encode_header(&old_header)).unwrap();

    // Simulate the filtering logic from poll_and_apply_file with last_local_seq = 49.
    let last_local_seq: u64 = 49;
    let mut synced = Vec::new();

    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let path = entry.unwrap().path();
        if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
            if let Some(seq) = parse_journal_seq_from_filename(filename) {
                let should_sync = if let Ok(is_sealed) = is_segment_sealed(&path) {
                    if !is_sealed {
                        false
                    } else {
                        seq > last_local_seq
                    }
                } else {
                    false
                };
                if should_sync {
                    synced.push(seq);
                }
            }
        }
    }

    // Only the sealed segment with seq=50 (> last_local_seq=49) should sync.
    assert_eq!(synced, vec![50], "Should only sync sealed segment with seq > last_local_seq");
}
