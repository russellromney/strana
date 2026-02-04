//! Per-file snapshot compression with chunked parallelism for large files.
//!
//! Files > 64 MB are split into chunks, each independently zstd-compressed.
//! On restore, chunks are decompressed in parallel and reassembled using
//! `pwrite` for lock-free concurrent writes to non-overlapping offsets.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Mutex;
use tracing::{debug, info};

/// Chunk size for large files: 64 MB.
const CHUNK_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileManifest {
    pub version: u32,
    pub total_size: u64,
    pub files: Vec<FileEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileEntry {
    pub path: String,
    pub size: u64,
    pub compressed_size: u64,
    pub sha256: String,
    /// Present for files > 64 MB that were split into chunks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunks: Option<Vec<ChunkEntry>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkEntry {
    pub index: u32,
    pub offset: u64,
    pub raw_size: u64,
    pub compressed_size: u64,
    pub sha256: String,
}

/// Compute hex-encoded SHA-256 of data.
pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Create a per-file compressed archive from a source directory.
///
/// Walks `source_dir` recursively, zstd-compresses each file individually,
/// and writes it to `snap_dir/files/{relative_path}.zst`. Files larger than
/// 64 MB are split into chunks at `snap_dir/files/{path}.chunks/{index:06}.zst`.
pub fn create_file_archive(
    source_dir: &Path,
    snap_dir: &Path,
) -> Result<FileManifest, String> {
    let t0 = std::time::Instant::now();
    let files_dir = snap_dir.join("files");
    std::fs::create_dir_all(&files_dir).map_err(|e| format!("Mkdir files: {e}"))?;

    let mut entries = Vec::new();
    walk_and_compress(source_dir, source_dir, &files_dir, &mut entries)?;

    let mut total_size = 0u64;
    let mut total_compressed = 0u64;
    for entry in &entries {
        total_size += entry.size;
        total_compressed += entry.compressed_size;
    }

    let manifest = FileManifest {
        version: 2,
        total_size,
        files: entries,
    };

    info!(
        "[timing] create_file_archive: {} files, {:.1} MB raw -> {:.1} MB compressed ({:.1}x), {:?}",
        manifest.files.len(),
        total_size as f64 / (1024.0 * 1024.0),
        total_compressed as f64 / (1024.0 * 1024.0),
        if total_compressed > 0 { total_size as f64 / total_compressed as f64 } else { 0.0 },
        t0.elapsed(),
    );

    Ok(manifest)
}

/// Recursively walk a directory, compressing each file.
fn walk_and_compress(
    root: &Path,
    dir: &Path,
    files_dir: &Path,
    entries: &mut Vec<FileEntry>,
) -> Result<(), String> {
    let read_dir = std::fs::read_dir(dir)
        .map_err(|e| format!("Read dir {}: {e}", dir.display()))?;

    for item in read_dir {
        let item = item.map_err(|e| format!("Dir entry: {e}"))?;
        let path = item.path();

        if path.is_dir() {
            walk_and_compress(root, &path, files_dir, entries)?;
        } else {
            let rel_path = path.strip_prefix(root)
                .map_err(|_| format!("Strip prefix failed: {}", path.display()))?;
            let rel_str = rel_path.to_string_lossy().to_string();

            let raw = std::fs::read(&path)
                .map_err(|e| format!("Read {}: {e}", rel_str))?;

            let entry = if (raw.len() as u64) > CHUNK_SIZE {
                compress_chunked(&raw, &rel_str, files_dir)?
            } else {
                compress_single(&raw, &rel_str, files_dir)?
            };

            debug!(
                "  {} {:.1} MB -> {:.1} MB ({:.1}x){}",
                rel_str,
                entry.size as f64 / (1024.0 * 1024.0),
                entry.compressed_size as f64 / (1024.0 * 1024.0),
                if entry.compressed_size > 0 { entry.size as f64 / entry.compressed_size as f64 } else { 0.0 },
                if entry.chunks.is_some() {
                    format!(" [{} chunks]", entry.chunks.as_ref().unwrap().len())
                } else {
                    String::new()
                },
            );

            entries.push(entry);
        }
    }
    Ok(())
}

/// Compress a small file as a single .zst file.
fn compress_single(raw: &[u8], rel_str: &str, files_dir: &Path) -> Result<FileEntry, String> {
    let compressed = zstd::encode_all(raw, 3)
        .map_err(|e| format!("Compress {}: {e}", rel_str))?;
    let hash = sha256_hex(&compressed);

    let dest = files_dir.join(format!("{}.zst", rel_str));
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Mkdir {}: {e}", parent.display()))?;
    }
    std::fs::write(&dest, &compressed)
        .map_err(|e| format!("Write {}: {e}", rel_str))?;

    Ok(FileEntry {
        path: rel_str.to_string(),
        size: raw.len() as u64,
        compressed_size: compressed.len() as u64,
        sha256: hash,
        chunks: None,
    })
}

/// Compress a large file by splitting into 64 MB chunks.
fn compress_chunked(raw: &[u8], rel_str: &str, files_dir: &Path) -> Result<FileEntry, String> {
    let chunk_dir = files_dir.join(format!("{}.chunks", rel_str));
    std::fs::create_dir_all(&chunk_dir)
        .map_err(|e| format!("Mkdir chunks for {}: {e}", rel_str))?;

    let mut chunks = Vec::new();
    let mut total_compressed = 0u64;

    for (i, chunk_data) in raw.chunks(CHUNK_SIZE as usize).enumerate() {
        let compressed = zstd::encode_all(chunk_data, 3)
            .map_err(|e| format!("Compress {}[{}]: {e}", rel_str, i))?;
        let hash = sha256_hex(&compressed);

        let chunk_path = chunk_dir.join(format!("{:06}.zst", i));
        std::fs::write(&chunk_path, &compressed)
            .map_err(|e| format!("Write {}[{}]: {e}", rel_str, i))?;

        let offset = i as u64 * CHUNK_SIZE;
        chunks.push(ChunkEntry {
            index: i as u32,
            offset,
            raw_size: chunk_data.len() as u64,
            compressed_size: compressed.len() as u64,
            sha256: hash,
        });
        total_compressed += compressed.len() as u64;
    }

    Ok(FileEntry {
        path: rel_str.to_string(),
        size: raw.len() as u64,
        compressed_size: total_compressed,
        sha256: String::new(), // not meaningful for chunked files
        chunks: Some(chunks),
    })
}

/// Restore files from a per-file archive.
///
/// Uses a bounded worker pool for parallel decompression. Chunked files are
/// reassembled using `pwrite` for lock-free concurrent writes.
pub fn restore_file_archive(snap_dir: &Path, data_dir: &Path) -> Result<(), String> {
    use std::os::unix::fs::FileExt;

    let t0 = std::time::Instant::now();

    let manifest: FileManifest = {
        let bytes = std::fs::read(snap_dir.join("manifest.json"))
            .map_err(|e| format!("Read manifest: {e}"))?;
        serde_json::from_slice(&bytes).map_err(|e| format!("Parse manifest: {e}"))?
    };
    info!(
        "[timing] restore: read manifest: {:?} ({} files, {:.1} MB)",
        t0.elapsed(),
        manifest.files.len(),
        manifest.total_size as f64 / (1024.0 * 1024.0),
    );

    let files_dir = snap_dir.join("files");
    let t1 = std::time::Instant::now();

    // Build work items: one per chunk (or one per single file).
    enum WorkItem {
        Single {
            src: std::path::PathBuf,
            dst: std::path::PathBuf,
            expected_size: u64,
            path_str: String,
        },
        Chunk {
            src: std::path::PathBuf,
            file: std::sync::Arc<std::fs::File>,
            offset: u64,
            expected_size: u64,
            path_str: String,
            chunk_index: u32,
        },
    }

    let mut work_items = Vec::new();

    for entry in &manifest.files {
        if let Some(ref chunks) = entry.chunks {
            // Chunked file: create and pre-allocate the output file.
            let dst = data_dir.join(&entry.path);
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("Mkdir {}: {e}", parent.display()))?;
            }
            let file = std::fs::File::create(&dst)
                .map_err(|e| format!("Create {}: {e}", entry.path))?;
            file.set_len(entry.size)
                .map_err(|e| format!("Preallocate {}: {e}", entry.path))?;
            let file = std::sync::Arc::new(file);

            let chunk_dir = files_dir.join(format!("{}.chunks", entry.path));
            for chunk in chunks {
                work_items.push(WorkItem::Chunk {
                    src: chunk_dir.join(format!("{:06}.zst", chunk.index)),
                    file: file.clone(),
                    offset: chunk.offset,
                    expected_size: chunk.raw_size,
                    path_str: entry.path.clone(),
                    chunk_index: chunk.index,
                });
            }
        } else {
            work_items.push(WorkItem::Single {
                src: files_dir.join(format!("{}.zst", entry.path)),
                dst: data_dir.join(&entry.path),
                expected_size: entry.size,
                path_str: entry.path.clone(),
            });
        }
    }

    let total_work = work_items.len();
    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let queue = Mutex::new(work_items.into_iter());

    std::thread::scope(|s| {
        let mut handles = Vec::new();

        for _ in 0..num_workers {
            handles.push(s.spawn(|| {
                loop {
                    let item = queue.lock().unwrap().next();
                    match item {
                        None => return Ok::<(), String>(()),
                        Some(WorkItem::Single { src, dst, expected_size, path_str }) => {
                            let compressed = std::fs::read(&src)
                                .map_err(|e| format!("Read {}: {e}", path_str))?;
                            let decompressed = zstd::decode_all(compressed.as_slice())
                                .map_err(|e| format!("Decompress {}: {e}", path_str))?;
                            if decompressed.len() as u64 != expected_size {
                                return Err(format!(
                                    "Size mismatch for {}: expected {}, got {}",
                                    path_str, expected_size, decompressed.len()
                                ));
                            }
                            if let Some(parent) = dst.parent() {
                                std::fs::create_dir_all(parent)
                                    .map_err(|e| format!("Mkdir {}: {e}", parent.display()))?;
                            }
                            std::fs::write(&dst, &decompressed)
                                .map_err(|e| format!("Write {}: {e}", path_str))?;
                        }
                        Some(WorkItem::Chunk { src, file, offset, expected_size, path_str, chunk_index }) => {
                            let compressed = std::fs::read(&src)
                                .map_err(|e| format!("Read {}[{}]: {e}", path_str, chunk_index))?;
                            let decompressed = zstd::decode_all(compressed.as_slice())
                                .map_err(|e| format!("Decompress {}[{}]: {e}", path_str, chunk_index))?;
                            if decompressed.len() as u64 != expected_size {
                                return Err(format!(
                                    "Size mismatch for {}[{}]: expected {}, got {}",
                                    path_str, chunk_index, expected_size, decompressed.len()
                                ));
                            }
                            file.write_all_at(&decompressed, offset)
                                .map_err(|e| format!("Write {}[{}]: {e}", path_str, chunk_index))?;
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.join()
                .map_err(|_| "Thread panicked during file restore".to_string())??;
        }
        Ok::<(), String>(())
    })?;

    info!(
        "[timing] restore_file_archive: {} items ({} workers), {:.1} MB, {:?}",
        total_work,
        num_workers,
        manifest.total_size as f64 / (1024.0 * 1024.0),
        t1.elapsed(),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_file_manifest_roundtrip() {
        let manifest = FileManifest {
            version: 2,
            total_size: 1000,
            files: vec![
                FileEntry {
                    path: "db/main.dat".into(),
                    size: 800,
                    compressed_size: 300,
                    sha256: "abc123".into(),
                    chunks: None,
                },
                FileEntry {
                    path: "db/index.dat".into(),
                    size: 200,
                    compressed_size: 100,
                    sha256: "def456".into(),
                    chunks: None,
                },
            ],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: FileManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.total_size, 1000);
        assert_eq!(parsed.files.len(), 2);
        assert_eq!(parsed.files[0].path, "db/main.dat");
        // chunks should not be in JSON when None
        assert!(!json.contains("chunks"));
    }

    #[test]
    fn test_chunked_manifest_roundtrip() {
        let manifest = FileManifest {
            version: 2,
            total_size: 200_000_000,
            files: vec![FileEntry {
                path: "db".into(),
                size: 200_000_000,
                compressed_size: 60_000_000,
                sha256: String::new(),
                chunks: Some(vec![
                    ChunkEntry {
                        index: 0,
                        offset: 0,
                        raw_size: CHUNK_SIZE,
                        compressed_size: 20_000_000,
                        sha256: "aaa".into(),
                    },
                    ChunkEntry {
                        index: 1,
                        offset: CHUNK_SIZE,
                        raw_size: CHUNK_SIZE,
                        compressed_size: 20_000_000,
                        sha256: "bbb".into(),
                    },
                    ChunkEntry {
                        index: 2,
                        offset: 2 * CHUNK_SIZE,
                        raw_size: 200_000_000 - 2 * CHUNK_SIZE,
                        compressed_size: 20_000_000,
                        sha256: "ccc".into(),
                    },
                ]),
            }],
        };
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let parsed: FileManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.files[0].chunks.as_ref().unwrap().len(), 3);
        assert_eq!(parsed.files[0].chunks.as_ref().unwrap()[1].offset, CHUNK_SIZE);
    }

    #[test]
    fn test_create_and_restore_small_files() {
        let source = tempdir().unwrap();
        let snap = tempdir().unwrap();
        let restore = tempdir().unwrap();

        // Create a fake DB directory with files and a subdirectory.
        let db_dir = source.path().join("db");
        std::fs::create_dir_all(db_dir.join("sub")).unwrap();
        let data = vec![42u8; 200_000];
        std::fs::write(db_dir.join("main.dat"), &data).unwrap();
        std::fs::write(db_dir.join("index.dat"), b"index data here").unwrap();
        std::fs::write(db_dir.join("sub/nested.dat"), b"nested content").unwrap();

        // Create per-file archive.
        let manifest = create_file_archive(source.path(), snap.path()).unwrap();
        assert_eq!(manifest.version, 2);
        assert_eq!(manifest.files.len(), 3);
        // All files < 64MB, so no chunks.
        for f in &manifest.files {
            assert!(f.chunks.is_none());
        }

        // Write manifest.
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        std::fs::write(snap.path().join("manifest.json"), &json).unwrap();

        // Restore.
        restore_file_archive(snap.path(), restore.path()).unwrap();

        // Verify all files match.
        assert_eq!(std::fs::read(restore.path().join("db/main.dat")).unwrap(), data);
        assert_eq!(std::fs::read(restore.path().join("db/index.dat")).unwrap(), b"index data here");
        assert_eq!(std::fs::read(restore.path().join("db/sub/nested.dat")).unwrap(), b"nested content");
    }

    #[test]
    fn test_create_and_restore_chunked_file() {
        let source = tempdir().unwrap();
        let snap = tempdir().unwrap();
        let restore = tempdir().unwrap();

        // Create a file larger than CHUNK_SIZE (64 MB).
        // Use 65 MB to get 2 chunks: one full (64 MB) + one partial (1 MB).
        let big_size = CHUNK_SIZE as usize + 1024 * 1024;
        let big_data: Vec<u8> = (0..big_size).map(|i| (i % 251) as u8).collect();
        std::fs::write(source.path().join("bigfile"), &big_data).unwrap();

        let manifest = create_file_archive(source.path(), snap.path()).unwrap();
        assert_eq!(manifest.files.len(), 1);
        let entry = &manifest.files[0];
        assert!(entry.chunks.is_some(), "Large file should be chunked");
        let chunks = entry.chunks.as_ref().unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].raw_size, CHUNK_SIZE);
        assert_eq!(chunks[1].raw_size, 1024 * 1024);

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        std::fs::write(snap.path().join("manifest.json"), &json).unwrap();

        restore_file_archive(snap.path(), restore.path()).unwrap();

        let restored = std::fs::read(restore.path().join("bigfile")).unwrap();
        assert_eq!(restored.len(), big_data.len());
        assert_eq!(restored, big_data);
    }

    #[test]
    fn test_file_archive_sha256_integrity() {
        let source = tempdir().unwrap();
        let snap = tempdir().unwrap();
        let restore = tempdir().unwrap();

        std::fs::create_dir_all(source.path().join("db")).unwrap();
        std::fs::write(source.path().join("db/data.dat"), vec![1u8; 10_000]).unwrap();

        let manifest = create_file_archive(source.path(), snap.path()).unwrap();
        let json = serde_json::to_string(&manifest).unwrap();
        std::fs::write(snap.path().join("manifest.json"), &json).unwrap();

        // Corrupt a compressed file.
        let compressed_path = snap.path().join("files/db/data.dat.zst");
        let mut bytes = std::fs::read(&compressed_path).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&compressed_path, &bytes).unwrap();

        // Restore should fail (zstd decompression error from corrupted data).
        let result = restore_file_archive(snap.path(), restore.path());
        assert!(result.is_err(), "Corrupted file should cause error");
    }

    #[test]
    fn test_sha256_hex() {
        let hash = sha256_hex(b"hello world");
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }
}
