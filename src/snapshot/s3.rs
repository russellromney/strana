//! S3 upload / download for snapshots (per-file format with chunked large files).

use std::path::Path;
use tracing::info;

/// Reference to a snapshot in S3.
pub struct SnapshotS3Ref {
    pub sequence: u64,
    pub prefix: String,
}

/// Find the latest snapshot in S3 by looking for manifest.json keys.
pub async fn find_latest_snapshot_s3(
    bucket: &str,
    prefix: &str,
) -> Result<SnapshotS3Ref, String> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);

    let resp = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .map_err(|e| format!("S3 list failed: {e}"))?;

    let contents = resp.contents();
    if contents.is_empty() {
        return Err("No snapshots found in S3 bucket".to_string());
    }

    let mut latest: Option<(u64, String)> = None;

    for obj in contents {
        if let Some(key) = obj.key() {
            if key.ends_with("/manifest.json") {
                if let Some(seq) = parse_seq_from_key(key) {
                    match latest {
                        Some((best, _)) if seq <= best => {}
                        _ => {
                            let snap_prefix =
                                key.strip_suffix("/manifest.json").unwrap().to_string();
                            latest = Some((seq, snap_prefix));
                        }
                    }
                }
            }
        }
    }

    latest
        .map(|(sequence, prefix)| SnapshotS3Ref { sequence, prefix })
        .ok_or_else(|| "No snapshot manifests found in S3".to_string())
}

fn parse_seq_from_key(key: &str) -> Option<u64> {
    let stripped = key.strip_suffix("/manifest.json")?;
    let seq_str = stripped.rsplit('/').next()?;
    seq_str.parse().ok()
}

/// Upload a snapshot to S3. Each compressed file/chunk is a separate S3 object.
///
/// Upload order: files first, then snapshot.meta, then manifest.json LAST (atomicity).
pub async fn upload_snapshot_s3_chunked(
    snap_dir: &Path,
    bucket: &str,
    prefix: &str,
    sequence: u64,
) -> Result<String, String> {
    use std::sync::Arc;

    let s3_prefix = format!("{}snapshots/{:016}", prefix, sequence);

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = Arc::new(aws_sdk_s3::Client::new(&config));

    let manifest_bytes = std::fs::read(snap_dir.join("manifest.json"))
        .map_err(|e| format!("Read manifest.json: {e}"))?;
    let manifest: super::chunked::FileManifest =
        serde_json::from_slice(&manifest_bytes)
            .map_err(|e| format!("Parse manifest: {e}"))?;

    let sem = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_TRANSFERS));
    let mut handles = Vec::new();
    let files_dir = snap_dir.join("files");

    for entry in &manifest.files {
        if let Some(ref chunks) = entry.chunks {
            // Chunked file: upload each chunk separately.
            for chunk in chunks {
                let local_path = files_dir.join(format!(
                    "{}.chunks/{:06}.zst", entry.path, chunk.index
                ));
                let file_bytes = std::fs::read(&local_path)
                    .map_err(|e| format!("Read {}[{}]: {e}", entry.path, chunk.index))?;
                let key = format!(
                    "{}/files/{}.chunks/{:06}.zst",
                    s3_prefix, entry.path, chunk.index
                );
                let client = client.clone();
                let bucket = bucket.to_string();
                let sem = sem.clone();

                handles.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await.map_err(|_| "Sem closed".to_string())?;
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(file_bytes.into())
                        .send()
                        .await
                        .map_err(|e| format!("Upload {}: {e}", key))?;
                    Ok::<(), String>(())
                }));
            }
        } else {
            // Single file: upload as before.
            let local_path = files_dir.join(format!("{}.zst", entry.path));
            let file_bytes = std::fs::read(&local_path)
                .map_err(|e| format!("Read {}: {e}", entry.path))?;
            let key = format!("{}/files/{}.zst", s3_prefix, entry.path);
            let client = client.clone();
            let bucket = bucket.to_string();
            let sem = sem.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.map_err(|_| "Sem closed".to_string())?;
                client
                    .put_object()
                    .bucket(&bucket)
                    .key(&key)
                    .body(file_bytes.into())
                    .send()
                    .await
                    .map_err(|e| format!("Upload {}: {e}", key))?;
                Ok::<(), String>(())
            }));
        }
    }

    for handle in handles {
        handle.await.map_err(|e| format!("Task join: {e}"))??;
    }

    // Upload snapshot.meta.
    let meta_bytes = std::fs::read(snap_dir.join("snapshot.meta"))
        .map_err(|e| format!("Read snapshot.meta: {e}"))?;
    client
        .put_object()
        .bucket(bucket)
        .key(format!("{}/snapshot.meta", s3_prefix))
        .body(meta_bytes.into())
        .send()
        .await
        .map_err(|e| format!("Upload snapshot.meta: {e}"))?;

    // Upload manifest.json LAST for atomicity.
    client
        .put_object()
        .bucket(bucket)
        .key(format!("{}/manifest.json", s3_prefix))
        .body(manifest_bytes.into())
        .send()
        .await
        .map_err(|e| format!("Upload manifest.json: {e}"))?;

    info!(
        "Snapshot uploaded to s3://{}/{} ({} files)",
        bucket, s3_prefix, manifest.files.len()
    );
    Ok(s3_prefix)
}

/// Download a snapshot from S3. Downloads each compressed file/chunk in parallel.
pub async fn download_snapshot_s3_chunked(
    bucket: &str,
    s3_prefix: &str,
    dest_dir: &Path,
) -> Result<(), String> {
    use std::sync::Arc;

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = Arc::new(aws_sdk_s3::Client::new(&config));

    let manifest_bytes =
        download_s3_object(&client, bucket, &format!("{}/manifest.json", s3_prefix)).await?;
    let meta_bytes =
        download_s3_object(&client, bucket, &format!("{}/snapshot.meta", s3_prefix)).await?;

    std::fs::create_dir_all(dest_dir).map_err(|e| format!("Mkdir: {e}"))?;
    std::fs::write(dest_dir.join("manifest.json"), &manifest_bytes)
        .map_err(|e| format!("Write manifest: {e}"))?;
    std::fs::write(dest_dir.join("snapshot.meta"), &meta_bytes)
        .map_err(|e| format!("Write meta: {e}"))?;

    let manifest: super::chunked::FileManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|e| format!("Parse manifest: {e}"))?;

    let files_dir = dest_dir.join("files");
    std::fs::create_dir_all(&files_dir).map_err(|e| format!("Mkdir files: {e}"))?;

    let sem = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_TRANSFERS));
    let mut handles = Vec::new();

    for entry in &manifest.files {
        if let Some(ref chunks) = entry.chunks {
            // Chunked file: download each chunk in parallel.
            let chunk_dir = files_dir.join(format!("{}.chunks", entry.path));
            std::fs::create_dir_all(&chunk_dir)
                .map_err(|e| format!("Mkdir chunks: {e}"))?;

            for chunk in chunks {
                let key = format!(
                    "{}/files/{}.chunks/{:06}.zst",
                    s3_prefix, entry.path, chunk.index
                );
                let dest_path = chunk_dir.join(format!("{:06}.zst", chunk.index));
                let expected_sha = chunk.sha256.clone();
                let client = client.clone();
                let bucket = bucket.to_string();
                let sem = sem.clone();
                let path_str = entry.path.clone();
                let chunk_index = chunk.index;

                handles.push(tokio::spawn(async move {
                    let compressed = {
                        let _permit =
                            sem.acquire().await.map_err(|_| "Sem closed".to_string())?;
                        download_s3_object(&client, &bucket, &key).await?
                    };

                    let actual_sha = super::chunked::sha256_hex(&compressed);
                    if actual_sha != expected_sha {
                        return Err(format!(
                            "Integrity failed for {}[{}]: expected {}, got {}",
                            path_str, chunk_index, expected_sha, actual_sha
                        ));
                    }

                    tokio::fs::write(&dest_path, &compressed)
                        .await
                        .map_err(|e| format!("Write {}[{}]: {e}", path_str, chunk_index))?;

                    Ok::<(), String>(())
                }));
            }
        } else {
            // Single file: download as before.
            let key = format!("{}/files/{}.zst", s3_prefix, entry.path);
            let dest_path = files_dir.join(format!("{}.zst", entry.path));
            let expected_sha = entry.sha256.clone();
            let client = client.clone();
            let bucket = bucket.to_string();
            let sem = sem.clone();

            handles.push(tokio::spawn(async move {
                let compressed = {
                    let _permit = sem.acquire().await.map_err(|_| "Sem closed".to_string())?;
                    download_s3_object(&client, &bucket, &key).await?
                };

                let actual_sha = super::chunked::sha256_hex(&compressed);
                if actual_sha != expected_sha {
                    return Err(format!(
                        "Integrity failed for {}: expected {}, got {}",
                        key, expected_sha, actual_sha
                    ));
                }

                if let Some(parent) = dest_path.parent() {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .map_err(|e| format!("Mkdir {}: {e}", parent.display()))?;
                }
                tokio::fs::write(&dest_path, &compressed)
                    .await
                    .map_err(|e| format!("Write {}: {e}", dest_path.display()))?;

                Ok::<(), String>(())
            }));
        }
    }

    for handle in handles {
        handle.await.map_err(|e| format!("Task join: {e}"))??;
    }

    info!(
        "Snapshot downloaded: {} files, {:.1} MB compressed",
        manifest.files.len(),
        manifest.files.iter().map(|f| f.compressed_size).sum::<u64>() as f64 / (1024.0 * 1024.0),
    );

    Ok(())
}

const MAX_PARALLEL_TRANSFERS: usize = 8;

async fn download_s3_object(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>, String> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("S3 get {}: {e}", key))?;
    let bytes = resp
        .body
        .collect()
        .await
        .map_err(|e| format!("S3 read {}: {e}", key))?
        .into_bytes();
    Ok(bytes.to_vec())
}
