use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graphd::graphj::{
    compact, decode_body, encode_header, read_header, GraphjHeader, COMPRESSION_NONE,
    COMPRESSION_ZSTD, ENCRYPTION_NONE, ENCRYPTION_XCHACHA20POLY1305, FLAG_ENCRYPTED, FLAG_SEALED,
};
use std::io::{Seek, SeekFrom, Write};

/// Generate realistic Cypher query data for benchmarking.
fn generate_cypher_queries(count: usize) -> Vec<u8> {
    let query = r#"MERGE (n:Entity {id: $id}) SET n.name = $name, n.embedding = $embedding, n.created_at = $timestamp RETURN n"#;
    let mut data = Vec::new();
    for i in 0..count {
        data.extend_from_slice(query.as_bytes());
        data.extend_from_slice(format!(" // Query {}\n", i).as_bytes());
    }
    data
}

/// Benchmark header encoding.
fn bench_header_encode(c: &mut Criterion) {
    let header = GraphjHeader {
        flags: FLAG_SEALED,
        compression: COMPRESSION_ZSTD,
        encryption: ENCRYPTION_NONE,
        zstd_level: 3,
        first_seq: 1,
        last_seq: 1000,
        entry_count: 1000,
        body_len: 65536,
        body_checksum: [0u8; 32],
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };

    c.bench_function("header_encode", |b| {
        b.iter(|| encode_header(black_box(&header)))
    });
}

/// Benchmark header decoding.
fn bench_header_decode(c: &mut Criterion) {
    let header = GraphjHeader {
        flags: FLAG_SEALED,
        compression: COMPRESSION_ZSTD,
        encryption: ENCRYPTION_NONE,
        zstd_level: 3,
        first_seq: 1,
        last_seq: 1000,
        entry_count: 1000,
        body_len: 65536,
        body_checksum: [0u8; 32],
        nonce: [0u8; 24],
        created_ms: 1700000000000,
    };
    let encoded = encode_header(&header);

    c.bench_function("header_decode", |b| {
        b.iter(|| {
            let mut cursor = std::io::Cursor::new(black_box(&encoded));
            read_header(&mut cursor).unwrap()
        })
    });
}

/// Benchmark compression at different levels.
fn bench_compression_levels(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_levels");
    let data = generate_cypher_queries(100); // ~10KB of Cypher
    group.throughput(Throughput::Bytes(data.len() as u64));

    for level in [1, 3, 6, 9, 15, 22] {
        group.bench_with_input(BenchmarkId::from_parameter(level), &level, |b, &level| {
            b.iter(|| zstd::encode_all(black_box(&data[..]), level).unwrap());
        });
    }
    group.finish();
}

/// Benchmark decompression.
fn bench_decompression(c: &mut Criterion) {
    let mut group = c.benchmark_group("decompression");
    let data = generate_cypher_queries(100);
    let compressed = zstd::encode_all(&data[..], 3).unwrap();
    group.throughput(Throughput::Bytes(compressed.len() as u64));

    group.bench_function("zstd_level_3", |b| {
        b.iter(|| zstd::decode_all(black_box(&compressed[..])).unwrap());
    });
    group.finish();
}

/// Benchmark encryption (XChaCha20-Poly1305).
fn bench_encryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("encryption");
    let data = generate_cypher_queries(100);
    let key = [0x42u8; 32];
    group.throughput(Throughput::Bytes(data.len() as u64));

    // Build header for AAD
    let header = GraphjHeader {
        flags: FLAG_SEALED | FLAG_ENCRYPTED,
        compression: COMPRESSION_NONE,
        encryption: ENCRYPTION_XCHACHA20POLY1305,
        zstd_level: 0,
        first_seq: 1,
        last_seq: 100,
        entry_count: 100,
        body_len: data.len() as u64,
        body_checksum: [0u8; 32],
        nonce: [0x11u8; 24],
        created_ms: 1700000000000,
    };

    group.bench_function("encrypt", |b| {
        b.iter(|| {
            use chacha20poly1305::{
                aead::{Aead, KeyInit, Payload},
                XChaCha20Poly1305, XNonce,
            };
            let cipher = XChaCha20Poly1305::new(&key.into());
            let nonce = XNonce::from_slice(&header.nonce);
            let aad = &encode_header(&header)[..40];
            cipher
                .encrypt(nonce, Payload {
                    msg: black_box(&data),
                    aad,
                })
                .unwrap();
        });
    });
    group.finish();
}

/// Benchmark decryption (XChaCha20-Poly1305).
fn bench_decryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("decryption");
    let data = generate_cypher_queries(100);
    let key = [0x42u8; 32];

    // Encrypt the data first
    use chacha20poly1305::{
        aead::{Aead, KeyInit, Payload},
        XChaCha20Poly1305, XNonce,
    };
    let cipher = XChaCha20Poly1305::new(&key.into());
    let nonce_bytes = [0x11u8; 24];
    let nonce = XNonce::from_slice(&nonce_bytes);

    let header = GraphjHeader {
        flags: FLAG_SEALED | FLAG_ENCRYPTED,
        compression: COMPRESSION_NONE,
        encryption: ENCRYPTION_XCHACHA20POLY1305,
        zstd_level: 0,
        first_seq: 1,
        last_seq: 100,
        entry_count: 100,
        body_len: data.len() as u64 + 16, // +16 for auth tag
        body_checksum: [0u8; 32],
        nonce: nonce_bytes,
        created_ms: 1700000000000,
    };

    let aad = &encode_header(&header)[..40];
    let ciphertext = cipher
        .encrypt(nonce, Payload { msg: &data, aad })
        .unwrap();

    group.throughput(Throughput::Bytes(ciphertext.len() as u64));

    group.bench_function("decrypt", |b| {
        b.iter(|| decode_body(black_box(&header), black_box(&ciphertext), Some(&key)).unwrap());
    });
    group.finish();
}

/// Benchmark compression + encryption combined.
fn bench_compress_encrypt(c: &mut Criterion) {
    let mut group = c.benchmark_group("compress_encrypt");
    let data = generate_cypher_queries(100);
    let key = [0x42u8; 32];
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_function("compress_then_encrypt", |b| {
        b.iter(|| {
            // Compress
            let compressed = zstd::encode_all(black_box(&data[..]), 3).unwrap();

            // Encrypt
            use chacha20poly1305::{
                aead::{Aead, KeyInit, Payload},
                XChaCha20Poly1305, XNonce,
            };
            let cipher = XChaCha20Poly1305::new(&key.into());
            let nonce_bytes = [0x11u8; 24];
            let nonce = XNonce::from_slice(&nonce_bytes);

            let header = GraphjHeader {
                flags: FLAG_SEALED | FLAG_ENCRYPTED,
                compression: COMPRESSION_ZSTD,
                encryption: ENCRYPTION_XCHACHA20POLY1305,
                zstd_level: 3,
                first_seq: 1,
                last_seq: 100,
                entry_count: 100,
                body_len: compressed.len() as u64 + 16,
                body_checksum: [0u8; 32],
                nonce: nonce_bytes,
                created_ms: 1700000000000,
            };

            let aad = &encode_header(&header)[..40];
            cipher
                .encrypt(nonce, Payload {
                    msg: &compressed,
                    aad,
                })
                .unwrap();
        });
    });
    group.finish();
}

/// Benchmark segment compaction (merging multiple files).
fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");

    // Create 5 temporary .graphj files with realistic data
    let dir = tempfile::tempdir().unwrap();
    let mut input_paths = Vec::new();
    for i in 0..5 {
        let path = dir.path().join(format!("segment-{:04}.graphj", i));
        let data = generate_cypher_queries(20); // ~2KB each

        // Write as unsealed .graphj
        let header = GraphjHeader {
            flags: FLAG_SEALED,
            compression: COMPRESSION_NONE,
            encryption: ENCRYPTION_NONE,
            zstd_level: 0,
            first_seq: i as u64 * 20 + 1,
            last_seq: (i as u64 + 1) * 20,
            entry_count: 20,
            body_len: data.len() as u64,
            body_checksum: {
                use sha2::{Digest, Sha256};
                Sha256::digest(&data).into()
            },
            nonce: [0u8; 24],
            created_ms: 1700000000000,
        };

        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(&encode_header(&header)).unwrap();
        file.write_all(&data).unwrap();
        drop(file);

        input_paths.push(path);
    }

    let output_path = dir.path().join("compacted.graphj");

    group.bench_function("compact_5_segments_no_compress", |b| {
        b.iter(|| {
            compact(
                black_box(&input_paths),
                &output_path,
                false,
                0,
                None,
            )
            .unwrap();
            std::fs::remove_file(&output_path).ok();
        });
    });

    group.bench_function("compact_5_segments_with_compress", |b| {
        b.iter(|| {
            compact(
                black_box(&input_paths),
                &output_path,
                true,
                3,
                None,
            )
            .unwrap();
            std::fs::remove_file(&output_path).ok();
        });
    });

    let key = [0x42u8; 32];
    group.bench_function("compact_5_segments_compress_encrypt", |b| {
        b.iter(|| {
            compact(
                black_box(&input_paths),
                &output_path,
                true,
                3,
                Some(&key),
            )
            .unwrap();
            std::fs::remove_file(&output_path).ok();
        });
    });

    group.finish();
}

/// Benchmark different data sizes.
fn bench_data_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_sizes");

    for size in [10, 100, 1000, 10000] {
        let data = generate_cypher_queries(size);
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(
            BenchmarkId::new("compress", data.len()),
            &data,
            |b, data| {
                b.iter(|| zstd::encode_all(black_box(&data[..]), 3).unwrap());
            },
        );

        let key = [0x42u8; 32];
        let header = GraphjHeader {
            flags: FLAG_SEALED | FLAG_ENCRYPTED,
            compression: COMPRESSION_NONE,
            encryption: ENCRYPTION_XCHACHA20POLY1305,
            zstd_level: 0,
            first_seq: 1,
            last_seq: size as u64,
            entry_count: size as u64,
            body_len: data.len() as u64,
            body_checksum: [0u8; 32],
            nonce: [0x11u8; 24],
            created_ms: 1700000000000,
        };

        group.bench_with_input(
            BenchmarkId::new("encrypt", data.len()),
            &data,
            |b, data| {
                b.iter(|| {
                    use chacha20poly1305::{
                        aead::{Aead, KeyInit, Payload},
                        XChaCha20Poly1305, XNonce,
                    };
                    let cipher = XChaCha20Poly1305::new(&key.into());
                    let nonce = XNonce::from_slice(&header.nonce);
                    let aad = &encode_header(&header)[..40];
                    cipher
                        .encrypt(nonce, Payload {
                            msg: black_box(data),
                            aad,
                        })
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_header_encode,
    bench_header_decode,
    bench_compression_levels,
    bench_decompression,
    bench_encryption,
    bench_decryption,
    bench_compress_encrypt,
    bench_compaction,
    bench_data_sizes,
);
criterion_main!(benches);
