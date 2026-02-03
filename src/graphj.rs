//! `.graphj` file format — universal journal file format for graphd.
//!
//! # Overview
//!
//! GraphJ (Graph Journal) is a container format for graphd's write-ahead journal.
//! One format is used for:
//! - Live journal segments (written entry-by-entry, fsynced)
//! - Compacted archives (multiple segments merged)
//! - S3/cold storage uploads
//! - Point-in-time restore inputs
//!
//! # Binary Format
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                        128-byte Fixed Header                         │
//! ├─────────┬──────┬────────────────────────────────────────────────────┤
//! │ Offset  │ Size │ Field                                               │
//! ├─────────┼──────┼────────────────────────────────────────────────────┤
//! │ 0-5     │ 6    │ magic             b"GRAPHJ"                         │
//! │ 6       │ 1    │ nul               0x00 separator                    │
//! │ 7       │ 1    │ version           0x01 (format version)             │
//! │ 8       │ 1    │ flags             SEALED | COMPRESSED | ENCRYPTED   │
//! │ 9       │ 1    │ compression       0=none, 1=zstd                    │
//! │ 10      │ 1    │ encryption        0=none, 1=xchacha20poly1305      │
//! │ 11      │ 1    │ reserved          0x00                              │
//! │ 12-15   │ 4    │ zstd_level        i32 LE (0 if uncompressed)       │
//! │ 16-23   │ 8    │ first_seq         u64 LE (first entry sequence)    │
//! │ 24-31   │ 8    │ last_seq          u64 LE (0 if unsealed)           │
//! │ 32-39   │ 8    │ entry_count       u64 LE (0 if unsealed)           │
//! │ 40-47   │ 8    │ body_len          u64 LE (0 if unsealed)           │
//! │ 48-79   │ 32   │ body_checksum     SHA-256 of body (zeros if unsealed) │
//! │ 80-103  │ 24   │ nonce             XChaCha20-Poly1305 nonce (zeros if unencrypted) │
//! │ 104-111 │ 8    │ created_ms        i64 LE (epoch milliseconds)      │
//! │ 112-127 │ 16   │ reserved          zeros (future use)                │
//! ├─────────┴──────┴────────────────────────────────────────────────────┤
//! │                        Variable-Length Body                          │
//! │    Raw journal entries (or compressed/encrypted transformation)      │
//! └──────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Flags (offset 8)
//!
//! - **SEALED (0x04)**: File is finalized. `last_seq`, `entry_count`, `body_len`,
//!   and `body_checksum` are valid. Unsealed files have these set to zero and
//!   readers scan to EOF.
//! - **COMPRESSED (0x01)**: Body is zstd-compressed. Only valid on sealed files.
//! - **ENCRYPTED (0x02)**: Body is XChaCha20-Poly1305 encrypted. Only valid on
//!   sealed files.
//!
//! # Body Format
//!
//! For **unsealed** files (live segments):
//! - Raw concatenated journal entries, each with structure:
//!   `[4B CRC32C LE][4B payload_len LE][8B sequence LE][32B prev_hash SHA256][protobuf payload]`
//! - Entries are written incrementally and fsynced periodically.
//!
//! For **sealed** files:
//! - Raw: same as unsealed
//! - Compressed: `zstd_compress(raw_entries)`
//! - Encrypted: `XChaCha20Poly1305::encrypt(nonce, key, AAD, optionally_compressed_entries)`
//!   - 16-byte Poly1305 auth tag is appended to ciphertext (included in `body_len`)
//!   - AAD = header bytes [0..40) — covers stable fields only
//!
//! # Processing Order
//!
//! **Encoding:** compress → encrypt
//! **Decoding:** decrypt → decompress
//!
//! # File Types
//!
//! 1. **Live segment** (unsealed, raw):
//!    - `flags = 0` (no SEALED, COMPRESSED, or ENCRYPTED)
//!    - Header written at creation with `first_seq`
//!    - Entries appended incrementally
//!    - On rotation/shutdown, header rewritten with SEALED + final values
//!
//! 2. **Compacted archive** (sealed, optionally compressed/encrypted):
//!    - Created by merging multiple segments via [`compact()`]
//!    - `flags = SEALED | optionally(COMPRESSED | ENCRYPTED)`
//!    - All header fields populated
//!    - Immutable
//!
//! # Backward Compatibility
//!
//! Legacy `.wal` files (raw entries, no header) are detected by checking for
//! the magic bytes. If the first 6 bytes don't match `b"GRAPHJ"`, the file is
//! treated as a legacy `.wal` and read from offset 0.
//!
//! # Security Properties
//!
//! - **Integrity**: SHA-256 body checksum + per-entry CRC32C + chain hashing
//! - **Authenticity**: AEAD encryption binds ciphertext to header via AAD
//! - **Confidentiality**: XChaCha20-Poly1305 with 192-bit nonces (safe for random generation)
//! - **Tamper detection**: Any modification to header AAD or ciphertext fails Poly1305 verification
//!
//! # Example Usage
//!
//! ```rust,ignore
//! // Create a compacted, compressed, encrypted archive
//! let key = [0x42u8; 32];
//! let inputs = ["journal-0000000000000001.graphj", "journal-0000000000000010.graphj"];
//! let header = compact(&inputs, "archive.graphj", true, 3, Some(&key))?;
//!
//! // Read it back
//! let mut file = File::open("archive.graphj")?;
//! let hdr = read_header(&mut file)?.unwrap();
//! let mut body = vec![0u8; hdr.body_len as usize];
//! file.read_exact(&mut body)?;
//! let raw_entries = decode_body(&hdr, &body, Some(&key))?;
//! ```

use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    XChaCha20Poly1305, XNonce,
};
use sha2::{Digest, Sha256};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

// ─── Constants ───

pub const MAGIC: &[u8; 6] = b"GRAPHJ";
pub const NUL: u8 = 0x00;
pub const VERSION: u8 = 0x01;
pub const HEADER_SIZE: usize = 128;
/// AAD covers header bytes [0..40) — magic, version, flags, algorithms,
/// first_seq, last_seq, entry_count. Excludes body_len and body_checksum
/// which aren't known until after encryption.
pub const AAD_LEN: usize = 40;

pub const FLAG_COMPRESSED: u8 = 0x01;
pub const FLAG_ENCRYPTED: u8 = 0x02;
pub const FLAG_SEALED: u8 = 0x04;

pub const COMPRESSION_NONE: u8 = 0;
pub const COMPRESSION_ZSTD: u8 = 1;

pub const ENCRYPTION_NONE: u8 = 0;
pub const ENCRYPTION_XCHACHA20POLY1305: u8 = 1;

// ─── Header ───

#[derive(Debug, Clone)]
pub struct GraphjHeader {
    pub flags: u8,
    pub compression: u8,
    pub encryption: u8,
    pub zstd_level: i32,
    pub first_seq: u64,
    pub last_seq: u64,
    pub entry_count: u64,
    pub body_len: u64,
    pub body_checksum: [u8; 32],
    pub nonce: [u8; 24],
    pub created_ms: i64,
}

impl GraphjHeader {
    /// Create an unsealed header for a new live segment.
    pub fn new_unsealed(first_seq: u64, created_ms: i64) -> Self {
        Self {
            flags: 0,
            compression: COMPRESSION_NONE,
            encryption: ENCRYPTION_NONE,
            zstd_level: 0,
            first_seq,
            last_seq: 0,
            entry_count: 0,
            body_len: 0,
            body_checksum: [0u8; 32],
            nonce: [0u8; 24],
            created_ms,
        }
    }

    pub fn is_sealed(&self) -> bool {
        self.flags & FLAG_SEALED != 0
    }

    pub fn is_compressed(&self) -> bool {
        self.flags & FLAG_COMPRESSED != 0
    }

    pub fn is_encrypted(&self) -> bool {
        self.flags & FLAG_ENCRYPTED != 0
    }
}

/// Encode a header into a 128-byte array.
pub fn encode_header(h: &GraphjHeader) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..6].copy_from_slice(MAGIC);
    buf[6] = NUL;
    buf[7] = VERSION;
    buf[8] = h.flags;
    buf[9] = h.compression;
    buf[10] = h.encryption;
    // buf[11] reserved
    buf[12..16].copy_from_slice(&h.zstd_level.to_le_bytes());
    buf[16..24].copy_from_slice(&h.first_seq.to_le_bytes());
    buf[24..32].copy_from_slice(&h.last_seq.to_le_bytes());
    buf[32..40].copy_from_slice(&h.entry_count.to_le_bytes());
    buf[40..48].copy_from_slice(&h.body_len.to_le_bytes());
    buf[48..80].copy_from_slice(&h.body_checksum);
    buf[80..104].copy_from_slice(&h.nonce);
    buf[104..112].copy_from_slice(&h.created_ms.to_le_bytes());
    // buf[112..128] reserved
    buf
}

/// Try to read and parse a `.graphj` header from a reader.
/// Returns `Ok(Some(header))` if magic matches, `Ok(None)` if it doesn't
/// (indicating a legacy `.wal` file). The reader is advanced by 128 bytes
/// on success, or left at an indeterminate position on `None` (caller
/// should seek back to 0).
pub fn read_header<R: Read>(r: &mut R) -> io::Result<Option<GraphjHeader>> {
    let mut buf = [0u8; HEADER_SIZE];
    r.read_exact(&mut buf)?;
    if &buf[0..6] != MAGIC {
        return Ok(None);
    }
    // version check
    let _version = buf[7];
    let flags = buf[8];
    let compression = buf[9];
    let encryption = buf[10];
    let zstd_level = i32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let first_seq = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    let last_seq = u64::from_le_bytes(buf[24..32].try_into().unwrap());
    let entry_count = u64::from_le_bytes(buf[32..40].try_into().unwrap());
    let body_len = u64::from_le_bytes(buf[40..48].try_into().unwrap());
    let mut body_checksum = [0u8; 32];
    body_checksum.copy_from_slice(&buf[48..80]);
    let mut nonce = [0u8; 24];
    nonce.copy_from_slice(&buf[80..104]);
    let created_ms = i64::from_le_bytes(buf[104..112].try_into().unwrap());

    Ok(Some(GraphjHeader {
        flags,
        compression,
        encryption,
        zstd_level,
        first_seq,
        last_seq,
        entry_count,
        body_len,
        body_checksum,
        nonce,
        created_ms,
    }))
}

// ─── Seal ───

/// Seal a live `.graphj` file: scan entries from offset 128, compute final
/// header fields, rewrite header with `FLAG_SEALED`.
pub fn seal_file(path: &Path) -> Result<GraphjHeader, String> {
    use std::fs::OpenOptions;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| format!("Failed to open file for sealing: {e}"))?;

    // Read existing header.
    let header = read_header(&mut file)
        .map_err(|e| format!("Failed to read header: {e}"))?
        .ok_or_else(|| "Not a .graphj file (magic mismatch)".to_string())?;

    if header.is_sealed() {
        return Ok(header);
    }

    // Scan body to compute last_seq, entry_count, body_len, body_checksum.
    let mut body_hasher = Sha256::new();
    let mut entry_count: u64 = 0;
    let mut last_seq: u64 = header.first_seq;
    let mut body_len: u64 = 0;

    loop {
        let mut entry_header = [0u8; 48]; // FIXED_HEADER from journal.rs
        match file.read_exact(&mut entry_header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("Error reading entry during seal: {e}")),
        }

        let payload_len =
            u32::from_le_bytes([entry_header[4], entry_header[5], entry_header[6], entry_header[7]])
                as usize;
        let sequence = u64::from_le_bytes(entry_header[8..16].try_into().unwrap());

        let mut payload = vec![0u8; payload_len];
        file.read_exact(&mut payload)
            .map_err(|e| format!("Truncated entry during seal at seq {sequence}: {e}"))?;

        // Hash the raw entry bytes.
        body_hasher.update(&entry_header);
        body_hasher.update(&payload);

        last_seq = sequence;
        entry_count += 1;
        body_len += (48 + payload_len) as u64;
    }

    let body_checksum: [u8; 32] = body_hasher.finalize().into();

    let sealed = GraphjHeader {
        flags: header.flags | FLAG_SEALED,
        last_seq,
        entry_count,
        body_len,
        body_checksum,
        ..header
    };

    // Rewrite header.
    file.seek(SeekFrom::Start(0))
        .map_err(|e| format!("Failed to seek for seal: {e}"))?;
    file.write_all(&encode_header(&sealed))
        .map_err(|e| format!("Failed to write sealed header: {e}"))?;
    file.sync_all()
        .map_err(|e| format!("Failed to sync sealed file: {e}"))?;

    Ok(sealed)
}

// ─── Decode body ───

/// Decrypt and/or decompress a sealed `.graphj` body, returning raw entry bytes.
pub fn decode_body(
    header: &GraphjHeader,
    body: &[u8],
    encryption_key: Option<&[u8; 32]>,
) -> Result<Vec<u8>, String> {
    let mut data = body.to_vec();

    // Decrypt first (reverse of compress-then-encrypt).
    if header.is_encrypted() {
        let key = encryption_key.ok_or("Encrypted .graphj file but no encryption key provided")?;
        let cipher = XChaCha20Poly1305::new(key.into());
        let nonce = XNonce::from_slice(&header.nonce);
        let aad = &encode_header(header)[..AAD_LEN];
        data = cipher
            .decrypt(nonce, Payload { msg: &data, aad })
            .map_err(|e| format!("Decryption failed: {e}"))?;
    }

    // Decompress.
    if header.is_compressed() {
        data = zstd::decode_all(data.as_slice())
            .map_err(|e| format!("Decompression failed: {e}"))?;
    }

    Ok(data)
}

// ─── Compact ───

/// Compact N input files into a single sealed `.graphj` file.
/// Reads raw entries from all inputs (handles both `.wal` and `.graphj`),
/// optionally compresses and encrypts, writes sealed output.
pub fn compact(
    inputs: &[impl AsRef<Path>],
    output: &Path,
    compress: bool,
    zstd_level: i32,
    encryption_key: Option<&[u8; 32]>,
) -> Result<GraphjHeader, String> {
    // Collect all raw entry bytes from input files.
    let mut raw_entries = Vec::new();
    let mut first_seq: u64 = u64::MAX;
    let mut last_seq: u64 = 0;
    let mut entry_count: u64 = 0;

    for input in inputs {
        let input = input.as_ref();
        let mut file = std::fs::File::open(input)
            .map_err(|e| format!("Failed to open {}: {e}", input.display()))?;

        // Detect format.
        let mut magic_buf = [0u8; 8];
        let bytes_read = file
            .read(&mut magic_buf)
            .map_err(|e| format!("Failed to read {}: {e}", input.display()))?;

        if bytes_read >= 6 && &magic_buf[0..6] == MAGIC {
            // .graphj file — read header, then body.
            file.seek(SeekFrom::Start(0))
                .map_err(|e| format!("Seek failed: {e}"))?;
            let hdr = read_header(&mut file)
                .map_err(|e| format!("Failed to read header from {}: {e}", input.display()))?
                .ok_or_else(|| "Magic check inconsistency".to_string())?;

            if hdr.is_sealed() && (hdr.is_compressed() || hdr.is_encrypted()) {
                // Need to decode the body first.
                let mut body = vec![0u8; hdr.body_len as usize];
                file.read_exact(&mut body)
                    .map_err(|e| format!("Failed to read sealed body: {e}"))?;
                let decoded = decode_body(&hdr, &body, encryption_key)?;
                raw_entries.extend_from_slice(&decoded);
            } else {
                // Unsealed or sealed-raw: read entries directly.
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)
                    .map_err(|e| format!("Failed to read entries: {e}"))?;
                raw_entries.extend_from_slice(&buf);
            }
        } else {
            // Legacy .wal — entire file is raw entries.
            file.seek(SeekFrom::Start(0))
                .map_err(|e| format!("Seek failed: {e}"))?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .map_err(|e| format!("Failed to read .wal: {e}"))?;
            raw_entries.extend_from_slice(&buf);
        }
    }

    // Scan raw entries to get first_seq, last_seq, entry_count.
    {
        let mut cursor = io::Cursor::new(&raw_entries);
        loop {
            let mut hdr = [0u8; 48];
            match cursor.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(format!("Error scanning entries: {e}")),
            }
            let payload_len =
                u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as usize;
            let seq = u64::from_le_bytes(hdr[8..16].try_into().unwrap());

            // Skip payload.
            let pos = cursor.position() + payload_len as u64;
            cursor.set_position(pos);

            if seq < first_seq {
                first_seq = seq;
            }
            if seq > last_seq {
                last_seq = seq;
            }
            entry_count += 1;
        }
    }

    if entry_count == 0 {
        first_seq = 0;
    }

    // Process body: compress then encrypt.
    let mut body = raw_entries;

    let compression_algo = if compress {
        body = zstd::encode_all(body.as_slice(), zstd_level)
            .map_err(|e| format!("Compression failed: {e}"))?;
        COMPRESSION_ZSTD
    } else {
        COMPRESSION_NONE
    };

    let mut flags = FLAG_SEALED;
    if compress {
        flags |= FLAG_COMPRESSED;
    }

    let mut nonce = [0u8; 24];
    let encryption_algo;

    if let Some(key) = encryption_key {
        flags |= FLAG_ENCRYPTED;
        encryption_algo = ENCRYPTION_XCHACHA20POLY1305;

        // Generate random nonce.
        use rand::RngCore;
        rand::rng().fill_bytes(&mut nonce);

        // Build AAD from stable header fields (offsets 0..40).
        let tmp_header = GraphjHeader {
            flags,
            compression: compression_algo,
            encryption: encryption_algo,
            zstd_level: if compress { zstd_level } else { 0 },
            first_seq,
            last_seq,
            entry_count,
            body_len: 0,
            body_checksum: [0u8; 32],
            nonce,
            created_ms: crate::journal::current_timestamp_ms(),
        };
        let header_bytes = encode_header(&tmp_header);
        let aad = &header_bytes[..AAD_LEN];

        let cipher = XChaCha20Poly1305::new(key.into());
        let nonce_ref = XNonce::from_slice(&nonce);
        body = cipher
            .encrypt(nonce_ref, Payload { msg: &body, aad })
            .map_err(|e| format!("Encryption failed: {e}"))?;
    } else {
        encryption_algo = ENCRYPTION_NONE;
    }

    let body_checksum: [u8; 32] = Sha256::digest(&body).into();
    let body_len = body.len() as u64;

    let header = GraphjHeader {
        flags,
        compression: compression_algo,
        encryption: encryption_algo,
        zstd_level: if compress { zstd_level } else { 0 },
        first_seq,
        last_seq,
        entry_count,
        body_len,
        body_checksum,
        nonce,
        created_ms: crate::journal::current_timestamp_ms(),
    };

    // Write output.
    let mut out = std::fs::File::create(output)
        .map_err(|e| format!("Failed to create output file: {e}"))?;
    out.write_all(&encode_header(&header))
        .map_err(|e| format!("Failed to write header: {e}"))?;
    out.write_all(&body)
        .map_err(|e| format!("Failed to write body: {e}"))?;
    out.sync_all()
        .map_err(|e| format!("Failed to sync output: {e}"))?;

    Ok(header)
}

// ─── Tests ───

#[cfg(test)]
mod tests {
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
                params: None,
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
            params: None,
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
            params: None,
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
}
