use crate::graphd as proto;
use prost::Message;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info};

// ─── Mutation detection ───

const MUTATION_KEYWORDS: &[&str] = &[
    "CREATE", "MERGE", "DELETE", "DROP", "ALTER", "COPY", "SET",
];

/// Returns true if the query likely contains a mutation keyword.
/// Over-journaling reads is harmless; missing mutations would be catastrophic.
pub fn is_mutation(query: &str) -> bool {
    let upper = query.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    for keyword in MUTATION_KEYWORDS {
        let kw_bytes = keyword.as_bytes();
        let kw_len = kw_bytes.len();
        let mut i = 0;
        while i + kw_len <= bytes.len() {
            if &bytes[i..i + kw_len] == kw_bytes {
                let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                let after_ok =
                    i + kw_len >= bytes.len() || !bytes[i + kw_len].is_ascii_alphanumeric();
                if before_ok && after_ok {
                    return true;
                }
            }
            i += 1;
        }
    }
    false
}

// ─── Param conversion (ParamValue ↔ protobuf, no JSON) ───

use crate::values::ParamValue;

fn param_value_to_graph_value(v: &ParamValue) -> proto::GraphValue {
    use proto::graph_value::Value;
    let value = match v {
        ParamValue::Null => Value::NullValue(proto::NullValue {}),
        ParamValue::Bool(b) => Value::BoolValue(*b),
        ParamValue::Int(i) => Value::IntValue(*i),
        ParamValue::Float(f) => Value::FloatValue(*f),
        ParamValue::String(s) => Value::StringValue(s.clone()),
        ParamValue::List(items) => Value::ListValue(proto::ListValue {
            values: items.iter().map(param_value_to_graph_value).collect(),
        }),
    };
    proto::GraphValue { value: Some(value) }
}

pub fn param_values_to_map_entries(params: &[(String, ParamValue)]) -> Vec<proto::MapEntry> {
    params
        .iter()
        .map(|(k, v)| proto::MapEntry {
            key: k.clone(),
            value: Some(param_value_to_graph_value(v)),
        })
        .collect()
}

fn graph_value_to_param_value(gv: &proto::GraphValue) -> ParamValue {
    match gv.value.as_ref() {
        Some(proto::graph_value::Value::NullValue(_)) | None => ParamValue::Null,
        Some(proto::graph_value::Value::BoolValue(b)) => ParamValue::Bool(*b),
        Some(proto::graph_value::Value::IntValue(i)) => ParamValue::Int(*i),
        Some(proto::graph_value::Value::FloatValue(f)) => ParamValue::Float(*f),
        Some(proto::graph_value::Value::StringValue(s)) => ParamValue::String(s.clone()),
        Some(proto::graph_value::Value::ListValue(list)) => {
            ParamValue::List(list.values.iter().map(graph_value_to_param_value).collect())
        }
        // Map values in proto → flatten to string fallback (params don't use maps).
        _ => ParamValue::Null,
    }
}

pub fn map_entries_to_param_values(entries: &[proto::MapEntry]) -> Vec<(String, ParamValue)> {
    entries
        .iter()
        .map(|e| {
            let value = e
                .value
                .as_ref()
                .map_or(ParamValue::Null, graph_value_to_param_value);
            (e.key.clone(), value)
        })
        .collect()
}

// ─── Types ───

pub struct PendingEntry {
    pub query: String,
    pub params: Vec<(String, ParamValue)>,
}

#[allow(dead_code)]
pub enum JournalCommand {
    Write(PendingEntry),
    Flush(std::sync::mpsc::SyncSender<()>),
    /// Force-seal the current segment so it can be uploaded to S3.
    /// Returns the path of the sealed segment, or None if no active segment.
    SealForUpload(std::sync::mpsc::SyncSender<Option<std::path::PathBuf>>),
    Shutdown,
}

pub type JournalSender = std::sync::mpsc::Sender<JournalCommand>;

pub struct JournalState {
    pub sequence: AtomicU64,
    pub chain_hash: Mutex<[u8; 32]>,
    /// Set to false when the journal writer thread exits (crash or shutdown).
    pub alive: AtomicBool,
}

impl JournalState {
    pub fn with_sequence_and_hash(seq: u64, hash: [u8; 32]) -> Self {
        Self {
            sequence: AtomicU64::new(seq),
            chain_hash: Mutex::new(hash),
            alive: AtomicBool::new(true),
        }
    }

    /// Returns true if the journal writer thread is still running.
    #[allow(dead_code)]
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }
}

pub fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ─── Binary format ───
// [4B CRC32C le][4B payload_len le][8B sequence le][32B prev_hash][protobuf payload]
// CRC32C covers everything after itself (bytes 4..).

const FIXED_HEADER: usize = 4 + 4 + 8 + 32; // 48 bytes

fn encode_entry(seq: u64, prev_hash: &[u8; 32], payload: &[u8]) -> Vec<u8> {
    let total = FIXED_HEADER + payload.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&[0u8; 4]); // CRC32C placeholder
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(prev_hash);
    buf.extend_from_slice(payload);
    let crc = crc32c::crc32c(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    buf
}

struct DecodedEntry {
    sequence: u64,
    prev_hash: [u8; 32],
    payload: Vec<u8>,
}

fn read_entry_from<R: Read>(reader: &mut R) -> Result<Option<DecodedEntry>, String> {
    // Read fixed header (48 bytes).
    let mut header = [0u8; FIXED_HEADER];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(format!("Failed to read journal header: {e}")),
    }

    let stored_crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let payload_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
    let sequence = u64::from_le_bytes([
        header[8], header[9], header[10], header[11], header[12], header[13], header[14],
        header[15],
    ]);
    let mut prev_hash = [0u8; 32];
    prev_hash.copy_from_slice(&header[16..48]);

    let mut payload = vec![0u8; payload_len];
    reader
        .read_exact(&mut payload)
        .map_err(|e| format!("Truncated journal payload at seq {sequence}: {e}"))?;

    // Validate CRC32C: covers bytes[4..] of the full entry.
    let mut crc_buf = Vec::with_capacity(44 + payload_len);
    crc_buf.extend_from_slice(&header[4..]);
    crc_buf.extend_from_slice(&payload);
    let computed_crc = crc32c::crc32c(&crc_buf);
    if computed_crc != stored_crc {
        return Err(format!(
            "CRC32C mismatch at seq {sequence}: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
        ));
    }

    Ok(Some(DecodedEntry {
        sequence,
        prev_hash,
        payload,
    }))
}

// ─── Writer ───

pub fn spawn_journal_writer(
    journal_dir: PathBuf,
    segment_max_bytes: u64,
    fsync_ms: u64,
    state: Arc<JournalState>,
) -> JournalSender {
    std::fs::create_dir_all(&journal_dir).expect("Failed to create journal directory");

    let (tx, rx) = std::sync::mpsc::channel::<JournalCommand>();

    std::thread::Builder::new()
        .name("journal-writer".into())
        .spawn(move || {
            writer_loop(rx, &journal_dir, segment_max_bytes, fsync_ms, &state);
            // Mark the writer as dead on exit (normal or panic).
            state.alive.store(false, Ordering::SeqCst);
        })
        .expect("Failed to spawn journal writer thread");

    tx
}

/// Zstd compression level for sealed journal segments.
const JOURNAL_ZSTD_LEVEL: i32 = 3;

/// Seal a .graphj segment: compress body with zstd, rewrite header with
/// SEALED + COMPRESSED flags + final values.
fn seal_segment(
    file: &mut File,
    first_seq: u64,
    last_seq: u64,
    entry_count: u64,
    _body_len: u64,
    _body_hasher: Sha256,
    created_ms: i64,
) -> io::Result<()> {
    // Read the raw body (everything after the 128-byte header).
    let file_len = file.seek(SeekFrom::End(0))?;
    let raw_body_len = file_len - crate::graphj::HEADER_SIZE as u64;
    file.seek(SeekFrom::Start(crate::graphj::HEADER_SIZE as u64))?;
    let mut raw_body = vec![0u8; raw_body_len as usize];
    file.read_exact(&mut raw_body)?;

    // Compress with zstd.
    let compressed = zstd::encode_all(raw_body.as_slice(), JOURNAL_ZSTD_LEVEL)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("zstd compress: {e}")))?;

    // Checksum of the compressed body (what's stored on disk).
    let mut hasher = Sha256::new();
    hasher.update(&compressed);
    let body_checksum: [u8; 32] = hasher.finalize().into();

    let header = crate::graphj::GraphjHeader {
        flags: crate::graphj::FLAG_SEALED | crate::graphj::FLAG_COMPRESSED,
        compression: crate::graphj::COMPRESSION_ZSTD,
        encryption: crate::graphj::ENCRYPTION_NONE,
        zstd_level: JOURNAL_ZSTD_LEVEL,
        first_seq,
        last_seq,
        entry_count,
        body_len: compressed.len() as u64,
        body_checksum,
        nonce: [0u8; 24],
        created_ms,
    };

    // Rewrite: header + compressed body, then truncate.
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&crate::graphj::encode_header(&header))?;
    file.write_all(&compressed)?;
    file.set_len(crate::graphj::HEADER_SIZE as u64 + compressed.len() as u64)?;
    file.sync_all()?;
    Ok(())
}

fn writer_loop(
    rx: std::sync::mpsc::Receiver<JournalCommand>,
    journal_dir: &Path,
    segment_max_bytes: u64,
    fsync_ms: u64,
    state: &JournalState,
) {
    let mut current_file: Option<File> = None;
    let mut current_size: u64 = 0;
    let mut segment_first_seq: u64 = 0;
    let mut segment_last_seq: u64 = 0;
    let mut segment_entry_count: u64 = 0;
    let mut body_hasher: Option<Sha256> = None;
    let timeout = Duration::from_millis(fsync_ms);

    loop {
        let cmd = rx.recv_timeout(timeout);
        match cmd {
            Ok(JournalCommand::Write(entry)) => {
                // Hold chain lock for the entire compute + write + update cycle.
                let mut chain = state.chain_hash.lock().unwrap_or_else(|e| e.into_inner());
                let prev_hash = *chain;
                let new_seq = state.sequence.load(Ordering::SeqCst) + 1;

                let proto_entry = proto::JournalEntry {
                    sequence: new_seq,
                    query: entry.query,
                    params: param_values_to_map_entries(&entry.params),
                    timestamp_ms: current_timestamp_ms(),
                };
                let payload = proto_entry.encode_to_vec();

                let mut hasher = Sha256::new();
                hasher.update(prev_hash);
                hasher.update(&payload);
                let new_hash: [u8; 32] = hasher.finalize().into();

                let buf = encode_entry(new_seq, &prev_hash, &payload);

                // Rotate segment if needed.
                if current_file.is_none()
                    || current_size + buf.len() as u64 > segment_max_bytes
                {
                    // Seal the old segment before rotation.
                    if let Some(ref mut f) = current_file {
                        if let Some(hasher) = body_hasher.take() {
                            let body_len = current_size - crate::graphj::HEADER_SIZE as u64;
                            if let Err(e) = seal_segment(
                                f,
                                segment_first_seq,
                                segment_last_seq,
                                segment_entry_count,
                                body_len,
                                hasher,
                                current_timestamp_ms(),
                            ) {
                                error!("Failed to seal segment on rotation: {e}");
                            }
                        }
                    }

                    // Create new .graphj segment (read+write so seal_segment can read body).
                    let path = journal_dir.join(format!("journal-{new_seq:016}.graphj"));
                    match File::options().read(true).write(true).create(true).truncate(true).open(&path) {
                        Ok(mut f) => {
                            // Write unsealed header.
                            let header = crate::graphj::GraphjHeader::new_unsealed(
                                new_seq,
                                current_timestamp_ms(),
                            );
                            if let Err(e) = f.write_all(&crate::graphj::encode_header(&header)) {
                                error!("Failed to write .graphj header: {e}");
                                continue;
                            }

                            info!("Journal: opened segment {}", path.display());
                            current_file = Some(f);
                            current_size = crate::graphj::HEADER_SIZE as u64;
                            segment_first_seq = new_seq;
                            segment_last_seq = 0;
                            segment_entry_count = 0;
                            body_hasher = Some(Sha256::new());
                        }
                        Err(e) => {
                            error!("Failed to create journal segment: {e}");
                            continue;
                        }
                    }
                }

                let file = current_file.as_mut().unwrap();

                // Shared lock already held from file creation; write safely.
                if let Err(e) = file.write_all(&buf) {
                    error!("Journal write error at seq {new_seq}: {e}");
                    // State NOT updated — next write retries with same sequence.
                    continue;
                }

                // Update body hasher with entry bytes.
                if let Some(ref mut hasher) = body_hasher {
                    hasher.update(&buf);
                }

                // Only update state after successful write.
                state.sequence.store(new_seq, Ordering::SeqCst);
                *chain = new_hash;
                drop(chain);
                current_size += buf.len() as u64;
                segment_last_seq = new_seq;
                segment_entry_count += 1;
            }
            Ok(JournalCommand::Flush(ack)) => {
                if let Some(ref mut f) = current_file {
                    let _ = f.sync_all();
                }
                let _ = ack.send(());
            }
            Ok(JournalCommand::SealForUpload(ack)) => {
                if let Some(ref mut f) = current_file {
                    if let Some(hasher) = body_hasher.take() {
                        let body_len = current_size - crate::graphj::HEADER_SIZE as u64;
                        if let Err(e) = seal_segment(
                            f,
                            segment_first_seq,
                            segment_last_seq,
                            segment_entry_count,
                            body_len,
                            hasher,
                            current_timestamp_ms(),
                        ) {
                            error!("Failed to seal segment for upload: {e}");
                            let _ = ack.send(None);
                            continue;
                        }
                        let path = journal_dir.join(format!(
                            "journal-{:016}.graphj",
                            segment_first_seq
                        ));
                        info!("Journal: sealed segment for upload: {}", path.display());
                        let _ = ack.send(Some(path));
                        // Clear current file so next write opens a new segment.
                        current_file = None;
                        current_size = 0;
                        segment_first_seq = 0;
                        segment_last_seq = 0;
                        segment_entry_count = 0;
                    } else {
                        // No hasher means segment was already sealed or empty header only.
                        let _ = ack.send(None);
                    }
                } else {
                    let _ = ack.send(None);
                }
            }
            Ok(JournalCommand::Shutdown) => {
                if let Some(ref mut f) = current_file {
                    if let Some(hasher) = body_hasher.take() {
                        let body_len = current_size - crate::graphj::HEADER_SIZE as u64;
                        let _ = seal_segment(
                            f,
                            segment_first_seq,
                            segment_last_seq,
                            segment_entry_count,
                            body_len,
                            hasher,
                            current_timestamp_ms(),
                        );
                    }
                }
                break;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Some(ref mut f) = current_file {
                    let _ = f.sync_data();
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                if let Some(ref mut f) = current_file {
                    if let Some(hasher) = body_hasher.take() {
                        let body_len = current_size - crate::graphj::HEADER_SIZE as u64;
                        let _ = seal_segment(
                            f,
                            segment_first_seq,
                            segment_last_seq,
                            segment_entry_count,
                            body_len,
                            hasher,
                            current_timestamp_ms(),
                        );
                    }
                }
                break;
            }
        }
    }
}

// ─── Reader ───

#[allow(dead_code)]
pub struct JournalReaderEntry {
    pub sequence: u64,
    pub entry: proto::JournalEntry,
    pub chain_hash: [u8; 32],
}

pub struct JournalReader {
    segments: Vec<PathBuf>,
    seg_idx: usize,
    reader: Option<BufReader<File>>,
    /// For sealed+compressed/encrypted .graphj files, we decode the body into
    /// memory and read entries from a cursor instead of the file.
    cursor_reader: Option<io::Cursor<Vec<u8>>>,
    running_hash: [u8; 32],
    start_seq: u64,
    encryption_key: Option<[u8; 32]>,
}

impl JournalReader {
    /// Open a journal reader starting from the beginning.
    #[allow(dead_code)]
    pub fn open(journal_dir: &Path) -> Result<Self, String> {
        Self::from_sequence(journal_dir, 0, [0u8; 32])
    }

    /// Open a journal reader with an encryption key for sealed .graphj files.
    #[allow(dead_code)]
    pub fn open_with_key(
        journal_dir: &Path,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<Self, String> {
        Self::from_sequence_with_key(journal_dir, 0, [0u8; 32], encryption_key)
    }

    /// Open a journal reader starting from a given sequence.
    /// Entries before `start_seq` are read and validated but not returned.
    pub fn from_sequence(
        journal_dir: &Path,
        start_seq: u64,
        initial_hash: [u8; 32],
    ) -> Result<Self, String> {
        Self::from_sequence_with_key(journal_dir, start_seq, initial_hash, None)
    }

    /// Open a journal reader starting from a given sequence with optional encryption key.
    pub fn from_sequence_with_key(
        journal_dir: &Path,
        start_seq: u64,
        initial_hash: [u8; 32],
        encryption_key: Option<[u8; 32]>,
    ) -> Result<Self, String> {
        let mut segments: Vec<PathBuf> = std::fs::read_dir(journal_dir)
            .map_err(|e| format!("Failed to read journal dir: {e}"))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .map_or(false, |ext| ext == "wal" || ext == "graphj")
                    && p.file_name()
                        .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
            })
            .collect();
        segments.sort();
        Ok(Self {
            segments,
            seg_idx: 0,
            reader: None,
            cursor_reader: None,
            running_hash: initial_hash,
            start_seq,
            encryption_key,
        })
    }
}

impl JournalReader {
    /// Process a decoded entry: validate chain, compute hash, decode protobuf.
    /// Returns Some(Ok(entry)) to yield, Some(Err) on error, None to skip (pre-start).
    fn process_entry(&mut self, decoded: DecodedEntry) -> Option<Result<JournalReaderEntry, String>> {
        if decoded.sequence < self.start_seq {
            let mut hasher = Sha256::new();
            hasher.update(decoded.prev_hash);
            hasher.update(&decoded.payload);
            self.running_hash = hasher.finalize().into();
            return None; // skip, keep iterating
        }

        if decoded.prev_hash != self.running_hash {
            return Some(Err(format!(
                "Chain hash mismatch at seq {}: expected {:x?}, got {:x?}",
                decoded.sequence,
                &self.running_hash[..4],
                &decoded.prev_hash[..4]
            )));
        }

        let mut hasher = Sha256::new();
        hasher.update(decoded.prev_hash);
        hasher.update(&decoded.payload);
        let new_hash: [u8; 32] = hasher.finalize().into();
        self.running_hash = new_hash;

        match proto::JournalEntry::decode(decoded.payload.as_slice()) {
            Ok(entry) => Some(Ok(JournalReaderEntry {
                sequence: decoded.sequence,
                entry,
                chain_hash: new_hash,
            })),
            Err(e) => Some(Err(format!(
                "Failed to decode journal entry at seq {}: {e}",
                decoded.sequence
            ))),
        }
    }

    /// Open a segment file, detecting format (.graphj vs legacy .wal).
    /// For sealed+compressed/encrypted .graphj, decodes body into cursor_reader.
    /// For raw .graphj or legacy .wal, sets up file reader at the right offset.
    fn open_segment(&mut self, path: &Path) -> Result<(), String> {
        let mut file = File::open(path)
            .map_err(|e| format!("Failed to open segment {}: {e}", path.display()))?;

        // Check magic bytes to detect format (only need first 6 bytes).
        let mut magic_buf = [0u8; 6];
        let is_graphj = match file.read_exact(&mut magic_buf) {
            Ok(()) => &magic_buf == crate::graphj::MAGIC,
            Err(_) => false, // file too small, treat as legacy
        };

        // Seek back and re-read properly.
        file.seek(SeekFrom::Start(0))
            .map_err(|e| format!("Failed to seek: {e}"))?;

        if is_graphj {
            let hdr = crate::graphj::read_header(&mut file)
                .map_err(|e| format!("Failed to read .graphj header: {e}"))?
                .ok_or_else(|| "Magic check inconsistency".to_string())?;

            if hdr.is_sealed() && (hdr.is_compressed() || hdr.is_encrypted()) {
                // Decode entire body into memory.
                let mut body = vec![0u8; hdr.body_len as usize];
                file.read_exact(&mut body)
                    .map_err(|e| format!("Failed to read sealed body: {e}"))?;
                let raw = crate::graphj::decode_body(
                    &hdr,
                    &body,
                    self.encryption_key.as_ref(),
                )?;
                self.cursor_reader = Some(io::Cursor::new(raw));
                self.reader = None;
            } else {
                // Unsealed or sealed-raw: reader is at offset 128 after read_header.
                self.reader = Some(BufReader::new(file));
                self.cursor_reader = None;
            }
        } else {
            // Legacy .wal: already at offset 0, read raw entries.
            self.reader = Some(BufReader::new(file));
            self.cursor_reader = None;
        }
        Ok(())
    }
}

impl Iterator for JournalReader {
    type Item = Result<JournalReaderEntry, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try reading from cursor (for decoded sealed .graphj files).
            if let Some(ref mut cursor) = self.cursor_reader {
                match read_entry_from(cursor) {
                    Ok(Some(decoded)) => {
                        match self.process_entry(decoded) {
                            Some(result) => return Some(result),
                            None => continue, // pre-start, skip
                        }
                    }
                    Ok(None) => {
                        self.cursor_reader = None;
                        self.seg_idx += 1;
                        continue;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Try reading from file reader.
            if let Some(ref mut reader) = self.reader {
                match read_entry_from(reader) {
                    Ok(Some(decoded)) => {
                        match self.process_entry(decoded) {
                            Some(result) => return Some(result),
                            None => continue, // pre-start, skip
                        }
                    }
                    Ok(None) => {
                        self.reader = None;
                        self.seg_idx += 1;
                        continue;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // No active reader — open next segment.
            if self.seg_idx >= self.segments.len() {
                return None;
            }
            let path = self.segments[self.seg_idx].clone();
            match self.open_segment(&path) {
                Ok(()) => {
                    // If open_segment set neither reader, skip this segment.
                    if self.reader.is_none() && self.cursor_reader.is_none() {
                        self.seg_idx += 1;
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// ─── Recovery ───

/// Recover the journal state (last sequence + chain hash) by scanning the last
/// segment on disk. Used on startup to continue the chain correctly.
/// Returns (0, [0u8; 32]) if the journal directory is empty or doesn't exist.
pub fn recover_journal_state(journal_dir: &Path) -> Result<(u64, [u8; 32]), String> {
    if !journal_dir.exists() {
        return Ok((0, [0u8; 32]));
    }

    let mut segments: Vec<PathBuf> = std::fs::read_dir(journal_dir)
        .map_err(|e| format!("Failed to read journal dir: {e}"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .map_or(false, |ext| ext == "wal" || ext == "graphj")
                && p.file_name()
                    .map_or(false, |n| n.to_string_lossy().starts_with("journal-"))
        })
        .collect();

    if segments.is_empty() {
        return Ok((0, [0u8; 32]));
    }

    segments.sort();

    // We need to compute the chain hash from the beginning because each entry's
    // hash depends on prev_hash. Read ALL segments to recover the full chain.
    let mut last_seq: u64 = 0;
    let mut running_hash = [0u8; 32];

    for segment_path in &segments {
        let mut file = File::open(segment_path)
            .map_err(|e| format!("Failed to open segment {}: {e}", segment_path.display()))?;

        // Detect .graphj format and read header.
        let hdr_opt = match crate::graphj::read_header(&mut file) {
            Ok(hdr) => hdr, // reader is now at offset 128 if Some
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                error!(
                    "Skipping corrupted .graphj file (truncated header): {}",
                    segment_path.display()
                );
                continue;
            }
            Err(e) => return Err(format!("Failed to read {}: {e}", segment_path.display())),
        };

        if let Some(ref hdr) = hdr_opt {
            if hdr.is_sealed() && hdr.is_compressed() {
                // Compressed segment: decode body into memory, read entries from cursor.
                let mut body = vec![0u8; hdr.body_len as usize];
                file.read_exact(&mut body)
                    .map_err(|e| format!("Failed to read compressed body: {e}"))?;
                let raw = crate::graphj::decode_body(hdr, &body, None)?;
                let mut cursor = io::Cursor::new(raw);
                loop {
                    match read_entry_from(&mut cursor) {
                        Ok(Some(decoded)) => {
                            let mut hasher = Sha256::new();
                            hasher.update(decoded.prev_hash);
                            hasher.update(&decoded.payload);
                            running_hash = hasher.finalize().into();
                            last_seq = decoded.sequence;
                        }
                        Ok(None) => break,
                        Err(e) => return Err(format!("Error reading journal during recovery: {e}")),
                    }
                }
                continue;
            }
        } else {
            // Legacy .wal — seek back to 0.
            file.seek(SeekFrom::Start(0))
                .map_err(|e| format!("Failed to seek: {e}"))?;
        }

        // Unsealed .graphj or legacy .wal: read entries directly from file.
        let mut reader = BufReader::new(file);
        loop {
            match read_entry_from(&mut reader) {
                Ok(Some(decoded)) => {
                    let mut hasher = Sha256::new();
                    hasher.update(decoded.prev_hash);
                    hasher.update(&decoded.payload);
                    running_hash = hasher.finalize().into();
                    last_seq = decoded.sequence;
                }
                Ok(None) => break, // EOF
                Err(e) => return Err(format!("Error reading journal during recovery: {e}")),
            }
        }
    }

    Ok((last_seq, running_hash))
}

// ─── Tests ───

#[cfg(test)]
mod tests {
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
}
