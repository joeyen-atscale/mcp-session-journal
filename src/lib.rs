//! `mcp-session-journal` — durable, append-only episodic log of MCP investigation sessions.
//!
//! Every mutation to a session state is snapshotted to a JSONL journal file.
//! Any session can be loaded from the journal at startup or by the replayer.
//!
//! # File layout
//! `<dir>/<session_id>.jsonl` — one file per session, one JSON object per line.
//! Rotation: when a journal file exceeds `max_file_size_bytes`, subsequent writes
//! go to `<session_id>.jsonl.1`, `.2`, etc. `load_session` reads all rotation
//! fragments in order.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Inline simplified versions of mcp-session-state types
// (kept standalone; using serde_json::Value for inner fields)
// ---------------------------------------------------------------------------

/// Simplified stand-in for `MqoHistoryEntry` from mcp-session-state.
/// Uses `serde_json::Value` for the inner MQO payload to remain standalone.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MqoHistoryEntry {
    pub query_id: String,
    pub timestamp_ms: u64,
    pub touched_entities: Vec<String>,
    pub payload: Value,
}

/// Simplified stand-in for `DatasetSummary` from mcp-session-state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatasetSummary {
    pub dataset_name: String,
    pub row_count: Option<u64>,
    pub columns: Vec<String>,
    pub metadata: Value,
}

/// Reconstructed in-memory session state produced by `load_session`.
#[derive(Debug, Clone, Default)]
pub struct SessionState {
    pub session_id: String,
    pub created_ms: u64,
    pub ttl_secs: u64,
    pub mqo_history: Vec<MqoHistoryEntry>,
    pub touched_entities: Vec<String>,
    pub handles: HashMap<String, DatasetSummary>,
    pub expired: bool,
}

// ---------------------------------------------------------------------------
// Core journal types
// ---------------------------------------------------------------------------

/// Configuration for the session journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalConfig {
    /// Base directory for journal files (e.g., `~/.local/share/mcp-sessions/`).
    pub dir: PathBuf,
    /// Rotate when journal file exceeds this size in bytes (default: 10 MB).
    pub max_file_size_bytes: u64,
}

impl JournalConfig {
    /// Create a config with the given directory and the default 10 MB rotation limit.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_file_size_bytes: 10 * 1024 * 1024,
        }
    }
}

/// A single persisted event in the session journal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JournalEntry {
    pub timestamp_ms: u64,
    pub session_id: String,
    pub event: JournalEvent,
}

/// The set of events that can be recorded for a session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JournalEvent {
    SessionCreated { ttl_secs: u64 },
    QueryRecorded { entry: MqoHistoryEntry },
    HandleAdded { handle_id: String, summary_snapshot: DatasetSummary },
    SessionExpired,
}

/// Lightweight summary of a persisted session, returned by `list_sessions`.
#[derive(Debug, Clone)]
pub struct JournalSessionSummary {
    pub session_id: String,
    pub created_ms: u64,
    pub last_event_ms: u64,
    pub query_count: usize,
    pub handle_count: usize,
}

// ---------------------------------------------------------------------------
// SessionJournal implementation
// ---------------------------------------------------------------------------

/// The main handle for reading and writing the session journal.
pub struct SessionJournal {
    config: JournalConfig,
}

impl SessionJournal {
    /// Create (or open) a journal, ensuring the directory exists.
    pub fn new(config: JournalConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.dir)?;
        Ok(Self { config })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Base path for a session's primary journal file.
    fn journal_path(&self, session_id: &str) -> PathBuf {
        self.config.dir.join(format!("{}.jsonl", session_id))
    }

    /// All rotation fragments for a session in order: base, .1, .2, …
    fn rotation_paths(&self, session_id: &str) -> Vec<PathBuf> {
        let base = self.journal_path(session_id);
        if !base.exists() {
            return vec![];
        }
        let mut paths = vec![base.clone()];
        let mut idx = 1u32;
        loop {
            let rotated = self
                .config
                .dir
                .join(format!("{}.jsonl.{}", session_id, idx));
            if rotated.exists() {
                paths.push(rotated);
                idx += 1;
            } else {
                break;
            }
        }
        paths
    }

    /// The active write path: the highest-numbered fragment that still has
    /// room, or a new fragment if the current one is full.
    fn active_write_path(&self, session_id: &str) -> io::Result<PathBuf> {
        let base = self.journal_path(session_id);
        if !base.exists() {
            return Ok(base);
        }
        // Walk existing rotation fragments, newest last.
        let mut idx: u32 = 1;
        let mut current = base.clone();
        loop {
            let meta = fs::metadata(&current)?;
            if meta.len() < self.config.max_file_size_bytes {
                return Ok(current);
            }
            let next = self
                .config
                .dir
                .join(format!("{}.jsonl.{}", session_id, idx));
            if next.exists() {
                current = next;
                idx += 1;
            } else {
                // Current fragment is full; next fragment doesn't exist yet.
                return Ok(next);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Append one `JournalEntry` to the journal, creating the file if needed.
    /// Rotates to a new fragment when `max_file_size_bytes` is reached.
    pub fn append(&self, entry: &JournalEntry) -> io::Result<()> {
        let path = self.active_write_path(&entry.session_id)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        let mut line = serde_json::to_string(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        line.push('\n');
        file.write_all(line.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    /// Replay all entries for `session_id` in chronological order (across rotation fragments).
    ///
    /// Returns `Err` naming the fragment path and line number if any line is
    /// corrupt (AC6 — no silent skips).
    pub fn replay_session(&self, session_id: &str) -> io::Result<Vec<JournalEntry>> {
        let fragments = self.rotation_paths(session_id);
        if fragments.is_empty() {
            return Ok(vec![]);
        }
        let mut entries = Vec::new();
        for frag_path in &fragments {
            let file = File::open(frag_path)?;
            let reader = BufReader::new(file);
            for (zero_idx, line_result) in reader.lines().enumerate() {
                let line_no = zero_idx + 1; // 1-based
                let line = line_result?;
                if line.trim().is_empty() {
                    continue;
                }
                let entry: JournalEntry = serde_json::from_str(&line).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "corrupt JSONL line {} in {}",
                            line_no,
                            frag_path.display()
                        ),
                    )
                })?;
                entries.push(entry);
            }
        }
        // Sort by timestamp to guarantee chronological order (AC5).
        entries.sort_by_key(|e| e.timestamp_ms);
        Ok(entries)
    }

    /// Reconstruct logical `SessionState` from the journal entries for
    /// `session_id`.  Returns `None` if no journal file exists for that ID.
    /// Returns `Err` on corrupt data (AC6).
    pub fn load_session(&self, session_id: &str) -> io::Result<Option<SessionState>> {
        let fragments = self.rotation_paths(session_id);
        if fragments.is_empty() {
            return Ok(None);
        }
        let entries = self.replay_session(session_id)?;
        let mut state = SessionState {
            session_id: session_id.to_string(),
            ..Default::default()
        };
        for entry in &entries {
            match &entry.event {
                JournalEvent::SessionCreated { ttl_secs } => {
                    state.created_ms = entry.timestamp_ms;
                    state.ttl_secs = *ttl_secs;
                }
                JournalEvent::QueryRecorded { entry: mqo_entry } => {
                    for entity in &mqo_entry.touched_entities {
                        if !state.touched_entities.contains(entity) {
                            state.touched_entities.push(entity.clone());
                        }
                    }
                    state.mqo_history.push(mqo_entry.clone());
                }
                JournalEvent::HandleAdded {
                    handle_id,
                    summary_snapshot,
                } => {
                    state
                        .handles
                        .insert(handle_id.clone(), summary_snapshot.clone());
                }
                JournalEvent::SessionExpired => {
                    state.expired = true;
                }
            }
        }
        Ok(Some(state))
    }

    /// List all sessions with persisted journal files in the configured dir.
    /// Each session appears once regardless of how many rotation fragments it has.
    pub fn list_sessions(&self) -> io::Result<Vec<JournalSessionSummary>> {
        let mut summaries: HashMap<String, JournalSessionSummary> = HashMap::new();
        let mut session_ids: Vec<String> = Vec::new();

        let read_dir = fs::read_dir(&self.config.dir)?;
        for entry_result in read_dir {
            let dir_entry = entry_result?;
            let name = dir_entry.file_name();
            let name_str = name.to_string_lossy().into_owned();
            // Collect base `.jsonl` files only; skip rotation fragments.
            if let Some(session_id) = name_str.strip_suffix(".jsonl") {
                // Rotation fragments look like "id.jsonl.N" — their stem contains a dot.
                // A bare session_id should not contain dots (it's a UUID or similar).
                if !session_id.is_empty() && !session_id.contains('.') {
                    session_ids.push(session_id.to_string());
                }
            }
        }

        for session_id in session_ids {
            let entries = self.replay_session(&session_id)?;
            let mut summary = JournalSessionSummary {
                session_id: session_id.clone(),
                created_ms: 0,
                last_event_ms: 0,
                query_count: 0,
                handle_count: 0,
            };
            let mut handle_ids: HashSet<String> = HashSet::new();
            for entry in &entries {
                if summary.created_ms == 0 || entry.timestamp_ms < summary.created_ms {
                    summary.created_ms = entry.timestamp_ms;
                }
                if entry.timestamp_ms > summary.last_event_ms {
                    summary.last_event_ms = entry.timestamp_ms;
                }
                match &entry.event {
                    JournalEvent::SessionCreated { .. } => {
                        summary.created_ms = entry.timestamp_ms;
                    }
                    JournalEvent::QueryRecorded { .. } => {
                        summary.query_count += 1;
                    }
                    JournalEvent::HandleAdded { handle_id, .. } => {
                        handle_ids.insert(handle_id.clone());
                    }
                    JournalEvent::SessionExpired => {}
                }
            }
            summary.handle_count = handle_ids.len();
            summaries.insert(session_id, summary);
        }

        Ok(summaries.into_values().collect())
    }
}

// ---------------------------------------------------------------------------
// Convenience: current timestamp in milliseconds
// ---------------------------------------------------------------------------

/// Returns the current Unix timestamp in milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
