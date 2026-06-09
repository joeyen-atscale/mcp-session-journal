# mcp-session-journal

Durable, append-only episodic log of MCP investigation sessions.

`mcp-session-state` holds investigation context in-memory with a TTL. When the MCP
server restarts or the agent disconnects, the session evaporates. This library wraps
session state with **durable, append-only disk persistence**: every mutation to a
session state is snapshotted to a JSONL journal file, and any session can be loaded
from the journal at startup or by the replayer.

## Why this exists

- **Session-level regression testing requires durability.** The `mcp-investigation-replayer`
  re-executes a prior investigation on a new build. It cannot function unless the
  investigation is persisted somewhere. The session journal is that persistence.
- **Server restarts are not catastrophic if the journal exists.** An agent reconnecting
  after a restart can restore its prior session from the journal and resume the
  investigation. Without the journal, every reconnect is a cold start.
- **Follows the tiger runlog pattern.** Append-only, each line is a complete snapshot,
  JSONL so it's diffable and portable. No SQLite, no external DB.

## File layout

`<dir>/<session_id>.jsonl` — one file per session, one JSON object per line.

Rotation: when a journal file exceeds `max_file_size_bytes`, subsequent writes go to
`<session_id>.jsonl.1`, `.2`, etc. `load_session` reads all rotation fragments in order.

## Usage

```rust
use mcp_session_journal::{JournalConfig, JournalEntry, JournalEvent, SessionJournal};

let config = JournalConfig::new("/var/lib/mcp-sessions");
let journal = SessionJournal::new(config)?;

// Record a new session
journal.append(&JournalEntry {
    timestamp_ms: now_ms(),
    session_id: "my-session-id".to_string(),
    event: JournalEvent::SessionCreated { ttl_secs: 3600 },
})?;

// Load it back
let state = journal.load_session("my-session-id")?;

// List all sessions
let summaries = journal.list_sessions()?;

// Replay all entries in order
let entries = journal.replay_session("my-session-id")?;
```

## Acceptance criteria

| # | Behaviour |
|---|-----------|
| AC1 | `append` creates `<dir>/<session_id>.jsonl` on first write with a valid JSON `SessionCreated` line |
| AC2 | `load_session` returns a `SessionState` with correct `mqo_history.len()` and all `touched_entities` |
| AC3 | `list_sessions` returns one summary per `.jsonl` file with correct `query_count` / `handle_count` |
| AC4 | File rotation triggers at `max_file_size_bytes`; `load_session` stitches fragments in order |
| AC5 | `replay_session` returns entries sorted by `timestamp_ms` |
| AC6 | A corrupt JSONL line returns `Err` naming the line number — no silent skips |
| AC7 | 1000 appends complete in < 500 ms (ignored in CI, run with `cargo test -- --ignored`) |

## License

MIT OR Apache-2.0
