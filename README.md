# mcp-session-journal

A durable, append-only JSONL log of MCP investigation sessions — so a server restart isn't a cold start, and a past investigation can be replayed.

## Why it exists

`mcp-session-state` keeps an investigation's context in memory under a TTL. That works until the MCP server restarts or the agent disconnects, at which point the session evaporates and the next reconnect starts from nothing. Two things need it to survive that:

- **Replay.** The `mcp-investigation-replayer` re-runs a prior investigation against a new build to check for regressions. It can only do that if the investigation was written down somewhere.
- **Resume.** An agent reconnecting after a restart can restore its prior session and pick up where it left off, instead of rebuilding context from scratch.

This library is that persistence. Every mutation to a session — created, query recorded, handle added, expired — is appended as one JSON line to a per-session journal file. The file is the record; memory is just a cache of it. The format follows the tiger runlog pattern: append-only, one complete event per line, JSONL so it stays diffable and portable. No SQLite, no external database.

## Install

A library crate, used as a dependency rather than installed as a binary:

```toml
[dependencies]
mcp-session-journal = { git = "https://github.com/joeyen-atscale/mcp-session-journal" }
```

## Usage

```rust
use mcp_session_journal::{
    JournalConfig, JournalEntry, JournalEvent, SessionJournal, now_ms,
};

let journal = SessionJournal::new(JournalConfig::new("/var/lib/mcp-sessions"))?;

// Record an event. Each append is one line in <dir>/<session_id>.jsonl.
journal.append(&JournalEntry {
    timestamp_ms: now_ms(),
    session_id: "my-session-id".to_string(),
    event: JournalEvent::SessionCreated { ttl_secs: 3600 },
})?;

// Rebuild the in-memory state by folding the journal. None if no such session.
if let Some(state) = journal.load_session("my-session-id")? {
    println!("{} queries, {} handles", state.mqo_history.len(), state.handles.len());
}

// List every persisted session, one summary each.
for summary in journal.list_sessions()? {
    println!("{}: {} queries", summary.session_id, summary.query_count);
}

// Or get the raw event stream in chronological order.
let entries = journal.replay_session("my-session-id")?;
```

`load_session` returns `Option<SessionState>` — `None` when the session has no journal file — and replays the events to reconstruct `mqo_history`, `handles`, deduplicated `touched_entities`, and the `expired` flag.

## How it works

One file per session: `<dir>/<session_id>.jsonl`, one JSON object per line.

When a file exceeds `max_file_size_bytes` (default 10 MB), writes roll over to `<session_id>.jsonl.1`, then `.2`, and so on. Reads stitch the fragments back together in order, so rotation is invisible to `load_session`, `list_sessions`, and `replay_session`.

A corrupt line is a hard error, not a skipped line. `replay_session` and `load_session` return `Err` naming the fragment and the 1-based line number, so a damaged journal surfaces immediately rather than silently dropping events.

The `MqoHistoryEntry` and `DatasetSummary` types here are standalone stand-ins for the corresponding `mcp-session-state` types — their inner payloads are `serde_json::Value`, which keeps this crate free of a dependency on session-state's full type graph.

## Where it fits

Part of the MCP investigation tooling: `mcp-session-state` holds live context, `mcp-session-journal` (this crate) persists it, and `mcp-investigation-replayer` reads the journal to re-run past sessions.

## Acceptance criteria

The behavior the tests in `tests/` pin down:

| # | Behavior |
|---|-----------|
| AC1 | `append` creates `<dir>/<session_id>.jsonl` on first write with a valid `SessionCreated` line |
| AC2 | `load_session` reconstructs `SessionState` with the right `mqo_history.len()` and all `touched_entities` |
| AC3 | `list_sessions` returns one summary per session with correct `query_count` / `handle_count` |
| AC4 | Rotation triggers at `max_file_size_bytes`; reads stitch fragments back in order |
| AC5 | `replay_session` returns entries sorted by `timestamp_ms` |
| AC6 | A corrupt JSONL line returns `Err` naming the line number — no silent skips |
| AC7 | 1000 appends complete under 500 ms (a `#[ignore]`d benchmark; run with `cargo test -- --ignored`) |

## License

MIT OR Apache-2.0
