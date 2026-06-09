/// AC4: When a journal file exceeds `max_file_size_bytes`, the next `append`
/// writes to `<session_id>.jsonl.1`. `load_session` reads both fragments in
/// order and reconstructs the complete session.
use mcp_session_journal::{JournalConfig, JournalEntry, JournalEvent, MqoHistoryEntry, SessionJournal};
use tempfile::TempDir;

fn tiny_journal(dir: &std::path::Path) -> SessionJournal {
    let config = JournalConfig {
        dir: dir.to_path_buf(),
        // Set a very small rotation limit so it triggers after a few entries.
        max_file_size_bytes: 300,
    };
    SessionJournal::new(config).unwrap()
}

fn make_entry(session_id: &str, ts: u64, query_id: &str) -> JournalEntry {
    JournalEntry {
        timestamp_ms: ts,
        session_id: session_id.to_string(),
        event: JournalEvent::QueryRecorded {
            entry: MqoHistoryEntry {
                query_id: query_id.to_string(),
                timestamp_ms: ts,
                touched_entities: vec!["Entity1".to_string()],
                payload: serde_json::Value::Null,
            },
        },
    }
}

#[test]
fn ac4_rotation_creates_fragment() {
    let dir = TempDir::new().unwrap();
    let journal = tiny_journal(dir.path());
    let session_id = "sess-rot";

    // First: SessionCreated
    journal
        .append(&JournalEntry {
            timestamp_ms: 1,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 3600 },
        })
        .unwrap();

    // Append enough entries to force rotation.
    for i in 0..20u64 {
        journal.append(&make_entry(session_id, 100 + i, &format!("q{}", i))).unwrap();
    }

    // At least one rotation fragment should exist.
    let frag1 = dir.path().join(format!("{}.jsonl.1", session_id));
    assert!(
        frag1.exists(),
        "rotation fragment .jsonl.1 should exist after overflow"
    );
}

#[test]
fn ac4_load_across_fragments() {
    let dir = TempDir::new().unwrap();
    let journal = tiny_journal(dir.path());
    let session_id = "sess-rot2";

    journal
        .append(&JournalEntry {
            timestamp_ms: 1,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 3600 },
        })
        .unwrap();

    const N: usize = 20;
    for i in 0..N as u64 {
        journal.append(&make_entry(session_id, 1000 + i, &format!("q{}", i))).unwrap();
    }

    // load_session must reconstruct all N queries across fragments.
    let state = journal.load_session(session_id).unwrap().unwrap();
    assert_eq!(
        state.mqo_history.len(),
        N,
        "all {} queries should be loaded across rotation fragments",
        N
    );
}

#[test]
fn ac4_replay_across_fragments_ordered() {
    let dir = TempDir::new().unwrap();
    let journal = tiny_journal(dir.path());
    let session_id = "sess-rot3";

    journal
        .append(&JournalEntry {
            timestamp_ms: 1,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 3600 },
        })
        .unwrap();

    const N: usize = 20;
    for i in 0..N as u64 {
        journal.append(&make_entry(session_id, 1000 + i, &format!("q{}", i))).unwrap();
    }

    let entries = journal.replay_session(session_id).unwrap();
    // Entries should be in ascending timestamp order.
    for window in entries.windows(2) {
        assert!(
            window[0].timestamp_ms <= window[1].timestamp_ms,
            "entries should be in chronological order"
        );
    }
    // Total entries = 1 (SessionCreated) + N (QueryRecorded)
    assert_eq!(entries.len(), N + 1);
}
