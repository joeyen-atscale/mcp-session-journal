/// AC1: `append` on a new session writes a `SessionCreated` entry as valid JSON
/// on the first line of `<dir>/<session_id>.jsonl`. The file is created if it
/// does not exist.
use mcp_session_journal::{JournalConfig, JournalEntry, JournalEvent, SessionJournal};
use std::fs;
use tempfile::TempDir;

fn temp_journal() -> (TempDir, SessionJournal) {
    let dir = TempDir::new().unwrap();
    let config = JournalConfig::new(dir.path());
    let journal = SessionJournal::new(config).unwrap();
    (dir, journal)
}

#[test]
fn ac1_session_created_first_line() {
    let (dir, journal) = temp_journal();
    let session_id = "sess-ac1-create";

    let entry = JournalEntry {
        timestamp_ms: 1_700_000_000_000,
        session_id: session_id.to_string(),
        event: JournalEvent::SessionCreated { ttl_secs: 3600 },
    };
    journal.append(&entry).unwrap();

    let path = dir.path().join(format!("{}.jsonl", session_id));
    assert!(path.exists(), "journal file should have been created");

    let contents = fs::read_to_string(&path).unwrap();
    let first_line = contents.lines().next().unwrap();

    // Must be valid JSON
    let parsed: serde_json::Value = serde_json::from_str(first_line)
        .expect("first line must be valid JSON");

    // Must contain SessionCreated
    assert_eq!(parsed["event"]["type"], "session_created");
    assert_eq!(parsed["event"]["ttl_secs"], 3600u64);
    assert_eq!(parsed["session_id"], session_id);
    assert_eq!(parsed["timestamp_ms"], 1_700_000_000_000u64);
}

#[test]
fn ac1_file_not_pre_existing() {
    let (dir, journal) = temp_journal();
    let session_id = "sess-ac1-new";
    let path = dir.path().join(format!("{}.jsonl", session_id));
    assert!(!path.exists());

    journal
        .append(&JournalEntry {
            timestamp_ms: 1,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 60 },
        })
        .unwrap();

    assert!(path.exists());
}
