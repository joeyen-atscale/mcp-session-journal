/// AC6: A corrupt JSONL line (truncated JSON) in the middle of a journal file
/// causes `load_session` to return `Err`, naming the corrupt line number.
/// It does NOT silently skip the corrupt line.
use mcp_session_journal::{JournalConfig, JournalEntry, JournalEvent, SessionJournal};
use std::fs::OpenOptions;
use std::io::Write;
use tempfile::TempDir;

fn write_good_entries(journal: &SessionJournal, session_id: &str, count: usize) {
    journal
        .append(&JournalEntry {
            timestamp_ms: 100,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 300 },
        })
        .unwrap();
    for i in 0..count {
        journal
            .append(&JournalEntry {
                timestamp_ms: 200 + i as u64,
                session_id: session_id.to_string(),
                event: JournalEvent::SessionExpired,
            })
            .unwrap();
    }
}

fn inject_corrupt_line(path: &std::path::Path) {
    // Append a truncated / corrupt JSON line to the file.
    let mut f = OpenOptions::new().append(true).open(path).unwrap();
    writeln!(f, "{{\"timestamp_ms\": 999, \"session_id\": \"x\", TRUNCATED").unwrap();
}

#[test]
fn ac6_corrupt_line_returns_err_not_silent() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let session_id = "sess-ac6";

    write_good_entries(&journal, session_id, 3);

    // Inject a corrupt line at the end of the file.
    let path = dir.path().join(format!("{}.jsonl", session_id));
    inject_corrupt_line(&path);

    let result = journal.load_session(session_id);
    assert!(
        result.is_err(),
        "load_session must return Err on corrupt line, not Ok"
    );
    let err = result.unwrap_err();
    let msg = err.to_string();
    // Error message must name the line number.
    assert!(
        msg.contains("line"),
        "error should mention 'line', got: {}",
        msg
    );
    // The corrupt line is the 5th line (1 SessionCreated + 3 SessionExpired + 1 corrupt).
    assert!(
        msg.contains('5'),
        "error should name line 5, got: {}",
        msg
    );
}

#[test]
fn ac6_corrupt_mid_file_stops_at_corrupt_line() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let session_id = "sess-ac6-mid";

    // Write 2 good entries.
    journal
        .append(&JournalEntry {
            timestamp_ms: 100,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 300 },
        })
        .unwrap();
    journal
        .append(&JournalEntry {
            timestamp_ms: 200,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionExpired,
        })
        .unwrap();

    // Inject corrupt line in the middle by rewriting the file.
    let path = dir.path().join(format!("{}.jsonl", session_id));
    let original = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = original.lines().collect();
    // Insert corrupt line after the first line.
    let mut new_content = lines[0].to_string();
    new_content.push('\n');
    new_content.push_str("{CORRUPT JSON\n");
    new_content.push_str(lines[1]);
    new_content.push('\n');
    std::fs::write(&path, &new_content).unwrap();

    let result = journal.load_session(session_id);
    assert!(result.is_err(), "should Err on corrupt mid-file line");
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains('2'), "should name line 2, got: {}", msg);
}

#[test]
fn ac6_replay_session_also_errors() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let session_id = "sess-ac6-replay";

    write_good_entries(&journal, session_id, 1);
    let path = dir.path().join(format!("{}.jsonl", session_id));
    inject_corrupt_line(&path);

    let result = journal.replay_session(session_id);
    assert!(result.is_err(), "replay_session must also Err on corrupt line");
}
