/// AC3: `list_sessions` returns one `JournalSessionSummary` per
/// `<session_id>.jsonl` file in `dir`. For a session with 5 queries and 4
/// handles, `query_count == 5` and `handle_count == 4`.
use mcp_session_journal::{
    DatasetSummary, JournalConfig, JournalEntry, JournalEvent, MqoHistoryEntry, SessionJournal,
};
use tempfile::TempDir;

fn write_session(
    journal: &SessionJournal,
    session_id: &str,
    query_count: usize,
    handle_count: usize,
) {
    journal
        .append(&JournalEntry {
            timestamp_ms: 1000,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 600 },
        })
        .unwrap();

    for i in 0..query_count {
        journal
            .append(&JournalEntry {
                timestamp_ms: 2000 + i as u64,
                session_id: session_id.to_string(),
                event: JournalEvent::QueryRecorded {
                    entry: MqoHistoryEntry {
                        query_id: format!("q{}", i),
                        timestamp_ms: 2000 + i as u64,
                        touched_entities: vec![format!("Entity{}", i)],
                        payload: serde_json::Value::Null,
                    },
                },
            })
            .unwrap();
    }

    for i in 0..handle_count {
        journal
            .append(&JournalEntry {
                timestamp_ms: 3000 + i as u64,
                session_id: session_id.to_string(),
                event: JournalEvent::HandleAdded {
                    handle_id: format!("handle-{}", i),
                    summary_snapshot: DatasetSummary {
                        dataset_name: format!("ds{}", i),
                        row_count: Some(100),
                        columns: vec![],
                        metadata: serde_json::Value::Null,
                    },
                },
            })
            .unwrap();
    }
}

#[test]
fn ac3_list_sessions_counts() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();

    write_session(&journal, "sess-a", 5, 4);
    write_session(&journal, "sess-b", 2, 1);
    write_session(&journal, "sess-c", 0, 0);

    let mut summaries = journal.list_sessions().unwrap();
    summaries.sort_by(|a, b| a.session_id.cmp(&b.session_id));

    assert_eq!(summaries.len(), 3, "should have 3 sessions");

    let a = summaries.iter().find(|s| s.session_id == "sess-a").unwrap();
    assert_eq!(a.query_count, 5);
    assert_eq!(a.handle_count, 4);

    let b = summaries.iter().find(|s| s.session_id == "sess-b").unwrap();
    assert_eq!(b.query_count, 2);
    assert_eq!(b.handle_count, 1);

    let c = summaries.iter().find(|s| s.session_id == "sess-c").unwrap();
    assert_eq!(c.query_count, 0);
    assert_eq!(c.handle_count, 0);
}

#[test]
fn ac3_list_empty_dir() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let summaries = journal.list_sessions().unwrap();
    assert!(summaries.is_empty());
}

#[test]
fn ac3_list_timestamps() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    write_session(&journal, "ts-test", 3, 2);

    let summaries = journal.list_sessions().unwrap();
    assert_eq!(summaries.len(), 1);
    let s = &summaries[0];
    assert_eq!(s.created_ms, 1000);
    assert!(s.last_event_ms >= s.created_ms);
}
