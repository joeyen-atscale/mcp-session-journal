/// AC5: `replay_session` returns all journal entries in chronological order
/// (by `timestamp_ms`). A session with interleaved `QueryRecorded` and
/// `HandleAdded` events is replayed in strict order.
use mcp_session_journal::{
    DatasetSummary, JournalConfig, JournalEntry, JournalEvent, MqoHistoryEntry, SessionJournal,
};
use tempfile::TempDir;

#[test]
fn ac5_replay_chronological_order() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let session_id = "sess-ac5";

    // Write events with intentionally non-sequential timestamps to verify
    // sort-by-timestamp in replay_session.
    let events: Vec<(u64, JournalEvent)> = vec![
        (1000, JournalEvent::SessionCreated { ttl_secs: 600 }),
        (
            1001,
            JournalEvent::QueryRecorded {
                entry: MqoHistoryEntry {
                    query_id: "q1".to_string(),
                    timestamp_ms: 1001,
                    touched_entities: vec!["A".to_string()],
                    payload: serde_json::Value::Null,
                },
            },
        ),
        (
            1002,
            JournalEvent::HandleAdded {
                handle_id: "h1".to_string(),
                summary_snapshot: DatasetSummary {
                    dataset_name: "ds1".to_string(),
                    row_count: None,
                    columns: vec![],
                    metadata: serde_json::Value::Null,
                },
            },
        ),
        (
            1003,
            JournalEvent::QueryRecorded {
                entry: MqoHistoryEntry {
                    query_id: "q2".to_string(),
                    timestamp_ms: 1003,
                    touched_entities: vec!["B".to_string()],
                    payload: serde_json::Value::Null,
                },
            },
        ),
        (
            1004,
            JournalEvent::HandleAdded {
                handle_id: "h2".to_string(),
                summary_snapshot: DatasetSummary {
                    dataset_name: "ds2".to_string(),
                    row_count: Some(500),
                    columns: vec!["col1".to_string()],
                    metadata: serde_json::Value::Null,
                },
            },
        ),
        (1005, JournalEvent::SessionExpired),
    ];

    for (ts, event) in &events {
        journal
            .append(&JournalEntry {
                timestamp_ms: *ts,
                session_id: session_id.to_string(),
                event: event.clone(),
            })
            .unwrap();
    }

    let replayed = journal.replay_session(session_id).unwrap();
    assert_eq!(replayed.len(), events.len());

    // Verify strict chronological order.
    for window in replayed.windows(2) {
        assert!(
            window[0].timestamp_ms <= window[1].timestamp_ms,
            "replay must be in chronological order: {} > {}",
            window[0].timestamp_ms,
            window[1].timestamp_ms
        );
    }

    // Verify all events are present.
    assert!(matches!(replayed[0].event, JournalEvent::SessionCreated { .. }));
    assert!(matches!(replayed[1].event, JournalEvent::QueryRecorded { .. }));
    assert!(matches!(replayed[2].event, JournalEvent::HandleAdded { .. }));
    assert!(matches!(replayed[3].event, JournalEvent::QueryRecorded { .. }));
    assert!(matches!(replayed[4].event, JournalEvent::HandleAdded { .. }));
    assert!(matches!(replayed[5].event, JournalEvent::SessionExpired));
}

#[test]
fn ac5_replay_empty_session() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let entries = journal.replay_session("no-such").unwrap();
    assert!(entries.is_empty());
}
