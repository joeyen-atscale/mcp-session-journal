/// AC2: `load_session` on a journal with N `QueryRecorded` entries returns a
/// `SessionState` with `mqo_history.len() == N` and `touched_entities`
/// containing all entity names from all N MQOs.
use mcp_session_journal::{
    DatasetSummary, JournalConfig, JournalEntry, JournalEvent, MqoHistoryEntry, SessionJournal,
};
use tempfile::TempDir;

fn make_mqo(query_id: &str, entities: Vec<&str>, ts: u64) -> MqoHistoryEntry {
    MqoHistoryEntry {
        query_id: query_id.to_string(),
        timestamp_ms: ts,
        touched_entities: entities.iter().map(|s| s.to_string()).collect(),
        payload: serde_json::Value::Null,
    }
}

#[test]
fn ac2_load_n_queries() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let session_id = "sess-ac2";

    // SessionCreated
    journal
        .append(&JournalEntry {
            timestamp_ms: 1000,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 600 },
        })
        .unwrap();

    // 5 QueryRecorded entries
    let mqos = vec![
        make_mqo("q1", vec!["SalesOrder", "Customer"], 1001),
        make_mqo("q2", vec!["Customer", "Product"], 1002),
        make_mqo("q3", vec!["Invoice"], 1003),
        make_mqo("q4", vec!["SalesOrder"], 1004),
        make_mqo("q5", vec!["Region", "Country"], 1005),
    ];

    for mqo in &mqos {
        journal
            .append(&JournalEntry {
                timestamp_ms: mqo.timestamp_ms,
                session_id: session_id.to_string(),
                event: JournalEvent::QueryRecorded { entry: mqo.clone() },
            })
            .unwrap();
    }

    let state = journal.load_session(session_id).unwrap().unwrap();

    assert_eq!(state.mqo_history.len(), 5, "mqo_history.len() should be 5");
    assert_eq!(state.session_id, session_id);
    assert_eq!(state.ttl_secs, 600);

    // All touched entities should be present (deduplicated)
    let expected_entities = vec![
        "SalesOrder", "Customer", "Product", "Invoice", "Region", "Country",
    ];
    for entity in &expected_entities {
        assert!(
            state.touched_entities.contains(&entity.to_string()),
            "touched_entities should contain '{}'",
            entity
        );
    }
    // Deduplication: "SalesOrder" appears in q1 and q4 — should appear only once
    let sales_order_count = state
        .touched_entities
        .iter()
        .filter(|e| e.as_str() == "SalesOrder")
        .count();
    assert_eq!(sales_order_count, 1, "touched_entities should deduplicate");
}

#[test]
fn ac2_load_nonexistent_returns_none() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let result = journal.load_session("no-such-session").unwrap();
    assert!(result.is_none());
}

#[test]
fn ac2_load_with_handles() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let session_id = "sess-ac2-handles";

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
            event: JournalEvent::HandleAdded {
                handle_id: "h1".to_string(),
                summary_snapshot: DatasetSummary {
                    dataset_name: "sales".to_string(),
                    row_count: Some(1000),
                    columns: vec!["id".to_string(), "amount".to_string()],
                    metadata: serde_json::Value::Null,
                },
            },
        })
        .unwrap();

    let state = journal.load_session(session_id).unwrap().unwrap();
    assert!(state.handles.contains_key("h1"));
    assert_eq!(state.handles["h1"].dataset_name, "sales");
}
