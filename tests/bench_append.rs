/// AC7 (SHOULD): Appending 1000 entries to a session journal completes in under
/// 500ms. Marked `#[ignore]` so it doesn't block CI by default.
use mcp_session_journal::{JournalConfig, JournalEntry, JournalEvent, MqoHistoryEntry, SessionJournal};
use std::time::Instant;
use tempfile::TempDir;

#[test]
#[ignore]
fn bench_1000_appends_under_500ms() {
    let dir = TempDir::new().unwrap();
    let journal = SessionJournal::new(JournalConfig::new(dir.path())).unwrap();
    let session_id = "bench-sess";

    // Warm-up: create the session.
    journal
        .append(&JournalEntry {
            timestamp_ms: 0,
            session_id: session_id.to_string(),
            event: JournalEvent::SessionCreated { ttl_secs: 3600 },
        })
        .unwrap();

    let start = Instant::now();
    for i in 0u64..1000 {
        journal
            .append(&JournalEntry {
                timestamp_ms: 1000 + i,
                session_id: session_id.to_string(),
                event: JournalEvent::QueryRecorded {
                    entry: MqoHistoryEntry {
                        query_id: format!("bench-q{}", i),
                        timestamp_ms: 1000 + i,
                        touched_entities: vec!["SalesOrder".to_string(), "Customer".to_string()],
                        payload: serde_json::json!({"some": "payload", "index": i}),
                    },
                },
            })
            .unwrap();
    }
    let elapsed = start.elapsed();

    println!("1000 appends took {:?}", elapsed);
    assert!(
        elapsed.as_millis() < 500,
        "1000 appends took {}ms, expected < 500ms",
        elapsed.as_millis()
    );
}
