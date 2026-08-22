use crate::injector::action::RetrievalPlan;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

/// Real outcome classification of an injected context package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutcomeType {
    /// Delivered package was consumed and task succeeded without extra iterations.
    ConsumedHelpful,
    /// Delivered package was consumed but required partial additional retrieval.
    ConsumedPartial,
    /// Delivered package was ignored or irrelevant.
    IgnoredIrrelevant,
    /// Subsequent compilation step failed.
    CompileFailed,
    /// Subsequent test execution failed.
    TestFailed,
}

impl OutcomeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ConsumedHelpful => "CONSUMED_HELPFUL",
            Self::ConsumedPartial => "CONSUMED_PARTIAL",
            Self::IgnoredIrrelevant => "IGNORED_IRRELEVANT",
            Self::CompileFailed => "COMPILE_FAILED",
            Self::TestFailed => "TEST_FAILED",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "CONSUMED_HELPFUL" => Self::ConsumedHelpful,
            "CONSUMED_PARTIAL" => Self::ConsumedPartial,
            "COMPILE_FAILED" => Self::CompileFailed,
            "TEST_FAILED" => Self::TestFailed,
            _ => Self::IgnoredIrrelevant,
        }
    }

    pub fn reward_weight(&self) -> f64 {
        match self {
            Self::ConsumedHelpful => 1.0,
            Self::ConsumedPartial => 0.4,
            Self::IgnoredIrrelevant => -0.5,
            Self::CompileFailed => -0.8,
            Self::TestFailed => -1.0,
        }
    }
}

/// Durable record of an anticipatory injection outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutcomeRecord {
    pub record_id: String,
    pub intent_summary: String,
    pub plan: RetrievalPlan,
    pub outcome_type: OutcomeType,
    pub tool_calls_count: u32,
    pub retrieval_wait_ms: u64,
    pub timestamp_ms: u64,
}

/// Thread-safe SQLite store for outcome records.
pub struct OutcomeFeedbackStore {
    conn: Mutex<Connection>,
}

impl OutcomeFeedbackStore {
    /// Opens or creates an outcome feedback store.
    pub fn open(path_or_uri: &str) -> Result<Self, rusqlite::Error> {
        let conn = if path_or_uri == "sqlite::memory:" || path_or_uri == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(path_or_uri)?
        };

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS outcome_records (
                record_id TEXT PRIMARY KEY,
                intent_summary TEXT NOT NULL,
                plan_json TEXT NOT NULL,
                outcome_type TEXT NOT NULL,
                tool_calls_count INTEGER NOT NULL,
                retrieval_wait_ms INTEGER NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_outcome_intent ON outcome_records(intent_summary);
            CREATE INDEX IF NOT EXISTS idx_outcome_timestamp ON outcome_records(timestamp_ms);",
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Records an outcome event.
    pub fn record_outcome(&self, record: &OutcomeRecord) -> Result<(), rusqlite::Error> {
        let plan_json = serde_json::to_string(&record.plan).unwrap_or_else(|_| "{}".into());
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO outcome_records (
                record_id, intent_summary, plan_json, outcome_type,
                tool_calls_count, retrieval_wait_ms, timestamp_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                record.record_id,
                record.intent_summary,
                plan_json,
                record.outcome_type.as_str(),
                record.tool_calls_count,
                record.retrieval_wait_ms,
                record.timestamp_ms
            ],
        )?;
        Ok(())
    }

    /// Retrieves all recorded outcomes up to limit.
    pub fn list_records(&self, limit: usize) -> Result<Vec<OutcomeRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT record_id, intent_summary, plan_json, outcome_type,
                    tool_calls_count, retrieval_wait_ms, timestamp_ms
             FROM outcome_records ORDER BY timestamp_ms DESC LIMIT ?1",
        )?;

        let rows = stmt.query_map(params![limit as i64], |row| {
            let record_id: String = row.get(0)?;
            let intent_summary: String = row.get(1)?;
            let plan_json: String = row.get(2)?;
            let outcome_type_str: String = row.get(3)?;
            let tool_calls_count: u32 = row.get(4)?;
            let retrieval_wait_ms: u64 = row.get(5)?;
            let timestamp_ms: u64 = row.get(6)?;

            let plan: RetrievalPlan = serde_json::from_str(&plan_json).unwrap_or_else(|_| {
                RetrievalPlan::new(
                    &record_id,
                    &intent_summary,
                    crate::injector::action::PlanStrategy::DistilledPolicy,
                )
            });

            Ok(OutcomeRecord {
                record_id,
                intent_summary,
                plan,
                outcome_type: OutcomeType::from_str(&outcome_type_str),
                tool_calls_count,
                retrieval_wait_ms,
                timestamp_ms,
            })
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    /// Calculates success ratio (0.0 to 1.0) for a given intent prefix.
    pub fn get_success_ratio(&self, intent_prefix: &str) -> Result<f64, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let query_pattern = format!("{intent_prefix}%");
        let mut stmt =
            conn.prepare("SELECT outcome_type FROM outcome_records WHERE intent_summary LIKE ?1")?;

        let mut total = 0usize;
        let mut helpful = 0usize;

        let rows = stmt.query_map(params![query_pattern], |row| {
            let t: String = row.get(0)?;
            Ok(t)
        })?;

        for r in rows {
            let t = r?;
            total += 1;
            if t == "CONSUMED_HELPFUL" || t == "CONSUMED_PARTIAL" {
                helpful += 1;
            }
        }

        if total == 0 {
            Ok(0.5) // Neutral prior when no data exists
        } else {
            Ok(helpful as f64 / total as f64)
        }
    }
}
