//! Compiler incident memory and historical fix linking.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// A recorded real rustc compiler failure, error diagnostics, and fixing commit link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompilerIncident {
    pub incident_id: String,
    pub repository_id: String,
    pub rustc_version: String,
    pub error_codes: Vec<String>,
    pub diagnostic: String,
    pub affected_symbol_ids: Vec<String>,
    pub failing_commit: Option<String>,
    pub fixing_commit: Option<String>,
    pub verified_fixed: bool,
    pub recorded_at_ms: u64,
}

/// Durable SQLite-backed store for compiler incidents and repair history.
pub struct CompilerMemoryStore {
    conn: Arc<Mutex<Connection>>,
}

impl CompilerMemoryStore {
    /// Opens or initializes a SQLite compiler incident database.
    pub fn open(db_url: &str) -> Result<Self, crate::types::Error> {
        let path = if db_url == "sqlite::memory:" {
            Connection::open_in_memory()
        } else {
            let p = db_url
                .trim_start_matches("sqlite://")
                .trim_start_matches("sqlite:");
            Connection::open(p)
        }
        .map_err(crate::types::Error::Db)?;

        let store = Self {
            conn: Arc::new(Mutex::new(path)),
        };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<(), crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS compiler_incidents (
                incident_id TEXT PRIMARY KEY,
                repository_id TEXT NOT NULL,
                rustc_version TEXT NOT NULL,
                error_codes TEXT NOT NULL,
                diagnostic TEXT NOT NULL,
                affected_symbol_ids TEXT NOT NULL,
                failing_commit TEXT,
                fixing_commit TEXT,
                verified_fixed INTEGER NOT NULL DEFAULT 0,
                recorded_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_compiler_incidents_repo ON compiler_incidents (repository_id);
            CREATE INDEX IF NOT EXISTS idx_compiler_incidents_fixed ON compiler_incidents (verified_fixed);
            ",
        )
        .map_err(crate::types::Error::Db)?;
        Ok(())
    }

    /// Records a new compiler incident.
    pub fn record_incident(&self, incident: &CompilerIncident) -> Result<(), crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let error_codes_json = serde_json::to_string(&incident.error_codes).unwrap_or_default();
        let symbol_ids_json =
            serde_json::to_string(&incident.affected_symbol_ids).unwrap_or_default();

        conn.execute(
            "INSERT INTO compiler_incidents (
                incident_id, repository_id, rustc_version, error_codes, diagnostic,
                affected_symbol_ids, failing_commit, fixing_commit, verified_fixed, recorded_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(incident_id) DO UPDATE SET
                error_codes = excluded.error_codes,
                diagnostic = excluded.diagnostic,
                affected_symbol_ids = excluded.affected_symbol_ids,
                fixing_commit = excluded.fixing_commit,
                verified_fixed = excluded.verified_fixed",
            params![
                incident.incident_id,
                incident.repository_id,
                incident.rustc_version,
                error_codes_json,
                incident.diagnostic,
                symbol_ids_json,
                incident.failing_commit,
                incident.fixing_commit,
                if incident.verified_fixed { 1 } else { 0 },
                incident.recorded_at_ms,
            ],
        )
        .map_err(crate::types::Error::Db)?;
        Ok(())
    }

    /// Marks a compiler incident as verified fixed with the fixing commit hash.
    pub fn mark_verified_fixed(
        &self,
        incident_id: &str,
        fixing_commit: &str,
    ) -> Result<(), crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE compiler_incidents SET verified_fixed = 1, fixing_commit = ?1 WHERE incident_id = ?2",
            params![fixing_commit, incident_id],
        )
        .map_err(crate::types::Error::Db)?;
        Ok(())
    }

    /// Finds past incidents related to an error code.
    pub fn find_by_error_code(
        &self,
        error_code: &str,
    ) -> Result<Vec<CompilerIncident>, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT incident_id, repository_id, rustc_version, error_codes, diagnostic,
                        affected_symbol_ids, failing_commit, fixing_commit, verified_fixed, recorded_at_ms
                 FROM compiler_incidents
                 WHERE error_codes LIKE ?1
                 ORDER BY recorded_at_ms DESC LIMIT 20",
            )
            .map_err(crate::types::Error::Db)?;

        let pattern = format!("%{}%", error_code);
        let rows = stmt
            .query_map(params![pattern], |row| {
                let error_codes_raw: String = row.get(3)?;
                let symbol_ids_raw: String = row.get(5)?;
                let verified_int: i32 = row.get(8)?;

                Ok(CompilerIncident {
                    incident_id: row.get(0)?,
                    repository_id: row.get(1)?,
                    rustc_version: row.get(2)?,
                    error_codes: serde_json::from_str(&error_codes_raw).unwrap_or_default(),
                    diagnostic: row.get(4)?,
                    affected_symbol_ids: serde_json::from_str(&symbol_ids_raw).unwrap_or_default(),
                    failing_commit: row.get(6)?,
                    fixing_commit: row.get(7)?,
                    verified_fixed: verified_int == 1,
                    recorded_at_ms: row.get(9)?,
                })
            })
            .map_err(crate::types::Error::Db)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(crate::types::Error::Db)?);
        }
        Ok(results)
    }

    /// Finds incidents touching a given symbol ID.
    pub fn find_by_symbol_id(
        &self,
        symbol_id: &str,
    ) -> Result<Vec<CompilerIncident>, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT incident_id, repository_id, rustc_version, error_codes, diagnostic,
                        affected_symbol_ids, failing_commit, fixing_commit, verified_fixed, recorded_at_ms
                 FROM compiler_incidents
                 WHERE affected_symbol_ids LIKE ?1
                 ORDER BY recorded_at_ms DESC LIMIT 20",
            )
            .map_err(crate::types::Error::Db)?;

        let pattern = format!("%{}%", symbol_id);
        let rows = stmt
            .query_map(params![pattern], |row| {
                let error_codes_raw: String = row.get(3)?;
                let symbol_ids_raw: String = row.get(5)?;
                let verified_int: i32 = row.get(8)?;

                Ok(CompilerIncident {
                    incident_id: row.get(0)?,
                    repository_id: row.get(1)?,
                    rustc_version: row.get(2)?,
                    error_codes: serde_json::from_str(&error_codes_raw).unwrap_or_default(),
                    diagnostic: row.get(4)?,
                    affected_symbol_ids: serde_json::from_str(&symbol_ids_raw).unwrap_or_default(),
                    failing_commit: row.get(6)?,
                    fixing_commit: row.get(7)?,
                    verified_fixed: verified_int == 1,
                    recorded_at_ms: row.get(9)?,
                })
            })
            .map_err(crate::types::Error::Db)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(crate::types::Error::Db)?);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compiler_memory_roundtrip() {
        let store = CompilerMemoryStore::open("sqlite::memory:").unwrap();
        let incident = CompilerIncident {
            incident_id: "inc-100".into(),
            repository_id: "nauti-nodes".into(),
            rustc_version: "1.80.0".into(),
            error_codes: vec!["E0308".into(), "E0382".into()],
            diagnostic: "mismatched types: expected bool, found &str".into(),
            affected_symbol_ids: vec!["sym-abc".into()],
            failing_commit: Some("deadbeef".into()),
            fixing_commit: None,
            verified_fixed: false,
            recorded_at_ms: now_ms(),
        };

        store.record_incident(&incident).unwrap();

        let found = store.find_by_error_code("E0308").unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].incident_id, "inc-100");
        assert!(!found[0].verified_fixed);

        store.mark_verified_fixed("inc-100", "c0ffee").unwrap();
        let updated = store.find_by_symbol_id("sym-abc").unwrap();
        assert_eq!(updated.len(), 1);
        assert!(updated[0].verified_fixed);
        assert_eq!(updated[0].fixing_commit.as_deref(), Some("c0ffee"));
    }
}
