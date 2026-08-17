//! Persistent node identity revocation store backed by SQLite.

use crate::types::{Error, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevocationEntry {
    pub identifier: String,
    pub reason: String,
    pub revoked_at_ms: u64,
}

pub struct RevocationStore {
    conn: Mutex<Connection>,
}

impl RevocationStore {
    pub fn open(db_url: &str) -> Result<Self> {
        let conn = if db_url == ":memory:" || db_url == "sqlite::memory:" {
            Connection::open_in_memory()?
        } else {
            let path = db_url.strip_prefix("sqlite:").unwrap_or(db_url);
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    let _ = std::fs::create_dir_all(parent);
                }
            }
            Connection::open(path)?
        };
        let _ = conn.pragma_update(None, "busy_timeout", 5000);
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS revoked_nodes (
                identifier TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                revoked_at_ms INTEGER NOT NULL
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn revoke(&self, identifier: &str, reason: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let now = crate::types::now_ms();
        conn.execute(
            "INSERT INTO revoked_nodes (identifier, reason, revoked_at_ms) VALUES (?1, ?2, ?3)
             ON CONFLICT(identifier) DO UPDATE SET reason = excluded.reason, revoked_at_ms = excluded.revoked_at_ms",
            params![identifier, reason, now],
        )?;
        Ok(())
    }

    pub fn unrevoke(&self, identifier: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let count = conn.execute(
            "DELETE FROM revoked_nodes WHERE identifier = ?1",
            params![identifier],
        )?;
        Ok(count > 0)
    }

    pub fn is_revoked(&self, identifier: &str) -> bool {
        if identifier.is_empty() {
            return false;
        }
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM revoked_nodes WHERE identifier = ?1",
                params![identifier],
                |r| r.get(0),
            )
            .unwrap_or(0);
        count > 0
    }

    pub fn list_revoked(&self) -> Result<Vec<RevocationEntry>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let mut stmt =
            conn.prepare("SELECT identifier, reason, revoked_at_ms FROM revoked_nodes")?;
        let rows = stmt.query_map([], |r| {
            Ok(RevocationEntry {
                identifier: r.get(0)?,
                reason: r.get(1)?,
                revoked_at_ms: r.get(2)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revocation_store_roundtrip() {
        let store = RevocationStore::open("sqlite::memory:").unwrap();
        assert!(!store.is_revoked("node-1"));
        store.revoke("node-1", "bad hardware").unwrap();
        assert!(store.is_revoked("node-1"));
        let list = store.list_revoked().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].identifier, "node-1");
        assert_eq!(list[0].reason, "bad hardware");
        assert!(store.unrevoke("node-1").unwrap());
        assert!(!store.is_revoked("node-1"));
    }
}
