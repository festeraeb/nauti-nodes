use crate::types::{Error, Result};
use rusqlite::{params, Connection};
use std::sync::Mutex;

pub struct QuotaStore {
    conn: Mutex<Connection>,
}

impl QuotaStore {
    pub fn open(db_url: &str) -> Result<Self> {
        let conn = if db_url == ":memory:" || db_url == "sqlite::memory:" {
            Connection::open_in_memory()?
        } else {
            let path = db_url.strip_prefix("sqlite:").unwrap_or(db_url);
            Connection::open(path)?
        };
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS api_quotas (
                api_key TEXT PRIMARY KEY,
                remaining_tokens INTEGER NOT NULL
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn set_balance(&self, api_key: &str, tokens: i64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "INSERT INTO api_quotas (api_key, remaining_tokens) VALUES (?1, ?2)
             ON CONFLICT(api_key) DO UPDATE SET remaining_tokens = excluded.remaining_tokens",
            params![api_key, tokens],
        )?;
        Ok(())
    }

    pub fn check_quota(&self, api_key: &str, need: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let rem: i64 = match conn.query_row(
            "SELECT remaining_tokens FROM api_quotas WHERE api_key = ?1",
            params![api_key],
            |r| r.get(0),
        ) {
            Ok(v) => v,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                let default_tokens: i64 = 1_000_000;
                let _ = conn.execute(
                    "INSERT INTO api_quotas (api_key, remaining_tokens) VALUES (?1, ?2)",
                    params![api_key, default_tokens],
                );
                default_tokens
            }
            Err(e) => return Err(e.into()),
        };
        if rem < need as i64 {
            return Err(Error::QuotaExceeded);
        }
        Ok(())
    }

    pub fn reserve_tokens(&self, api_key: &str, amount: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        let amt = amount as i64;
        let updated = conn.execute(
            "UPDATE api_quotas SET remaining_tokens = remaining_tokens - ?1
             WHERE api_key = ?2 AND remaining_tokens >= ?3",
            params![amt, api_key, amt],
        )?;
        if updated == 0 {
            return Err(Error::QuotaExceeded);
        }
        Ok(())
    }

    pub fn release_tokens(&self, api_key: &str, amount: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| Error::Internal(e.to_string()))?;
        conn.execute(
            "UPDATE api_quotas SET remaining_tokens = remaining_tokens + ?1 WHERE api_key = ?2",
            params![amount as i64, api_key],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_and_release() {
        let q = QuotaStore::open("sqlite::memory:").unwrap();
        q.set_balance("k1", 1000).unwrap();
        q.check_quota("k1", 100).unwrap();
        q.reserve_tokens("k1", 400).unwrap();
        assert!(q.check_quota("k1", 700).is_err());
        q.release_tokens("k1", 200).unwrap();
        q.check_quota("k1", 500).unwrap();
    }
}
