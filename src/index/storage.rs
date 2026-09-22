//! Relational and vector sidecar storage for structural code symbols.

use super::chunk::SymbolChunk;
use super::symbol::{CodeSymbol, SymbolKind};
use crate::types::now_ms;
use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};

/// Structural index store backed by SQLite relational tables and sidecar vectors.
#[derive(Clone)]
pub struct StructuralStorage {
    conn: Arc<Mutex<Connection>>,
}

impl StructuralStorage {
    /// Opens or creates an index storage database.
    pub fn open(db_url: &str) -> Result<Self, crate::types::Error> {
        let conn = if db_url == "sqlite::memory:" {
            Connection::open_in_memory()
        } else {
            let p = db_url
                .trim_start_matches("sqlite://")
                .trim_start_matches("sqlite:");
            Connection::open(p)
        }
        .map_err(crate::types::Error::Db)?;

        let storage = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        storage.init_schema()?;
        Ok(storage)
    }

    fn init_schema(&self) -> Result<(), crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS structural_symbols (
                symbol_id TEXT PRIMARY KEY,
                repository_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                language TEXT NOT NULL,
                qualified_name TEXT NOT NULL,
                kind TEXT NOT NULL,
                start_byte INTEGER NOT NULL,
                end_byte INTEGER NOT NULL,
                start_line INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                signature TEXT,
                parent_symbol_id TEXT,
                content_hash TEXT NOT NULL,
                doc_comment TEXT,
                visibility TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_sym_repo_path ON structural_symbols(repository_id, relative_path);
            CREATE INDEX IF NOT EXISTS idx_sym_qualified ON structural_symbols(qualified_name);
            CREATE INDEX IF NOT EXISTS idx_sym_kind ON structural_symbols(kind);

            CREATE TABLE IF NOT EXISTS structural_chunks (
                chunk_id TEXT PRIMARY KEY,
                symbol_id TEXT NOT NULL,
                repository_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                qualified_name TEXT NOT NULL,
                kind TEXT NOT NULL,
                signature TEXT,
                doc_comment TEXT,
                body TEXT NOT NULL,
                start_line INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                token_count INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                embedding_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_chunk_symbol ON structural_chunks(symbol_id);

            CREATE TABLE IF NOT EXISTS indexed_files (
                relative_path TEXT PRIMARY KEY,
                repository_id TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                last_indexed_ms INTEGER NOT NULL
            );
            ",
        )
        .map_err(crate::types::Error::Db)?;
        Ok(())
    }

    /// Upserts a list of symbols for a given file.
    pub fn upsert_file_symbols(
        &self,
        repo: &str,
        path: &str,
        content_hash: &str,
        symbols: &[CodeSymbol],
    ) -> Result<(), crate::types::Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(crate::types::Error::Db)?;

        // Delete stale symbols for this file
        tx.execute(
            "DELETE FROM structural_symbols WHERE repository_id = ?1 AND relative_path = ?2",
            params![repo, path],
        )
        .map_err(crate::types::Error::Db)?;

        for sym in symbols {
            tx.execute(
                "INSERT INTO structural_symbols (
                    symbol_id, repository_id, relative_path, language, qualified_name,
                    kind, start_byte, end_byte, start_line, end_line, signature,
                    parent_symbol_id, content_hash, doc_comment, visibility
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                params![
                    sym.symbol_id,
                    sym.repository_id,
                    sym.relative_path,
                    sym.language,
                    sym.qualified_name,
                    serde_json::to_string(&sym.kind)
                        .unwrap_or_default()
                        .trim_matches('"'),
                    sym.start_byte as i64,
                    sym.end_byte as i64,
                    sym.start_line as i64,
                    sym.end_line as i64,
                    sym.signature,
                    sym.parent_symbol_id,
                    sym.content_hash,
                    sym.doc_comment,
                    sym.visibility,
                ],
            )
            .map_err(crate::types::Error::Db)?;
        }

        // Record indexed file status
        tx.execute(
            "INSERT INTO indexed_files (relative_path, repository_id, content_hash, last_indexed_ms)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(relative_path) DO UPDATE SET
                content_hash = excluded.content_hash,
                last_indexed_ms = excluded.last_indexed_ms",
            params![path, repo, content_hash, now_ms() as i64],
        )
        .map_err(crate::types::Error::Db)?;

        tx.commit().map_err(crate::types::Error::Db)?;
        Ok(())
    }

    /// Stores a chunk with an optional embedding vector.
    pub fn store_chunk(
        &self,
        chunk: &SymbolChunk,
        embedding: Option<&[f32]>,
    ) -> Result<(), crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let embedding_json = embedding.map(|e| serde_json::to_string(e).unwrap_or_default());

        conn.execute(
            "INSERT INTO structural_chunks (
                chunk_id, symbol_id, repository_id, relative_path, qualified_name,
                kind, signature, doc_comment, body, start_line, end_line,
                token_count, content_hash, embedding_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            ON CONFLICT(chunk_id) DO UPDATE SET
                body = excluded.body,
                token_count = excluded.token_count,
                content_hash = excluded.content_hash,
                embedding_json = excluded.embedding_json",
            params![
                chunk.chunk_id,
                chunk.symbol_id,
                chunk.repository_id,
                chunk.relative_path,
                chunk.qualified_name,
                serde_json::to_string(&chunk.kind)
                    .unwrap_or_default()
                    .trim_matches('"'),
                chunk.signature,
                chunk.doc_comment,
                chunk.body,
                chunk.start_line as i64,
                chunk.end_line as i64,
                chunk.token_count as i64,
                chunk.content_hash,
                embedding_json,
            ],
        )
        .map_err(crate::types::Error::Db)?;
        Ok(())
    }

    /// Finds symbols by exact qualified name.
    pub fn find_by_qualified_name(
        &self,
        name: &str,
    ) -> Result<Vec<CodeSymbol>, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT symbol_id, repository_id, relative_path, language, qualified_name,
                        kind, start_byte, end_byte, start_line, end_line, signature,
                        parent_symbol_id, content_hash, doc_comment, visibility
                 FROM structural_symbols
                 WHERE qualified_name = ?1 OR qualified_name LIKE ?2",
            )
            .map_err(crate::types::Error::Db)?;

        let like_pat = format!("%::{}", name);
        let rows = stmt
            .query_map(params![name, like_pat], parse_symbol_row)
            .map_err(crate::types::Error::Db)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(crate::types::Error::Db)?);
        }
        Ok(results)
    }

    /// Finds all symbols in a given file.
    pub fn get_file_symbols(&self, path: &str) -> Result<Vec<CodeSymbol>, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT symbol_id, repository_id, relative_path, language, qualified_name,
                        kind, start_byte, end_byte, start_line, end_line, signature,
                        parent_symbol_id, content_hash, doc_comment, visibility
                 FROM structural_symbols
                 WHERE relative_path = ?1
                 ORDER BY start_line ASC",
            )
            .map_err(crate::types::Error::Db)?;

        let rows = stmt
            .query_map(params![path], parse_symbol_row)
            .map_err(crate::types::Error::Db)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(crate::types::Error::Db)?);
        }
        Ok(results)
    }

    /// Retrieves all stored chunks and their optional embeddings for vector/hybrid search.
    pub fn get_all_chunks(
        &self,
    ) -> Result<Vec<(SymbolChunk, Option<Vec<f32>>)>, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT chunk_id, symbol_id, repository_id, relative_path, qualified_name,
                        kind, signature, doc_comment, body, start_line, end_line,
                        token_count, content_hash, embedding_json
                 FROM structural_chunks",
            )
            .map_err(crate::types::Error::Db)?;

        let rows = stmt
            .query_map([], |row| {
                let kind_str: String = row.get(5)?;
                let kind = parse_kind(&kind_str);
                let emb_json: Option<String> = row.get(13)?;
                let embedding = emb_json
                    .as_deref()
                    .and_then(|s| serde_json::from_str::<Vec<f32>>(s).ok());

                let chunk = SymbolChunk {
                    chunk_id: row.get(0)?,
                    symbol_id: row.get(1)?,
                    repository_id: row.get(2)?,
                    relative_path: row.get(3)?,
                    qualified_name: row.get(4)?,
                    kind,
                    signature: row.get(6)?,
                    doc_comment: row.get(7)?,
                    body: row.get(8)?,
                    start_line: row.get::<_, i64>(9)? as usize,
                    end_line: row.get::<_, i64>(10)? as usize,
                    token_count: row.get::<_, i64>(11)? as usize,
                    content_hash: row.get(12)?,
                };
                Ok((chunk, embedding))
            })
            .map_err(crate::types::Error::Db)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(crate::types::Error::Db)?);
        }
        Ok(results)
    }

    /// Purges orphaned chunks whose symbol_id is no longer present in structural_symbols.
    pub fn purge_orphaned_chunks(&self) -> Result<usize, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn
            .execute(
                "DELETE FROM structural_chunks WHERE symbol_id NOT IN (SELECT symbol_id FROM structural_symbols)",
                [],
            )
            .map_err(crate::types::Error::Db)?;
        Ok(deleted)
    }

    /// Lists all code symbols in the database.
    pub fn list_all_symbols(&self) -> Result<Vec<CodeSymbol>, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT symbol_id, repository_id, relative_path, language, qualified_name,
                        kind, start_byte, end_byte, start_line, end_line, signature,
                        parent_symbol_id, content_hash, doc_comment, visibility
                 FROM structural_symbols ORDER BY repository_id, relative_path, start_line",
            )
            .map_err(crate::types::Error::Db)?;

        let rows = stmt
            .query_map([], parse_symbol_row)
            .map_err(crate::types::Error::Db)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(crate::types::Error::Db)?);
        }
        Ok(results)
    }

    /// Lists all stored symbol chunks in the database.
    pub fn list_all_chunks(&self) -> Result<Vec<SymbolChunk>, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT chunk_id, symbol_id, repository_id, relative_path, qualified_name,
                        kind, signature, doc_comment, body, start_line, end_line, token_count, content_hash
                 FROM structural_chunks ORDER BY repository_id, relative_path, start_line",
            )
            .map_err(crate::types::Error::Db)?;

        let rows = stmt
            .query_map([], |row| {
                let kind_str: String = row.get(5)?;
                Ok(SymbolChunk {
                    chunk_id: row.get(0)?,
                    symbol_id: row.get(1)?,
                    repository_id: row.get(2)?,
                    relative_path: row.get(3)?,
                    qualified_name: row.get(4)?,
                    kind: parse_kind(&kind_str),
                    signature: row.get(6)?,
                    doc_comment: row.get(7)?,
                    body: row.get(8)?,
                    start_line: row.get::<_, i64>(9)? as usize,
                    end_line: row.get::<_, i64>(10)? as usize,
                    token_count: row.get::<_, i64>(11)? as usize,
                    content_hash: row.get(12)?,
                })
            })
            .map_err(crate::types::Error::Db)?;

        let mut results = Vec::new();
        for r in rows {
            results.push(r.map_err(crate::types::Error::Db)?);
        }
        Ok(results)
    }

    /// Returns the total count of symbols stored.
    pub fn count_symbols(&self) -> Result<usize, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM structural_symbols", [], |row| {
                row.get(0)
            })
            .map_err(crate::types::Error::Db)?;
        Ok(count as usize)
    }

    /// Returns the total count of chunks stored.
    pub fn count_chunks(&self) -> Result<usize, crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM structural_chunks", [], |row| {
                row.get(0)
            })
            .map_err(crate::types::Error::Db)?;
        Ok(count as usize)
    }

    /// Executes VACUUM on the database connection.
    pub fn vacuum(&self) -> Result<(), crate::types::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("VACUUM", [])
            .map_err(crate::types::Error::Db)?;
        Ok(())
    }
}

fn parse_kind(s: &str) -> SymbolKind {
    match s {
        "module" => SymbolKind::Module,
        "struct" => SymbolKind::Struct,
        "enum" => SymbolKind::Enum,
        "variant" => SymbolKind::EnumVariant,
        "trait" => SymbolKind::Trait,
        "impl" => SymbolKind::Impl,
        "function" => SymbolKind::Function,
        "method" => SymbolKind::Method,
        "const" => SymbolKind::Const,
        "static" => SymbolKind::Static,
        "type_alias" => SymbolKind::TypeAlias,
        "macro" => SymbolKind::Macro,
        "test" => SymbolKind::Test,
        "field" => SymbolKind::Field,
        _ => SymbolKind::Function,
    }
}

fn parse_symbol_row(row: &rusqlite::Row) -> rusqlite::Result<CodeSymbol> {
    let kind_str: String = row.get(5)?;
    let kind = parse_kind(&kind_str);

    Ok(CodeSymbol {
        symbol_id: row.get(0)?,
        repository_id: row.get(1)?,
        relative_path: row.get(2)?,
        language: row.get(3)?,
        qualified_name: row.get(4)?,
        kind,
        start_byte: row.get::<_, i64>(6)? as usize,
        end_byte: row.get::<_, i64>(7)? as usize,
        start_line: row.get::<_, i64>(8)? as usize,
        end_line: row.get::<_, i64>(9)? as usize,
        signature: row.get(10)?,
        parent_symbol_id: row.get(11)?,
        content_hash: row.get(12)?,
        doc_comment: row.get(13)?,
        visibility: row.get(14)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_upsert_and_query() {
        let storage = StructuralStorage::open("sqlite::memory:").unwrap();
        let sym = CodeSymbol {
            symbol_id: "s1".into(),
            repository_id: "repo".into(),
            relative_path: "src/main.rs".into(),
            language: "rust".into(),
            qualified_name: "AppState::init".into(),
            kind: SymbolKind::Method,
            start_byte: 0,
            end_byte: 100,
            start_line: 1,
            end_line: 5,
            signature: Some("pub fn init()".into()),
            parent_symbol_id: None,
            content_hash: "h1".into(),
            doc_comment: Some("Initializes app".into()),
            visibility: Some("pub".into()),
        };

        storage
            .upsert_file_symbols("repo", "src/main.rs", "filehash", &[sym.clone()])
            .unwrap();

        let found = storage.find_by_qualified_name("AppState::init").unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].qualified_name, "AppState::init");

        let by_suffix = storage.find_by_qualified_name("init").unwrap();
        assert_eq!(by_suffix.len(), 1);
    }
}
