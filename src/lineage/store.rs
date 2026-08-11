//! SQLite-backed lineage graph: parent/child relationships, history, queries.
//!
//! Node 1 is the canonical authority for relationship truth.
//! Every node except Node 1 has exactly ONE authoritative parent.
//! Relationship history is preserved; re-parenting is an explicit auditable event.

use crate::types::{now_ms, Error, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tracing::{debug, info};

/// Status of a node relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelationshipStatus {
    /// Active parent-child relationship.
    Active,
    /// Node was re-parented; this relationship is superseded but preserved.
    Superseded,
    /// Node was explicitly revoked from the federation.
    Revoked,
}

impl RelationshipStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Superseded => "superseded",
            Self::Revoked => "revoked",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "active" => Self::Active,
            "superseded" => Self::Superseded,
            "revoked" => Self::Revoked,
            _ => Self::Active,
        }
    }
}

/// A recorded parent-child relationship.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRelationship {
    pub node_id: String,
    pub parent_node_id: Option<String>,
    pub root_node_id: String,
    pub branch_id: String,
    pub depth: u32,
    pub status: RelationshipStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// SQLite-backed lineage store.
pub struct LineageStore {
    conn: Mutex<Connection>,
}

impl LineageStore {
    /// Open (or create) a lineage store at the given SQLite connection.
    pub fn new(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS lineage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                parent_node_id TEXT,
                root_node_id TEXT NOT NULL,
                branch_id TEXT NOT NULL DEFAULT '',
                depth INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_lineage_node ON lineage(node_id, status);
            CREATE INDEX IF NOT EXISTS idx_lineage_parent ON lineage(parent_node_id, status);
            CREATE INDEX IF NOT EXISTS idx_lineage_root ON lineage(root_node_id);
            ",
        )
        .map_err(|e| Error::Internal(format!("lineage schema: {e}")))?;

        info!("lineage store initialized");
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Open a lineage store from a database URL (e.g. "sqlite:nauti.db" or ":memory:").
    pub fn open(db_url: &str) -> Result<Self> {
        let conn = if db_url == ":memory:" || db_url == "sqlite::memory:" {
            Connection::open_in_memory()
                .map_err(|e| Error::Internal(format!("open memory db: {e}")))?
        } else {
            // Strip "sqlite:" prefix if present
            let path = db_url.strip_prefix("sqlite:").unwrap_or(db_url);
            Connection::open(path)
                .map_err(|e| Error::Internal(format!("open db {path}: {e}")))?
        };
        Self::new(conn)
    }

    /// Set the root node (Node 1). Must be called once at startup.
    pub fn set_root(&self, root_node_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // Check if root already exists
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM lineage WHERE node_id = ?1 AND parent_node_id IS NULL",
                params![root_node_id],
                |row| row.get::<_, i64>(0),
            )
            .map(|c| c > 0)
            .unwrap_or(false);

        if exists {
            debug!(node_id = %root_node_id, "root already registered");
            return Ok(());
        }

        let now = now_ms();
        conn.execute(
            "INSERT INTO lineage (node_id, parent_node_id, root_node_id, branch_id, depth, status, created_at_ms, updated_at_ms)
             VALUES (?1, NULL, ?1, 'root', 0, 'active', ?2, ?2)",
            params![root_node_id, now],
        )
        .map_err(|e| Error::Internal(format!("set_root: {e}")))?;
        info!(node_id = %root_node_id, "root node registered in lineage");
        Ok(())
    }

    /// Register a new node under a parent. Returns the new relationship.
    /// Prevents cycles, self-parenting, and duplicate identity.
    pub fn add_child(
        &self,
        child_id: &str,
        parent_id: &str,
    ) -> Result<NodeRelationship> {
        // Validate: no self-parenting
        if child_id == parent_id {
            return Err(Error::Internal("cannot parent a node to itself".into()));
        }

        let conn = self.conn.lock().unwrap();
        let now = now_ms();

        // Validate: child must not already have an active parent
        let existing: Option<String> = conn
            .query_row(
                "SELECT parent_node_id FROM lineage WHERE node_id = ?1 AND status = 'active'",
                params![child_id],
                |row| row.get(0),
            )
            .ok()
            .flatten();
        if existing.is_some() {
            return Err(Error::Internal(format!(
                "node {child_id} already has an active parent"
            )));
        }

        // Validate: parent must exist in the lineage
        let parent_depth: u32 = conn
            .query_row(
                "SELECT depth FROM lineage WHERE node_id = ?1 AND status = 'active'",
                params![parent_id],
                |row| row.get(0),
            )
            .map_err(|_| Error::Internal(format!("parent {parent_id} not found in lineage")))?;

        // Validate: cycle check — child must not be an ancestor of parent
        if self.is_ancestor_of_locked(&conn, child_id, parent_id)? {
            return Err(Error::Internal(format!(
                "adding {child_id} as child of {parent_id} would create a cycle"
            )));
        }

        // Get root and branch from parent
        let (root_id, branch_id): (String, String) = conn
            .query_row(
                "SELECT root_node_id, branch_id FROM lineage WHERE node_id = ?1 AND status = 'active'",
                params![parent_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| Error::Internal(format!("parent {parent_id} not found")))?;

        let new_depth = parent_depth + 1;
        let new_branch = if branch_id.is_empty() || branch_id == "root" {
            format!("branch-{child_id}")
        } else {
            branch_id.clone()
        };

        conn.execute(
            "INSERT INTO lineage (node_id, parent_node_id, root_node_id, branch_id, depth, status, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, ?6)",
            params![child_id, parent_id, root_id, new_branch, new_depth, now],
        )
        .map_err(|e| Error::Internal(format!("add_child: {e}")))?;

        let rel = NodeRelationship {
            node_id: child_id.to_string(),
            parent_node_id: Some(parent_id.to_string()),
            root_node_id: root_id,
            branch_id: new_branch,
            depth: new_depth,
            status: RelationshipStatus::Active,
            created_at_ms: now,
            updated_at_ms: now,
        };

        info!(child = %child_id, parent = %parent_id, depth = new_depth, "child added to lineage");
        Ok(rel)
    }

    /// Re-parent a node. The old relationship is marked Superseded.
    pub fn reparent(&self, node_id: &str, new_parent_id: &str) -> Result<NodeRelationship> {
        if node_id == new_parent_id {
            return Err(Error::Internal("cannot parent a node to itself".into()));
        }

        let conn = self.conn.lock().unwrap();
        let now = now_ms();

        // Supersede the old active relationship
        conn.execute(
            "UPDATE lineage SET status = 'superseded', updated_at_ms = ?1 WHERE node_id = ?2 AND status = 'active'",
            params![now, node_id],
        )
        .map_err(|e| Error::Internal(format!("reparent supersede: {e}")))?;

        // Get new parent info
        let (parent_depth, root_id, branch_id): (u32, String, String) = conn
            .query_row(
                "SELECT depth, root_node_id, branch_id FROM lineage WHERE node_id = ?1 AND status = 'active'",
                params![new_parent_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|_| Error::Internal(format!("new parent {new_parent_id} not found")))?;

        // Cycle check
        if self.is_ancestor_of_locked(&conn, node_id, new_parent_id)? {
            return Err(Error::Internal(format!(
                "reparenting {node_id} under {new_parent_id} would create a cycle"
            )));
        }

        let new_depth = parent_depth + 1;
        let new_branch = if branch_id.is_empty() || branch_id == "root" {
            format!("branch-{node_id}")
        } else {
            branch_id.clone()
        };

        conn.execute(
            "INSERT INTO lineage (node_id, parent_node_id, root_node_id, branch_id, depth, status, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, ?6)",
            params![node_id, new_parent_id, root_id, new_branch, new_depth, now],
        )
        .map_err(|e| Error::Internal(format!("reparent insert: {e}")))?;

        let rel = NodeRelationship {
            node_id: node_id.to_string(),
            parent_node_id: Some(new_parent_id.to_string()),
            root_node_id: root_id,
            branch_id: new_branch,
            depth: new_depth,
            status: RelationshipStatus::Active,
            created_at_ms: now,
            updated_at_ms: now,
        };

        info!(node = %node_id, new_parent = %new_parent_id, "node reparented");
        Ok(rel)
    }

    /// Revoke a node from the federation.
    pub fn revoke(&self, node_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = now_ms();
        conn.execute(
            "UPDATE lineage SET status = 'revoked', updated_at_ms = ?1 WHERE node_id = ?2 AND status = 'active'",
            params![now, node_id],
        )
        .map_err(|e| Error::Internal(format!("revoke: {e}")))?;
        info!(node = %node_id, "node revoked from lineage");
        Ok(())
    }

    /// Get the active relationship for a node.
    pub fn get_relationship(&self, node_id: &str) -> Result<Option<NodeRelationship>> {
        let conn = self.conn.lock().unwrap();
        self.get_relationship_locked(&conn, node_id)
    }

    fn get_relationship_locked(&self, conn: &Connection, node_id: &str) -> Result<Option<NodeRelationship>> {
        let mut stmt = conn
            .prepare(
                "SELECT node_id, parent_node_id, root_node_id, branch_id, depth, status, created_at_ms, updated_at_ms
                 FROM lineage WHERE node_id = ?1 AND status = 'active'",
            )
            .map_err(|e| Error::Internal(format!("prepare: {e}")))?;

        let row = stmt.query_row(params![node_id], |row| {
            Ok(NodeRelationship {
                node_id: row.get(0)?,
                parent_node_id: row.get(1)?,
                root_node_id: row.get(2)?,
                branch_id: row.get(3)?,
                depth: row.get(4)?,
                status: RelationshipStatus::from_str(&row.get::<_, String>(5)?),
                created_at_ms: row.get(6)?,
                updated_at_ms: row.get(7)?,
            })
        });

        match row {
            Ok(r) => Ok(Some(r)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(Error::Internal(format!("get_relationship: {e}"))),
        }
    }

    /// Get the parent of a node.
    pub fn parent(&self, node_id: &str) -> Result<Option<String>> {
        let rel = self.get_relationship(node_id)?;
        Ok(rel.and_then(|r| r.parent_node_id))
    }

    /// Get the children of a node.
    pub fn children(&self, node_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT node_id FROM lineage WHERE parent_node_id = ?1 AND status = 'active'")
            .map_err(|e| Error::Internal(format!("prepare: {e}")))?;

        let rows = stmt
            .query_map(params![node_id], |row| row.get::<_, String>(0))
            .map_err(|e| Error::Internal(format!("children query: {e}")))?;

        let mut result = Vec::new();
        for row in rows {
            if let Ok(id) = row {
                result.push(id);
            }
        }
        Ok(result)
    }

    /// Get all ancestors of a node (walking up the tree to root).
    pub fn ancestors(&self, node_id: &str) -> Result<Vec<String>> {
        let mut ancestors = Vec::new();
        let mut current = node_id.to_string();
        loop {
            let parent = self.parent(&current)?;
            match parent {
                Some(p) if p == current => break, // root's parent is itself
                Some(p) => {
                    ancestors.push(p.clone());
                    current = p;
                }
                None => break,
            }
        }
        Ok(ancestors)
    }

    /// Get all descendants of a node (BFS).
    pub fn descendants(&self, node_id: &str) -> Result<Vec<String>> {
        let mut descendants = Vec::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(node_id.to_string());
        while let Some(current) = queue.pop_front() {
            let kids = self.children(&current)?;
            for kid in kids {
                if kid != node_id {
                    descendants.push(kid.clone());
                    queue.push_back(kid);
                }
            }
        }
        Ok(descendants)
    }

    /// Check if `potential_ancestor` is an ancestor of `node_id`.
    pub fn is_ancestor_of(&self, potential_ancestor: &str, node_id: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        self.is_ancestor_of_locked(&conn, potential_ancestor, node_id)
    }

    fn is_ancestor_of_locked(&self, conn: &Connection, potential_ancestor: &str, node_id: &str) -> Result<bool> {
        let mut current = node_id.to_string();
        loop {
            let parent: Option<String> = conn
                .query_row(
                    "SELECT parent_node_id FROM lineage WHERE node_id = ?1 AND status = 'active'",
                    params![current],
                    |row| row.get(0),
                )
                .ok()
                .flatten();

            match parent {
                Some(ref p) if p == potential_ancestor => return Ok(true),
                Some(ref p) if p == &current => return Ok(false), // root
                Some(p) => current = p,
                None => return Ok(false),
            }
        }
    }

    /// Check if two nodes belong to the same lineage (one is ancestor of the other,
    /// or they share a configured branch root).
    /// Check if two nodes belong to the same lineage.
    /// Two nodes are same-lineage if:
    ///   - they share the same root AND
    ///   - they share the same branch, OR one is ancestor of the other, OR they are siblings
    pub fn same_lineage(&self, a: &str, b: &str) -> Result<bool> {
        if a == b {
            return Ok(true);
        }
        let rel_a = self.get_relationship(a)?;
        let rel_b = self.get_relationship(b)?;
        match (rel_a, rel_b) {
            (Some(ra), Some(rb)) => {
                if ra.root_node_id != rb.root_node_id {
                    return Ok(false);
                }
                // Same branch
                if ra.branch_id == rb.branch_id {
                    return Ok(true);
                }
                // One is ancestor of the other
                if self.is_ancestor_of(a, b)? || self.is_ancestor_of(b, a)? {
                    return Ok(true);
                }
                // Siblings: same parent
                if ra.parent_node_id.is_some() && ra.parent_node_id == rb.parent_node_id {
                    return Ok(true);
                }
                // Same root = same lineage (federation membership)
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Compute lineage distance between two nodes.
    /// Returns the number of edges between them, or None if unrelated.
    pub fn lineage_distance(&self, a: &str, b: &str) -> Result<Option<u32>> {
        if a == b {
            return Ok(Some(0));
        }
        let rel_a = self.get_relationship(a)?;
        let rel_b = self.get_relationship(b)?;
        match (rel_a, rel_b) {
            (Some(ra), Some(rb)) => {
                // Same root required
                if ra.root_node_id != rb.root_node_id {
                    return Ok(None);
                }
                // Walk ancestors of both, find common ancestor, sum distances
                let ancestors_a = self.ancestors(a)?;
                let ancestors_b = self.ancestors(b)?;
                // ancestors list is [parent, grandparent, ...], so index 0 = 1 edge up
                for (i, anc_a) in ancestors_a.iter().enumerate() {
                    if let Some(j) = ancestors_b.iter().position(|x| x == anc_a) {
                        // distance = (i+1) edges from a to common ancestor + (j+1) edges from b to common ancestor
                        return Ok(Some((i + 1 + j + 1) as u32));
                    }
                }
                // One might be the direct ancestor of the other
                if ancestors_b.contains(&a.to_string()) {
                    let pos = ancestors_b.iter().position(|x| x == a).unwrap();
                    return Ok(Some((pos + 1) as u32));
                }
                if ancestors_a.contains(&b.to_string()) {
                    let pos = ancestors_a.iter().position(|x| x == b).unwrap();
                    return Ok(Some((pos + 1) as u32));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Find the common ancestor of two nodes.
    pub fn common_ancestor(&self, a: &str, b: &str) -> Result<Option<String>> {
        let ancestors_a = self.ancestors(a)?;
        let ancestors_b = self.ancestors(b)?;

        for anc in &ancestors_a {
            if ancestors_b.contains(anc) {
                return Ok(Some(anc.clone()));
            }
        }
        Ok(None)
    }

    /// Get all active relationships in the tree.
    pub fn all_active(&self) -> Result<Vec<NodeRelationship>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT node_id, parent_node_id, root_node_id, branch_id, depth, status, created_at_ms, updated_at_ms
                 FROM lineage WHERE status = 'active'",
            )
            .map_err(|e| Error::Internal(format!("prepare: {e}")))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(NodeRelationship {
                    node_id: row.get(0)?,
                    parent_node_id: row.get(1)?,
                    root_node_id: row.get(2)?,
                    branch_id: row.get(3)?,
                    depth: row.get(4)?,
                    status: RelationshipStatus::from_str(&row.get::<_, String>(5)?),
                    created_at_ms: row.get(6)?,
                    updated_at_ms: row.get(7)?,
                })
            })
            .map_err(|e| Error::Internal(format!("all_active: {e}")))?;

        let mut result = Vec::new();
        for row in rows {
            if let Ok(r) = row {
                result.push(r);
            }
        }
        Ok(result)
    }

    /// Get the root node ID.
    pub fn root_node_id(&self) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let r = conn
            .query_row(
                "SELECT node_id FROM lineage WHERE parent_node_id IS NULL AND status = 'active' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .ok();
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> LineageStore {
        LineageStore::open(":memory:").unwrap()
    }

    #[test]
    fn set_root_and_query() {
        let store = test_store();
        store.set_root("node-1").unwrap();
        assert_eq!(store.root_node_id().unwrap(), Some("node-1".into()));
        let rel = store.get_relationship("node-1").unwrap().unwrap();
        assert!(rel.parent_node_id.is_none());
        assert_eq!(rel.depth, 0);
    }

    #[test]
    fn add_child_and_query() {
        let store = test_store();
        store.set_root("root").unwrap();
        let rel = store.add_child("child-1", "root").unwrap();
        assert_eq!(rel.parent_node_id, Some("root".into()));
        assert_eq!(rel.depth, 1);
        assert_eq!(store.parent("child-1").unwrap(), Some("root".into()));
        assert_eq!(store.children("root").unwrap(), vec!["child-1"]);
    }

    #[test]
    fn prevent_self_parenting() {
        let store = test_store();
        store.set_root("root").unwrap();
        assert!(store.add_child("root", "root").is_err());
    }

    #[test]
    fn prevent_duplicate_parent() {
        let store = test_store();
        store.set_root("root").unwrap();
        store.add_child("child", "root").unwrap();
        assert!(store.add_child("child", "root").is_err());
    }

    #[test]
    fn prevent_cycle() {
        let store = test_store();
        store.set_root("root").unwrap();
        store.add_child("a", "root").unwrap();
        store.add_child("b", "a").unwrap();
        // root -> a -> b, then try to add root as child of b (cycle)
        assert!(store.add_child("root", "b").is_err());
    }

    #[test]
    fn ancestors_and_descendants() {
        let store = test_store();
        store.set_root("root").unwrap();
        store.add_child("a", "root").unwrap();
        store.add_child("b", "a").unwrap();
        store.add_child("c", "a").unwrap();

        let anc = store.ancestors("b").unwrap();
        assert_eq!(anc, vec!["a", "root"]);

        let desc = store.descendants("root").unwrap();
        assert_eq!(desc.len(), 3);
        assert!(desc.contains(&"a".to_string()));
        assert!(desc.contains(&"b".to_string()));
        assert!(desc.contains(&"c".to_string()));
    }

    #[test]
    fn same_lineage() {
        let store = test_store();
        store.set_root("root").unwrap();
        store.add_child("a", "root").unwrap();
        store.add_child("b", "a").unwrap();
        store.add_child("x", "root").unwrap();

        assert!(store.same_lineage("root", "b").unwrap());
        assert!(store.same_lineage("a", "b").unwrap());
        // a and x share root/branch, so same_lineage should be true
        assert!(store.same_lineage("a", "x").unwrap());
    }

    #[test]
    fn lineage_distance() {
        let store = test_store();
        store.set_root("root").unwrap();
        store.add_child("a", "root").unwrap();
        store.add_child("b", "a").unwrap();

        assert_eq!(store.lineage_distance("root", "b").unwrap(), Some(2));
        assert_eq!(store.lineage_distance("root", "a").unwrap(), Some(1));
        assert_eq!(store.lineage_distance("root", "root").unwrap(), Some(0));
    }

    #[test]
    fn reparent_preserves_history() {
        let store = test_store();
        store.set_root("root").unwrap();
        store.add_child("a", "root").unwrap();
        store.reparent("a", "root").unwrap(); // re-parent under same parent (still tests supersede logic)
        // The old relationship should be superseded
        let conn = store.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM lineage WHERE node_id = 'a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2); // original + reparented
    }

    #[test]
    fn common_ancestor() {
        let store = test_store();
        store.set_root("root").unwrap();
        store.add_child("a", "root").unwrap();
        store.add_child("b", "a").unwrap();
        store.add_child("c", "a").unwrap();

        let ca = store.common_ancestor("b", "c").unwrap();
        assert_eq!(ca, Some("a".to_string()));
    }
}