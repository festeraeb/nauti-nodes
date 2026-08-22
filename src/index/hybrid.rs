//! Hybrid multi-stage structural, lexical, compiler-memory, and vector retrieval engine.

use super::compiler_memory::{CompilerIncident, CompilerMemoryStore};
use super::embedder::Embedder;
use super::storage::StructuralStorage;
use super::symbol::SymbolKind;
use serde::{Deserialize, Serialize};

/// A retrieved code symbol result with exact match provenance and ranking breakdown.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrievalResult {
    pub symbol_id: String,
    pub repository_id: String,
    pub relative_path: String,
    pub qualified_name: String,
    pub kind: SymbolKind,
    pub signature: Option<String>,
    pub doc_comment: Option<String>,
    pub body: String,
    pub start_line: usize,
    pub end_line: usize,
    pub score: f32,
    pub match_reasons: Vec<String>,
    pub raw_vector_score: Option<f32>,
    pub compiler_incidents: Vec<CompilerIncident>,
}

/// Hybrid retrieval coordinator.
pub struct HybridRetriever {
    storage: StructuralStorage,
    compiler_memory: CompilerMemoryStore,
    embedder: Option<Box<dyn Embedder>>,
}

impl HybridRetriever {
    pub fn new(
        storage: StructuralStorage,
        compiler_memory: CompilerMemoryStore,
        embedder: Option<Box<dyn Embedder>>,
    ) -> Self {
        Self {
            storage,
            compiler_memory,
            embedder,
        }
    }

    /// Performs multi-stage hybrid retrieval across AST symbols, compiler incidents, and vector embeddings.
    pub async fn query(
        &self,
        query_str: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<RetrievalResult>> {
        let query_tokens: Vec<&str> = query_str.split_whitespace().collect();
        let query_lower = query_str.to_lowercase();

        // 1. Check if query matches a compiler error code (e.g. "E0308", "E0382")
        let compiler_incidents = self
            .compiler_memory
            .find_by_error_code(query_str)
            .unwrap_or_default();

        // 2. Compute query embedding if real embedder is present
        let query_embedding = if let Some(ref emb) = self.embedder {
            emb.embed(query_str).await.ok()
        } else {
            None
        };

        // 3. Retrieve all structural chunks from storage
        let chunks_with_emb = self.storage.get_all_chunks().unwrap_or_default();
        let mut scored_results: Vec<RetrievalResult> = Vec::new();

        for (chunk, emb) in chunks_with_emb {
            let mut score = 0.0f32;
            let mut reasons = Vec::new();
            let mut raw_vec_score = None;

            // (A) Exact Symbol / Qualified-Name match
            if chunk.qualified_name == query_str {
                score += 100.0;
                reasons.push("exact_qualified_name_match".to_string());
            } else if chunk.qualified_name.to_lowercase() == query_lower {
                score += 80.0;
                reasons.push("case_insensitive_name_match".to_string());
            } else if chunk.qualified_name.ends_with(&format!("::{}", query_str)) {
                score += 60.0;
                reasons.push("suffix_name_match".to_string());
            }

            // (B) Path / Module match
            if chunk.relative_path.contains(query_str) {
                score += 25.0;
                reasons.push("path_match".to_string());
            }

            // (C) Compiler Incident linkage
            let mut linked_incidents = Vec::new();
            for inc in &compiler_incidents {
                if inc.affected_symbol_ids.contains(&chunk.symbol_id) {
                    score += 40.0;
                    reasons.push(format!("compiler_incident_match_{}", inc.incident_id));
                    linked_incidents.push(inc.clone());
                }
            }

            // Also check compiler memory by symbol ID
            if let Ok(sym_incs) = self.compiler_memory.find_by_symbol_id(&chunk.symbol_id) {
                for inc in sym_incs {
                    if !linked_incidents
                        .iter()
                        .any(|i| i.incident_id == inc.incident_id)
                    {
                        score += 15.0;
                        reasons.push(format!("past_incident_repair_{}", inc.incident_id));
                        linked_incidents.push(inc);
                    }
                }
            }

            // (D) Lexical token overlap
            let body_lower = chunk.body.to_lowercase();
            let mut matched_tokens = 0;
            for token in &query_tokens {
                if body_lower.contains(&token.to_lowercase()) {
                    matched_tokens += 1;
                }
            }
            if matched_tokens > 0 {
                let lexical_score = (matched_tokens as f32 / query_tokens.len() as f32) * 20.0;
                score += lexical_score;
                reasons.push(format!(
                    "lexical_overlap_{}/{}",
                    matched_tokens,
                    query_tokens.len()
                ));
            }

            // (E) Real Vector Cosine Similarity (if both query and chunk embedding are present)
            if let (Some(q_emb), Some(c_emb)) = (&query_embedding, &emb) {
                let sim = cosine_similarity(q_emb, c_emb);
                raw_vec_score = Some(sim);
                if sim > 0.6 {
                    let vec_points = sim * 15.0;
                    score += vec_points;
                    reasons.push(format!("vector_similarity_{:.3}", sim));
                }
            }

            if score > 0.0 {
                scored_results.push(RetrievalResult {
                    symbol_id: chunk.symbol_id,
                    repository_id: chunk.repository_id,
                    relative_path: chunk.relative_path,
                    qualified_name: chunk.qualified_name,
                    kind: chunk.kind,
                    signature: chunk.signature,
                    doc_comment: chunk.doc_comment,
                    body: chunk.body,
                    start_line: chunk.start_line,
                    end_line: chunk.end_line,
                    score,
                    match_reasons: reasons,
                    raw_vector_score: raw_vec_score,
                    compiler_incidents: linked_incidents,
                });
            }
        }

        // Sort descending by score
        scored_results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored_results.truncate(limit);

        Ok(scored_results)
    }
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    if norm_a <= 0.0 || norm_b <= 0.0 {
        0.0
    } else {
        dot / (norm_a.sqrt() * norm_b.sqrt())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![1.0, 0.0, 0.0];
        let v3 = vec![0.0, 1.0, 0.0];

        assert!((cosine_similarity(&v1, &v2) - 1.0).abs() < 1e-5);
        assert!((cosine_similarity(&v1, &v3) - 0.0).abs() < 1e-5);
    }
}
