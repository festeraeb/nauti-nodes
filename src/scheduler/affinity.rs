use crate::types::NodeId;
use dashmap::DashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct AffinityRouter {
    prefix_to_node: DashMap<u64, NodeId>,
}

impl Default for AffinityRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl AffinityRouter {
    pub fn new() -> Self {
        Self {
            prefix_to_node: DashMap::new(),
        }
    }

    pub fn hash_prefix(prompt_prefix: &str) -> u64 {
        let slice = &prompt_prefix[..prompt_prefix.len().min(512)];
        let mut h = DefaultHasher::new();
        slice.hash(&mut h);
        h.finish()
    }

    pub fn lookup(&self, prompt_prefix: &str) -> Option<NodeId> {
        self.prefix_to_node
            .get(&Self::hash_prefix(prompt_prefix))
            .map(|e| e.clone())
    }

    pub fn remember(&self, prompt_prefix: &str, node_id: &str) {
        self.prefix_to_node
            .insert(Self::hash_prefix(prompt_prefix), node_id.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_prefix_same_hash() {
        assert_eq!(
            AffinityRouter::hash_prefix("hello world"),
            AffinityRouter::hash_prefix("hello world")
        );
    }

    #[test]
    fn remember_and_lookup() {
        let r = AffinityRouter::new();
        r.remember("prompt chunk", "node-a");
        assert_eq!(r.lookup("prompt chunk").as_deref(), Some("node-a"));
    }
}
