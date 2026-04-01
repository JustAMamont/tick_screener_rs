use crate::config::model::{ConfigSnapshot, FeedKey, ScannerRuntimeConfig};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use parking_lot::RwLock;

/// Holds the current config state and computes diffs between old and new snapshots.
pub struct ConfigRegistry {
    current: RwLock<Arc<ConfigSnapshot>>,
}

impl ConfigRegistry {
    pub fn new(initial: Arc<ConfigSnapshot>) -> Self {
        Self {
            current: RwLock::new(initial),
        }
    }

    /// Apply a new config snapshot, returning a diff of what changed.
    pub fn update(&self, new_snapshot: Arc<ConfigSnapshot>) -> ConfigDiff {
        let old = self.current.read().clone();

        let old_ids: HashSet<&str> = old.scanners.iter().map(|s| s.scanner_id.as_str()).collect();
        let new_ids: HashSet<&str> = new_snapshot.scanners.iter().map(|s| s.scanner_id.as_str()).collect();

        let added: Vec<String> = new_ids.difference(&old_ids).map(|s| s.to_string()).collect();
        let removed: Vec<String> = old_ids.difference(&new_ids).map(|s| s.to_string()).collect();

        // Find modified scanners (same id, different config)
        let mut modified = Vec::new();
        let new_map: HashMap<&str, &ScannerRuntimeConfig> = new_snapshot
            .scanners
            .iter()
            .map(|s| (s.scanner_id.as_str(), s))
            .collect();

        let old_map: HashMap<&str, &ScannerRuntimeConfig> = old
            .scanners
            .iter()
            .map(|s| (s.scanner_id.as_str(), s))
            .collect();

        for id in new_ids.intersection(&old_ids) {
            let old_cfg = old_map[*id];
            let new_cfg = new_map[*id];
            if old_cfg.quote != new_cfg.quote
                || old_cfg.blacklist != new_cfg.blacklist
                || old_cfg.alert_settings != new_cfg.alert_settings
            {
                modified.push(id.to_string());
            }
        }

        // Compute feed changes
        let old_feeds: HashSet<FeedKey> = old.scanners
            .iter()
            .map(|s| FeedKey::new(s.exchange, s.market_type))
            .collect();
        let new_feeds: HashSet<FeedKey> = new_snapshot.scanners
            .iter()
            .map(|s| FeedKey::new(s.exchange, s.market_type))
            .collect();

        let feeds_added: Vec<FeedKey> = new_feeds.difference(&old_feeds).cloned().collect();
        let feeds_removed: Vec<FeedKey> = old_feeds.difference(&new_feeds).cloned().collect();

        *self.current.write() = new_snapshot;

        ConfigDiff {
            added,
            removed,
            modified,
            feeds_added,
            feeds_removed,
        }
    }

    /// Get a clone of the current config snapshot.
    pub fn snapshot(&self) -> Arc<ConfigSnapshot> {
        self.current.read().clone()
    }

    /// Get runtime config for a specific scanner.
    pub fn get_scanner_config(&self, scanner_id: &str) -> Option<ScannerRuntimeConfig> {
        self.current
            .read()
            .scanners
            .iter()
            .find(|s| s.scanner_id == scanner_id)
            .cloned()
    }
}

/// Result of diffing two config snapshots.
#[derive(Debug, Clone)]
pub struct ConfigDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub modified: Vec<String>,
    pub feeds_added: Vec<FeedKey>,
    pub feeds_removed: Vec<FeedKey>,
}

impl ConfigDiff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.modified.is_empty()
            && self.feeds_added.is_empty()
            && self.feeds_removed.is_empty()
    }
}
