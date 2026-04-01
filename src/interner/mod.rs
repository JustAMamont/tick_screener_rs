use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Clone, Copy, Hash, Eq, PartialEq, Debug)]
pub struct SymbolId(pub u32);

pub struct SymbolInterner {
    map: DashMap<String, u32>,
    strings: RwLock<Vec<String>>,
    next_id: AtomicU32,
}

impl SymbolInterner {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            strings: RwLock::new(Vec::with_capacity(4096)),
            next_id: AtomicU32::new(0),
        }
    }

    /// Intern a string, returning its SymbolId. Thread-safe.
    #[inline]
    pub fn intern(&self, s: &str) -> SymbolId {
        if let Some(id) = self.map.get(s) {
            return SymbolId(*id);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.map.insert(s.to_string(), id);
        self.strings.write().push(s.to_string());
        SymbolId(id)
    }

    /// Get the string for a SymbolId.
    #[inline]
    pub fn resolve(&self, id: SymbolId) -> String {
        self.strings
            .read()
            .get(id.0 as usize)
            .cloned()
            .unwrap_or_default()
    }

    /// Current number of interned symbols.
    pub fn len(&self) -> usize {
        self.next_id.load(Ordering::Relaxed) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for SymbolInterner {
    fn default() -> Self {
        Self::new()
    }
}
