use crate::interner::{SymbolId, SymbolInterner};
use crate::scanner::metrics::{CandleStats, Metrics};
use dashmap::DashMap;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ============================================================================
// CANDLE
// ============================================================================

#[derive(Clone, Copy, Debug, Default)]
pub struct Candle {
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl Candle {
    #[inline(always)]
    fn new(ts: i64, price: f64, cost: f64) -> Self {
        Self {
            ts,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: cost,
        }
    }

    #[inline(always)]
    fn update(&mut self, price: f64, cost: f64) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.volume += cost;
    }
}

// ============================================================================
// ALERT
// ============================================================================

#[derive(Debug, Clone)]
pub struct Alert {
    pub symbol: String,
    pub ts: i64,
    pub message: String,
    pub alert_type: String,
}

// ============================================================================
// SCANNER CONFIG
// ============================================================================

#[derive(Debug, Clone)]
pub struct ScannerConfig {
    pub return_limit: f64,
    pub volume_limit: f64,
    pub timeframe_s: i64,
    pub currency_type: String,
    pub delimiter: String,
    pub blacklist: Arc<HashSet<String>>,
}

impl ScannerConfig {
    #[inline]
    pub fn is_blacklisted(&self, symbol: &str) -> bool {
        self.blacklist.contains(symbol)
    }
}

// ============================================================================
// SCANNER STATS
// ============================================================================

#[derive(Debug, Clone)]
pub struct ScannerStats {
    pub symbols_count: usize,
    pub active_in_batch: usize,
    pub trades_processed: u64,
    pub alerts_generated: u64,
    pub candle_stats: CandleStats,
}

// ============================================================================
// SCANNER CORE
// ============================================================================

/// High-performance trade processing engine.
///
/// Uses SymbolId (u32) instead of String for all hash-map keys —
/// faster hashing, less memory, better cache locality.
/// Thread-local metrics eliminate atomic contention on hot path.
/// Rayon parallel processing across symbols.
pub struct ScannerCore {
    /// Interner shared across the application
    interner: Arc<SymbolInterner>,
    /// Candles indexed by SymbolId (u32 hash — very fast)
    candles: DashMap<SymbolId, Arc<RwLock<Option<Candle>>>>,
    last_alert_ts: DashMap<SymbolId, i64>,
    last_active: RwLock<HashSet<SymbolId>>,
    /// Thread-local metrics — no atomic contention in hot path
    metrics: Metrics,
}

impl ScannerCore {
    pub fn new(interner: Arc<SymbolInterner>) -> Self {
        Self {
            interner,
            candles: DashMap::new(),
            last_alert_ts: DashMap::new(),
            last_active: RwLock::new(HashSet::new()),
            metrics: Metrics::new(),
        }
    }

    /// Process a batch of trades. Returns alerts that match criteria.
    /// Input: Vec of (symbol_string, timestamp_ms, price, cost).
    /// Interns symbols to SymbolId internally — zero-cost after first occurrence.
    #[inline]
    pub fn process_trades(
        &self,
        trades: Vec<(String, i64, f64, f64)>,
        config: &ScannerConfig,
    ) -> Vec<Alert> {
        let tf_ms = config.timeframe_s * 1000;
        let ret_limit = config.return_limit;
        let vol_limit = config.volume_limit;
        let currency_type = &config.currency_type;
        let delimiter = &config.delimiter;
        let blacklist = Arc::clone(&config.blacklist);
        let interner = Arc::clone(&self.interner);
        let candles = &self.candles;
        let last_alert_ts = &self.last_alert_ts;
        let last_active = &self.last_active;
        let metrics = &self.metrics;

        let trades_count = trades.len() as u64;
        metrics.record_trades(trades_count);

        // Intern all symbols and group trades by SymbolId
        let mut by_symbol: HashMap<SymbolId, (/*formatted*/ String, Vec<(i64, f64, f64)>)> =
            HashMap::new();
        for (sym, ts, px, cost) in trades {
            if blacklist.contains(&sym) {
                continue;
            }
            let sid = interner.intern(&sym);
            let fmt_sym = format_symbol(&sym, currency_type, delimiter);
            by_symbol
                .entry(sid)
                .or_insert_with(|| (fmt_sym, Vec::new()))
                .1
                .push((ts, px, cost));
        }

        if by_symbol.is_empty() {
            return Vec::new();
        }

        // Track active symbols (we do NOT clear it here anymore, so it accumulates between stats flushes)
        {
            let mut active = last_active.write();
            for sid in by_symbol.keys() {
                active.insert(*sid);
            }
        }

        // Process symbols in parallel with rayon
        let alerts: Vec<Alert> = by_symbol
            .into_par_iter()
            .filter_map(|(sid, (fmt_sym, trades))| {
                let candle_arc = candles
                    .entry(sid)
                    .or_insert_with(|| Arc::new(RwLock::new(None)))
                    .clone();
                let mut candle_guard = candle_arc.write();
                let mut symbol_alerts = Vec::new();

                for (ts, price, cost) in trades {
                    if ts <= 0 {
                        continue;
                    }
                    let candle_ts = (ts / tf_ms) * tf_ms;

                    match &mut *candle_guard {
                        None => {
                            *candle_guard = Some(Candle::new(candle_ts, price, cost));
                        }
                        Some(candle) if candle.ts == candle_ts => {
                            candle.update(price, cost);
                        }
                        Some(candle) if candle_ts > candle.ts => {
                            // Candle closed — check for alert conditions
                            let closed = *candle;
                            let pct = if closed.close >= closed.open {
                                if closed.low > 0.0 {
                                    ((closed.high - closed.low) / closed.low) * 100.0
                                } else {
                                    0.0
                                }
                            } else if closed.high > 0.0 {
                                ((closed.low - closed.high) / closed.high) * 100.0
                            } else {
                                0.0
                            };

                            // Record candle closure diagnostics
                            metrics.record_candle_closed(closed.volume, pct);
                            let vol_met = closed.volume >= vol_limit;
                            let pct_met = pct.abs() >= ret_limit;
                            if vol_met { metrics.record_candle_vol_ok(); }
                            if pct_met { metrics.record_candle_pct_ok(); }

                            if vol_met && pct_met {
                                let should_alert = match last_alert_ts.get(&sid) {
                                    Some(prev_ts) if *prev_ts == closed.ts => {
                                        metrics.record_candle_suppressed();
                                        false
                                    }
                                    _ => true,
                                };
                                if should_alert {
                                    last_alert_ts.insert(sid, closed.ts);
                                    symbol_alerts.push(Alert {
                                        symbol: fmt_sym.clone(),
                                        ts: closed.ts,
                                        message: format_alert(&fmt_sym, pct, closed.volume),
                                        alert_type: "volatility".to_string(),
                                    });
                                }
                            }
                            *candle_guard = Some(Candle::new(candle_ts, price, cost));
                        }
                        _ => {}
                    }
                }

                if symbol_alerts.is_empty() {
                    None
                } else {
                    Some(symbol_alerts)
                }
            })
            .flatten()
            .collect();

        if !alerts.is_empty() {
            metrics.record_alerts(alerts.len() as u64);
        }

        alerts
    }

    /// Number of symbols with active candles
    pub fn len(&self) -> usize {
        self.candles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.candles.is_empty()
    }

    /// Clear all state
    pub fn clear(&self) {
        self.candles.clear();
        self.last_active.write().clear();
        self.last_alert_ts.clear();
        self.metrics.reset();
    }

    /// Remove candles for symbols not in the active set
    pub fn cleanup_symbols_by_ids(&self, active_ids: &HashSet<SymbolId>) {
        self.candles.retain(|k, _| active_ids.contains(k));
        self.last_active.write().retain(|s| active_ids.contains(s));
        self.last_alert_ts.retain(|k, _| active_ids.contains(k));
    }

    /// Remove candles for symbols not in the active set (string-based convenience).
    /// Interns each active string to obtain its SymbolId for lookup.
    pub fn cleanup_symbols(&self, active: &HashSet<String>) {
        let active_ids: HashSet<SymbolId> = active
            .iter()
            .map(|s| self.interner.intern(s))
            .collect();
        self.cleanup_symbols_by_ids(&active_ids);
    }

    /// Get current stats (flushes thread-local counters to global).
    pub fn stats(&self) -> ScannerStats {
        let cs = self.metrics.flush();
        
        // Read and clear the active symbols counter for the next interval
        let active_count = {
            let mut active = self.last_active.write();
            let count = active.len();
            active.clear();
            count
        };

        ScannerStats {
            symbols_count: self.candles.len(),
            active_in_batch: active_count,
            trades_processed: cs.trades_processed,
            alerts_generated: cs.alerts_generated,
            candle_stats: cs,
        }
    }

    /// Get stats WITHOUT flushing (approximate, includes unflushed thread-local).
    pub fn stats_no_flush(&self) -> ScannerStats {
        let cs = self.metrics.global();
        ScannerStats {
            symbols_count: self.candles.len(),
            active_in_batch: self.last_active.read().len(),
            trades_processed: cs.trades_processed,
            alerts_generated: cs.alerts_generated,
            candle_stats: cs,
        }
    }

    /// Reset all metrics counters
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }

    /// Get reference to the interner
    pub fn interner(&self) -> &Arc<SymbolInterner> {
        &self.interner
    }
}

// ============================================================================
// HELPERS
// ============================================================================

#[inline(always)]
fn format_alert(symbol: &str, price_return: f64, volume: f64) -> String {
    let tens = (price_return.abs() / 10.0).floor() as usize + 1;
    let emoji = if price_return > 0.0 {
        "\u{1F7E2}".repeat(tens)
    } else {
        "\u{1F534}".repeat(tens)
    };
    format!(
        "`{}` {} {:.3}%\nVol: {:.2}$",
        symbol, emoji, price_return, volume
    )
}

#[inline(always)]
fn format_symbol(symbol: &str, currency_type: &str, delimiter: &str) -> String {
    let s = symbol.replace('/', delimiter);
    if currency_type == "spot" {
        s
    } else {
        s.split(':').next().unwrap_or(&s).to_string()
    }
}