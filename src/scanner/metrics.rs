use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use thread_local::ThreadLocal;

/// Per-thread local metrics — zero contention on hot path
#[derive(Default, Clone, Copy)]
struct LocalMetrics {
    trades_processed: u64,
    alerts_generated: u64,
    candles_closed: u64,
    candles_vol_ok: u64,
    candles_pct_ok: u64,
    candles_suppressed: u64,
    max_volume: f64,
    max_pct: f64,
}

/// Interior-mutable wrapper for `LocalMetrics` inside a `ThreadLocal`.
///
/// `Cell<LocalMetrics>` doesn't implement `Sync`, but `ThreadLocal::iter()`
/// requires `T: Sync`. We use `UnsafeCell` + an `unsafe impl Sync` instead.
///
/// SAFETY: `ThreadLocal` guarantees that each thread only ever accesses
/// its own entry, so there is no data race across threads.
struct SyncCell {
    value: UnsafeCell<LocalMetrics>,
}

unsafe impl Sync for SyncCell {}

impl SyncCell {
    /// Read the current value. Caller must be on the owning thread.
    #[inline(always)]
    fn get(&self) -> LocalMetrics {
        // SAFETY: only accessed from the owning thread (guaranteed by ThreadLocal)
        unsafe { *self.value.get() }
    }

    /// Write a new value. Caller must be on the owning thread.
    #[inline(always)]
    fn set(&self, val: LocalMetrics) {
        // SAFETY: only accessed from the owning thread (guaranteed by ThreadLocal)
        unsafe {
            *self.value.get() = val;
        }
    }
}

impl Default for SyncCell {
    fn default() -> Self {
        Self {
            value: UnsafeCell::new(LocalMetrics::default()),
        }
    }
}

/// Aggregated stats including candle diagnostics.
#[derive(Debug, Clone, Default)]
pub struct CandleStats {
    pub trades_processed: u64,
    pub alerts_generated: u64,
    pub candles_closed: u64,
    pub candles_vol_ok: u64,
    pub candles_pct_ok: u64,
    pub candles_suppressed: u64,
    pub max_volume: f64,
    pub max_pct: f64,
}

/// Thread-safe metrics with per-thread accumulation.
///
/// rayon workers write to thread-local counters (no atomic contention).
/// `flush()` aggregates to global counters (called from Monitor task).
pub struct Metrics {
    local: ThreadLocal<SyncCell>,
    global_trades: AtomicU64,
    global_alerts: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            local: ThreadLocal::new(),
            global_trades: AtomicU64::new(0),
            global_alerts: AtomicU64::new(0),
        }
    }

    /// Record a processed trade. Called from rayon worker thread — zero contention.
    #[inline(always)]
    pub fn record_trade(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.trades_processed += 1;
        cell.set(m);
    }

    /// Record a generated alert. Called from rayon worker thread — zero contention.
    #[inline(always)]
    pub fn record_alert(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.alerts_generated += 1;
        cell.set(m);
    }

    /// Record a closed candle with its stats.
    #[inline(always)]
    pub fn record_candle_closed(&self, volume: f64, pct: f64) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.candles_closed += 1;
        m.max_volume = m.max_volume.max(volume);
        m.max_pct = m.max_pct.max(pct.abs());
        cell.set(m);
    }

    /// Record a candle that met volume threshold.
    #[inline(always)]
    pub fn record_candle_vol_ok(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.candles_vol_ok += 1;
        cell.set(m);
    }

    /// Record a candle that met pct threshold.
    #[inline(always)]
    pub fn record_candle_pct_ok(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.candles_pct_ok += 1;
        cell.set(m);
    }

    /// Record a candle suppressed by last_alert_ts duplicate.
    #[inline(always)]
    pub fn record_candle_suppressed(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.candles_suppressed += 1;
        cell.set(m);
    }

    /// Record multiple trades at once (more efficient for batching).
    #[inline(always)]
    pub fn record_trades(&self, count: u64) {
        if count == 0 {
            return;
        }
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.trades_processed += count;
        cell.set(m);
    }

    /// Record multiple alerts at once.
    #[inline(always)]
    pub fn record_alerts(&self, count: u64) {
        if count == 0 {
            return;
        }
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.alerts_generated += count;
        cell.set(m);
    }

    /// Flush all thread-local counters to global totals.
    /// Called periodically (e.g., every 60s from Monitor task).
    /// Returns CandleStats after flush.
    pub fn flush(&self) -> CandleStats {
        let mut total_trades = 0u64;
        let mut total_alerts = 0u64;
        let mut total_closed = 0u64;
        let mut total_vol_ok = 0u64;
        let mut total_pct_ok = 0u64;
        let mut total_suppressed = 0u64;
        let mut global_max_vol = 0.0f64;
        let mut global_max_pct = 0.0f64;

        for cell in self.local.iter() {
            let m = cell.get();
            total_trades += m.trades_processed;
            total_alerts += m.alerts_generated;
            total_closed += m.candles_closed;
            total_vol_ok += m.candles_vol_ok;
            total_pct_ok += m.candles_pct_ok;
            total_suppressed += m.candles_suppressed;
            global_max_vol = global_max_vol.max(m.max_volume);
            global_max_pct = global_max_pct.max(m.max_pct);
            // Reset local counters
            cell.set(LocalMetrics::default());
        }

        // Add to global
        let prev_trades = self.global_trades.fetch_add(total_trades, Ordering::Relaxed);
        let prev_alerts = self.global_alerts.fetch_add(total_alerts, Ordering::Relaxed);

        CandleStats {
            trades_processed: prev_trades + total_trades,
            alerts_generated: prev_alerts + total_alerts,
            candles_closed: total_closed,
            candles_vol_ok: total_vol_ok,
            candles_pct_ok: total_pct_ok,
            candles_suppressed: total_suppressed,
            max_volume: global_max_vol,
            max_pct: global_max_pct,
        }
    }

    /// Get current global totals (approximate — may not include unflushed thread-local).
    pub fn global(&self) -> CandleStats {
        let mut trades = self.global_trades.load(Ordering::Relaxed);
        let mut alerts = self.global_alerts.load(Ordering::Relaxed);
        let mut closed = 0u64;
        let mut vol_ok = 0u64;
        let mut pct_ok = 0u64;
        let mut suppressed = 0u64;
        let mut max_vol = 0.0f64;
        let mut max_pct = 0.0f64;

        for cell in self.local.iter() {
            let m = cell.get();
            trades += m.trades_processed;
            alerts += m.alerts_generated;
            closed += m.candles_closed;
            vol_ok += m.candles_vol_ok;
            pct_ok += m.candles_pct_ok;
            suppressed += m.candles_suppressed;
            max_vol = max_vol.max(m.max_volume);
            max_pct = max_pct.max(m.max_pct);
        }

        CandleStats {
            trades_processed: trades,
            alerts_generated: alerts,
            candles_closed: closed,
            candles_vol_ok: vol_ok,
            candles_pct_ok: pct_ok,
            candles_suppressed: suppressed,
            max_volume: max_vol,
            max_pct: max_pct,
        }
    }

    /// Reset all counters (global + thread-local).
    pub fn reset(&self) {
        for cell in self.local.iter() {
            cell.set(LocalMetrics::default());
        }
        self.global_trades.store(0, Ordering::Relaxed);
        self.global_alerts.store(0, Ordering::Relaxed);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
