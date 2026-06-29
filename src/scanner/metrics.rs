//! Метрики сканера: thread-local накопление + атомарная агрегация.
//!
//! # Зачем thread-local?
//!
//! В горячем пути `process_trades` работает до десятков rayon-воркеров
//! одновременно. Если бы каждый инкремент счётчика шёл через `AtomicU64`
//! (`fetch_add`), происходила бы cache-line bouncing между ядрами -
//! дорогостоящая операция (десятки наносекунд на конфликт).
//!
//! Вместо этого каждый воркер пишет в свой собственный `SyncCell`
//! (через `ThreadLocal`), что вообще не требует межъядерной синхронизации.
//! Глобальные счётчики обновляются только при `flush()` - это разовая
//! операция за 60 секунд, накладные расходы пренебрежимо малы.
//!
//! # Безопасность `SyncCell`
//!
//! `UnsafeCell<LocalMetrics>` сам по себе не `Sync`. Но мы используем
//! его внутри `ThreadLocal`, которое гарантирует, что каждый поток
//! обращается только к своей ячейке. Поэтому `unsafe impl Sync` здесь
//! корректен: межпоточных гонок не возникает по построению.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use thread_local::ThreadLocal;

/// Локальные метрики одного потока. Никаких атомиков - копия на поток.
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

/// Interior-mutable обёртка над [`LocalMetrics`] для использования
/// внутри `ThreadLocal`.
///
/// `Cell<LocalMetrics>` не реализует `Sync`, а `ThreadLocal::iter()`
/// требует `T: Sync`. Поэтому берём `UnsafeCell` + явный `unsafe impl Sync`.
///
/// # Безопасность
///
/// `ThreadLocal` гарантирует, что каждый поток обращается только к
/// своей ячейке (по construction). Чтение и запись всегда происходят
/// с того же потока, который создал ячейку. Поэтому межпоточных гонок
/// не возникает, и `Sync` здесь безопасен.
struct SyncCell {
    value: UnsafeCell<LocalMetrics>,
}

// SAFETY: см. комментарий в документации типа - `ThreadLocal` обеспечивает
// потоковую изоляцию доступа.
unsafe impl Sync for SyncCell {}

impl SyncCell {
    /// Читает текущее значение. Вызывающий должен быть на потоке-владельце.
    #[inline(always)]
    fn get(&self) -> LocalMetrics {
        // SAFETY: только поток-владелец обращается к этой ячейке
        // (гарантирует `ThreadLocal`).
        unsafe { *self.value.get() }
    }

    /// Записывает новое значение. Вызывающий должен быть на потоке-владельце.
    #[inline(always)]
    fn set(&self, val: LocalMetrics) {
        // SAFETY: см. `get`.
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

/// Агрегированные метрики по закрытым свечам за интервал.
#[derive(Debug, Clone, Default)]
pub struct CandleStats {
    /// Всего трейдов обработано (кумулятивно с запуска).
    pub trades_processed: u64,
    /// Всего алертов сгенерировано (кумулятивно с запуска).
    pub alerts_generated: u64,
    /// Свечей закрыто за последний интервал.
    pub candles_closed: u64,
    /// Из них прошли порог по объёму.
    pub candles_vol_ok: u64,
    /// Из них прошли порог по проценту.
    pub candles_pct_ok: u64,
    /// Подавлены как дубликаты (тот же ts).
    pub candles_suppressed: u64,
    /// Максимальный объём среди закрытых свечей.
    pub max_volume: f64,
    /// Максимальный % изменения среди закрытых свечей.
    pub max_pct: f64,
}

/// Потокобезопасные метрики с thread-local накоплением.
///
/// Rayon-воркеры пишут в свои локальные счётчики (без атомиков).
/// [`Metrics::flush`] агрегирует в глобальные атомики (вызывается из
/// Monitor-таски раз в 60 секунд).
///
/// # Порядок памяти
///
/// Глобальные счётчики используют `Ordering::Relaxed` - нам не нужна
/// строгая синхронизация с другими операциями, только консистентность
/// самих значений. `fetch_add` на `AtomicU64` атомарен сам по себе.
pub struct Metrics {
    /// Thread-local накопители.
    local: ThreadLocal<SyncCell>,
    /// Глобальный кумулятивный счётчик трейдов (после flush).
    global_trades: AtomicU64,
    /// Глобальный кумулятивный счётчик алертов (после flush).
    global_alerts: AtomicU64,
}

impl Metrics {
    /// Создаёт пустой экземпляр метрик.
    pub fn new() -> Self {
        Self {
            local: ThreadLocal::new(),
            global_trades: AtomicU64::new(0),
            global_alerts: AtomicU64::new(0),
        }
    }

    /// Записать один обработанный трейд. Горячий путь - без атомиков.
    #[inline(always)]
    pub fn record_trade(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.trades_processed += 1;
        cell.set(m);
    }

    /// Записать один сгенерированный алерт. Горячий путь - без атомиков.
    #[inline(always)]
    pub fn record_alert(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.alerts_generated += 1;
        cell.set(m);
    }

    /// Записать закрытую свечу с её характеристиками.
    #[inline(always)]
    pub fn record_candle_closed(&self, volume: f64, pct: f64) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.candles_closed += 1;
        m.max_volume = m.max_volume.max(volume);
        m.max_pct = m.max_pct.max(pct.abs());
        cell.set(m);
    }

    /// Записать свечу, прошедшую порог по объёму.
    #[inline(always)]
    pub fn record_candle_vol_ok(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.candles_vol_ok += 1;
        cell.set(m);
    }

    /// Записать свечу, прошедшую порог по проценту.
    #[inline(always)]
    pub fn record_candle_pct_ok(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.candles_pct_ok += 1;
        cell.set(m);
    }

    /// Записать подавленный дубль алерта.
    #[inline(always)]
    pub fn record_candle_suppressed(&self) {
        let cell = self.local.get_or_default();
        let mut m = cell.get();
        m.candles_suppressed += 1;
        cell.set(m);
    }

    /// Записать сразу `count` трейдов (эффективнее для батчей).
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

    /// Записать сразу `count` алертов.
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

    /// Сбросить все thread-local счётчики в глобальные и вернуть снимок.
    ///
    /// Вызывается периодически (например, раз в 60 секунд из Monitor-таски).
    /// После вызова значения в [`CandleStats::candles_closed`] и др.
    /// отражают статистику именно за последний интервал, а
    /// `trades_processed` / `alerts_generated` - кумулятивно с запуска.
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
            // Сбрасываем локальный счётчик - следующий интервал начнётся с нуля.
            cell.set(LocalMetrics::default());
        }

        // Relaxed: нас интересует только атомарность самого сложения.
        let prev_trades = self
            .global_trades
            .fetch_add(total_trades, Ordering::Relaxed);
        let prev_alerts = self
            .global_alerts
            .fetch_add(total_alerts, Ordering::Relaxed);

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

    /// Текущие глобальные итоги без сброса thread-local.
    ///
    /// Приблизительные значения - могут не включать ещё не сброшенные
    /// локальные счётчики. Подходит для отладки и live-мониторинга.
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
            max_pct,
        }
    }

    /// Полный сброс всех счётчиков (глобальные + thread-local).
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

#[cfg(test)]
mod tests {
    use super::*;
    use rayon::prelude::*;

    #[test]
    fn new_metrics_starts_empty() {
        let m = Metrics::new();
        let cs = m.global();
        assert_eq!(cs.trades_processed, 0);
        assert_eq!(cs.alerts_generated, 0);
        assert_eq!(cs.candles_closed, 0);
    }

    #[test]
    fn record_trade_increments_counter() {
        let m = Metrics::new();
        m.record_trade();
        m.record_trade();
        m.record_trade();
        let cs = m.global();
        assert_eq!(cs.trades_processed, 3);
    }

    #[test]
    fn record_trades_batch_increments_counter() {
        let m = Metrics::new();
        m.record_trades(100);
        m.record_trades(0); // no-op
        let cs = m.global();
        assert_eq!(cs.trades_processed, 100);
    }

    #[test]
    fn record_alert_increments_counter() {
        let m = Metrics::new();
        m.record_alert();
        m.record_alert();
        let cs = m.global();
        assert_eq!(cs.alerts_generated, 2);
    }

    #[test]
    fn record_alerts_batch_increments_counter() {
        let m = Metrics::new();
        m.record_alerts(5);
        let cs = m.global();
        assert_eq!(cs.alerts_generated, 5);
    }

    #[test]
    fn record_candle_closed_updates_max() {
        let m = Metrics::new();
        m.record_candle_closed(1000.0, 1.5);
        m.record_candle_closed(5000.0, 0.5);
        m.record_candle_closed(3000.0, 3.0);
        let cs = m.global();
        assert_eq!(cs.candles_closed, 3);
        assert_eq!(cs.max_volume, 5000.0);
        assert_eq!(cs.max_pct, 3.0);
    }

    #[test]
    fn record_candle_vol_ok_increments() {
        let m = Metrics::new();
        m.record_candle_vol_ok();
        m.record_candle_vol_ok();
        let cs = m.global();
        assert_eq!(cs.candles_vol_ok, 2);
    }

    #[test]
    fn record_candle_pct_ok_increments() {
        let m = Metrics::new();
        m.record_candle_pct_ok();
        let cs = m.global();
        assert_eq!(cs.candles_pct_ok, 1);
    }

    #[test]
    fn record_candle_suppressed_increments() {
        let m = Metrics::new();
        m.record_candle_suppressed();
        m.record_candle_suppressed();
        m.record_candle_suppressed();
        let cs = m.global();
        assert_eq!(cs.candles_suppressed, 3);
    }

    #[test]
    fn flush_returns_cumulative_and_resets_local() {
        let m = Metrics::new();
        m.record_trades(100);
        m.record_alerts(5);
        m.record_candle_closed(1000.0, 2.0);

        let cs1 = m.flush();
        assert_eq!(cs1.trades_processed, 100);
        assert_eq!(cs1.alerts_generated, 5);
        assert_eq!(cs1.candles_closed, 1); // сбрасывается после flush

        // После flush локальные обнулены, но глобальные накопительные
        m.record_trades(50);
        let cs2 = m.flush();
        assert_eq!(cs2.trades_processed, 150); // 100 + 50
        assert_eq!(cs2.alerts_generated, 5);
        assert_eq!(cs2.candles_closed, 0); // в этом интервале свечей не закрывалось
    }

    #[test]
    fn reset_clears_all_counters() {
        let m = Metrics::new();
        m.record_trades(100);
        m.record_alerts(5);
        m.record_candle_closed(1000.0, 2.0);
        m.flush();
        m.record_trades(50);

        m.reset();
        let cs = m.global();
        assert_eq!(cs.trades_processed, 0);
        assert_eq!(cs.alerts_generated, 0);
        assert_eq!(cs.candles_closed, 0);
    }

    #[test]
    fn parallel_writes_aggregate_correctly() {
        // Проверяем что thread-local накопители корректно агрегируются
        // при параллельной записи из множества rayon-воркеров.
        let m = std::sync::Arc::new(Metrics::new());
        (0..1000).into_par_iter().for_each(|_| {
            m.record_trade();
            m.record_alert();
            m.record_candle_closed(100.0, 1.0);
        });
        let cs = m.flush();
        assert_eq!(cs.trades_processed, 1000);
        assert_eq!(cs.alerts_generated, 1000);
        assert_eq!(cs.candles_closed, 1000);
        assert_eq!(cs.max_volume, 100.0);
    }
}
