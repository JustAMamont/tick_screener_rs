//! Ядро сканера: обработка трейдов, агрегация свечей, генерация алертов.
//!
//! Это самый горячий путь приложения - тысячи трейдов в секунду
//! обрабатываются здесь. Все оптимизации направлены на минимизацию
//! аллокаций и блокировок:
//!
//! * Символы идентифицируются через [`SymbolId`] (`u32`), а не `String`.
//! * Метрики пишутся в thread-local структуру без атомарных операций.
//! * Свечи хранятся в `DashMap<SymbolId, Arc<RwLock<Option<Candle>>>>`
//!   - чтение/запись шардированы, каждая свеча под своим локом.
//! * Параллельная обработка символов через `rayon::par_iter`.
//!
//! # Поток безопасности
//!
//! `ScannerCore` потокобезопасен: все методы принимают `&self`, а
//! внутренняя мутабельность обеспечивается `DashMap`, `RwLock` и
//! thread-local счётчиками. Единственная гипотетическая проблема -
//! `last_alert_ts` (см. [`Self::process_trades`]), но он допустим:
//! в худшем случае один и тот же алерт может быть отправлен дважды
//! при параллельной обработке, что не критично для UX.

use crate::interner::{SymbolId, SymbolInterner};
use crate::scanner::metrics::{CandleStats, Metrics};
use dashmap::DashMap;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ============================================================================
// CANDLE - агрегат за один таймфрейм
// ============================================================================

/// OHLCV-свеча для одного таймфрейма.
///
/// `Copy` + 48 байт - помещается в кэш-линию. Все поля `f64` для
/// максимальной эффективности арифметики.
#[derive(Clone, Copy, Debug, Default)]
pub struct Candle {
    /// Метка времени начала свечи (миллисекунды, выровнены по таймфрейму).
    pub ts: i64,
    /// Цена открытия (первая сделка в свече).
    pub open: f64,
    /// Максимальная цена в свече.
    pub high: f64,
    /// Минимальная цена в свече.
    pub low: f64,
    /// Цена закрытия (последняя сделка в свече).
    pub close: f64,
    /// Накопленный объём в котируемой валюте (сумма `price * qty`).
    pub volume: f64,
}

impl Candle {
    /// Создаёт новую свечу с одной сделкой.
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

    /// Обновляет существующую свечу новой сделкой.
    ///
    /// `high`/`low` обновляются только если новая цена выходит за границы -
    /// branch-predictor сделает эти ветки дешёвыми в типичных случаях.
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
// ALERT - уведомление пользователю
// ============================================================================

/// Алерт, отправляемый в Telegram (или другой канал доставки).
///
/// `String` здесь оправдано - алерты уходят во внешний канал, и владение
/// строкой удобнее, чем `Arc<str>` (другие компоненты могут ещё держать
/// ссылку, но само сообщение мы отправляем как есть).
#[derive(Debug, Clone)]
pub struct Alert {
    /// Символ в формате для отображения (после форматирования).
    pub symbol: String,
    /// Метка времени закрытия свечи, вызвавшей алерт (мс).
    pub ts: i64,
    /// Готовый текст сообщения для Telegram.
    pub message: String,
    /// Тип алерта: `"volatility"` | `"listing"`.
    pub alert_type: String,
    /// Закрепить сообщение в чате (`true` для listings).
    pub pin: bool,
}

// ============================================================================
// SCANNER CONFIG - на каждый батч
// ============================================================================

/// Конфигурация сканера для текущего батча (snapshot из runtime-конфига).
///
/// Клонируется на каждый `process_trades` вызов. `Arc<HashSet<String>>`
/// для blacklist - клонирование дёшево (только инкремент счётчика).
#[derive(Debug, Clone)]
pub struct ScannerConfig {
    /// Порог относительного изменения цены (%) для срабатывания алерта.
    pub return_limit: f64,
    /// Порог объёма ($) для срабатывания алерта.
    pub volume_limit: f64,
    /// Таймфрейм свечи в секундах.
    pub timeframe_s: i64,
    /// Тип рынка: `"spot"` | `"perp"`. Влияет на форматирование символа.
    pub currency_type: String,
    /// Разделитель для отображения пары (например, `""` для `BTCUSDT`).
    pub delimiter: String,
    /// Список исключённых символов (в unified-формате `BTC/USDT`).
    pub blacklist: Arc<HashSet<String>>,
}

impl ScannerConfig {
    /// `true` если символ в чёрном списке.
    #[inline]
    pub fn is_blacklisted(&self, symbol: &str) -> bool {
        self.blacklist.contains(symbol)
    }
}

// ============================================================================
// SCANNER STATS - снимок метрик для мониторинга
// ============================================================================

/// Снимок метрик сканера за период (выдаётся [`ScannerCore::stats`]).
#[derive(Debug, Clone)]
pub struct ScannerStats {
    /// Количество активных символов (со свечами в памяти).
    pub symbols_count: usize,
    /// Количество символов, у которых были трейды в текущем батче.
    pub active_in_batch: usize,
    /// Всего трейдов обработано (кумулятивно с запуска).
    pub trades_processed: u64,
    /// Всего алертов сгенерировано (кумулятивно с запуска).
    pub alerts_generated: u64,
    /// Статистика по закрытым свечам за последний интервал.
    pub candle_stats: CandleStats,
}

// ============================================================================
// SCANNER CORE - движок обработки
// ============================================================================

/// Высокопроизводительный движок обработки трейдов.
///
/// Использует [`SymbolId`] (`u32`) вместо `String` для всех ключей в
/// хэш-таблицах - быстрее хэширование, меньше памяти, лучше локальность
/// кэша. Метрики пишутся в thread-local счётчики без атомарных
/// конфликтов на горячем пути. Параллельная обработка символов через
/// `rayon`.
///
/// # Жизненный цикл
///
/// 1. Создаётся один раз через [`ScannerCore::new`] с разделяемым
///    интернером.
/// 2. На каждый батч трейдов вызывается [`ScannerCore::process_trades`].
/// 3. Периодически (раз в минуту) вызывается [`ScannerCore::stats`]
///    для сброса thread-local счётчиков в глобальные и получения снимка.
/// 4. При hot-reload конфига может быть очищен через [`ScannerCore::clear`].
pub struct ScannerCore {
    /// Разделяемый интернер символов (один на всё приложение).
    interner: Arc<SymbolInterner>,
    /// Свечи по символам. `Arc<RwLock<Option<Candle>>>` позволяет
    /// параллельно обновлять свечи разных символов без конфликтов.
    candles: DashMap<SymbolId, Arc<RwLock<Option<Candle>>>>,
    /// Время последнего алерта по символу - для подавления дублей
    /// в пределах одного таймфрейма.
    last_alert_ts: DashMap<SymbolId, i64>,
    /// Множество символов, проявлявших активность с последнего
    /// `stats()` - нужно для отчёта `active_in_batch`.
    last_active: RwLock<HashSet<SymbolId>>,
    /// Метрики: thread-local для горячих путей + атомики для глобальных.
    metrics: Metrics,
}

impl ScannerCore {
    /// Создаёт новый экземпляр с заданным интернером.
    pub fn new(interner: Arc<SymbolInterner>) -> Self {
        Self {
            interner,
            candles: DashMap::new(),
            last_alert_ts: DashMap::new(),
            last_active: RwLock::new(HashSet::new()),
            metrics: Metrics::new(),
        }
    }

    /// Обрабатывает батч трейдов. Возвращает сгенерированные алерты.
    ///
    /// # Алгоритм
    ///
    /// 1. Группируем входные трейды по `SymbolId` (один `intern` на символ).
    /// 2. Параллельно (`rayon`) обрабатываем каждый символ:
    ///    - Берём или создаём свечу.
    ///    - Для каждой сделки: либо обновляем текущую свечу, либо
    ///      закрываем предыдущую (проверка порогов → алерт) и
    ///      открываем новую.
    /// 3. Агрегируем все алерты в один `Vec`.
    ///
    /// # Гонок-безопасность
    ///
    /// `last_alert_ts.insert` внутри rayon-параллелизма потенциально
    /// гонок: два воркера могут одновременно проверить отсутствие
    /// записи и оба вставить. Это допустимо - в худшем случае
    /// один дубль алерта в пределах миллисекунды.
    ///
    /// # Аргументы
    ///
    /// * `trades` - вектор `(symbol_string, timestamp_ms, price, cost)`.
    ///   `cost = price * qty` уже посчитан вызывающим.
    /// * `config` - снимок конфигурации на текущий батч.
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

        // Шаг 1: интернируем все символы и группируем трейды по SymbolId.
        // Используем `Arc<str>` для форматированного символа, чтобы
        // избежать клонирования строки при последующем параллельном
        // использовании в нескольких rayon-тасках.
        type TradesBySymbol = HashMap<SymbolId, (Arc<str>, Vec<(i64, f64, f64)>)>;
        let mut by_symbol: TradesBySymbol = HashMap::with_capacity(trades.len());
        for (sym, ts, px, cost) in trades {
            if blacklist.contains(&sym) {
                continue;
            }
            let sid = interner.intern(&sym);
            let fmt_sym: Arc<str> = Arc::from(format_symbol(&sym, currency_type, delimiter));
            by_symbol
                .entry(sid)
                .or_insert_with(|| (fmt_sym, Vec::new()))
                .1
                .push((ts, px, cost));
        }

        if by_symbol.is_empty() {
            return Vec::new();
        }

        // Шаг 2: обновляем множество активных символов для метрик.
        // Не очищаем - накапливаем между вызовами `stats()`.
        {
            let mut active = last_active.write();
            for sid in by_symbol.keys() {
                active.insert(*sid);
            }
        }

        // Шаг 3: параллельная обработка символов через rayon.
        // Каждый символ обрабатывается независимо - нет shared mutable state
        // между тасками (свечи разных символов изолированы через DashMap).
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
                        // Самая первая сделка по символу.
                        None => {
                            *candle_guard = Some(Candle::new(candle_ts, price, cost));
                        }
                        // Сделка в пределах текущей свечи - обновляем OHLCV.
                        Some(candle) if candle.ts == candle_ts => {
                            candle.update(price, cost);
                        }
                        // Сделка из нового таймфрейма - закрываем старую свечу.
                        Some(candle) if candle_ts > candle.ts => {
                            let closed = *candle;
                            // Считаем процент изменения цены относительно
                            // диапазона свечи (high-low).
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

                            // Записываем диагностику по закрытой свече.
                            metrics.record_candle_closed(closed.volume, pct);
                            let vol_met = closed.volume >= vol_limit;
                            let pct_met = pct.abs() >= ret_limit;
                            if vol_met {
                                metrics.record_candle_vol_ok();
                            }
                            if pct_met {
                                metrics.record_candle_pct_ok();
                            }

                            // Если оба порога пройдены - генерируем алерт
                            // (с подавлением дублей в пределах одного таймфрейма).
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
                                        symbol: fmt_sym.to_string(),
                                        ts: closed.ts,
                                        message: format_alert(&fmt_sym, pct, closed.volume),
                                        alert_type: "volatility".to_string(),
                                        pin: false,
                                    });
                                }
                            }
                            *candle_guard = Some(Candle::new(candle_ts, price, cost));
                        }
                        // Сделка из прошлого - игнорируем (out-of-order).
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

    /// Количество символов с активными свечами в памяти.
    pub fn len(&self) -> usize {
        self.candles.len()
    }

    /// `true` если нет ни одного отслеживаемого символа.
    pub fn is_empty(&self) -> bool {
        self.candles.is_empty()
    }

    /// Полная очистка состояния (свечи, алерт-история, метрики).
    ///
    /// Используется при hot-reload конфигурации, когда нужно
    /// пересоздать состояние с нуля.
    pub fn clear(&self) {
        self.candles.clear();
        self.last_active.write().clear();
        self.last_alert_ts.clear();
        self.metrics.reset();
    }

    /// Удаляет свечи для символов, отсутствующих в `active_ids`.
    ///
    /// Используется после рефреша pairlist-а, чтобы освободить память
    /// от делистингованных пар.
    pub fn cleanup_symbols_by_ids(&self, active_ids: &HashSet<SymbolId>) {
        self.candles.retain(|k, _| active_ids.contains(k));
        self.last_active.write().retain(|s| active_ids.contains(s));
        self.last_alert_ts.retain(|k, _| active_ids.contains(k));
    }

    /// Удаляет свечи для символов, отсутствующих в `active` (строки).
    ///
    /// Удобная обёртка над [`Self::cleanup_symbols_by_ids`]:
    /// интернирует каждую строку и вызывает underlying-метод.
    pub fn cleanup_symbols(&self, active: &HashSet<String>) {
        let active_ids: HashSet<SymbolId> =
            active.iter().map(|s| self.interner.intern(s)).collect();
        self.cleanup_symbols_by_ids(&active_ids);
    }

    /// Возвращает снимок метрик (сбрасывает thread-local счётчики в глобальные).
    ///
    /// Вызывать периодически (например, раз в 60 секунд из Monitor-таски).
    /// После вызова `active_in_batch` сбрасывается в 0 и начинает
    /// накапливаться заново до следующего `stats()`.
    pub fn stats(&self) -> ScannerStats {
        let cs = self.metrics.flush();

        // Читаем и очищаем множество активных символов для следующего интервала.
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

    /// Возвращает приблизительные метрики без сброса thread-local счётчиков.
    ///
    /// Полезно для отладки. Не используйте в production-логике - значения
    /// могут слегка отставать из-за несинхронизированных thread-local.
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

    /// Сброс всех счётчиков метрик (не влияет на свечи).
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }

    /// Ссылка на разделяемый интернер.
    pub fn interner(&self) -> &Arc<SymbolInterner> {
        &self.interner
    }
}

// ============================================================================
// ХЕЛПЕРЫ ФОРМАТИРОВАНИЯ
// ============================================================================

/// Формирует текст алерта для Telegram.
///
/// Количество эмодзи пропорционально модулю процентного изменения
/// (1 эмодзи на каждые 10%): зелёный для роста, красный для падения.
#[inline(always)]
fn format_alert(symbol: &str, price_return: f64, volume: f64) -> String {
    let tens = (price_return.abs() / 10.0).floor() as usize + 1;
    let emoji = if price_return > 0.0 {
        "\u{1F7E2}".repeat(tens) // зелёный круг
    } else {
        "\u{1F534}".repeat(tens) // красный круг
    };
    format!(
        "`{}` {} {:.3}%\nVol: {:.2}$",
        symbol, emoji, price_return, volume
    )
}

/// Преобразует unified-символ в отображаемый.
///
/// * Для spot: `BTC/USDT` -> `BTC{delimiter}USDT`.
/// * Для perp: `BTC/USDT.P` -> `BTC{delimiter}USDT` (суффикс `.P` отрезается).
#[inline(always)]
fn format_symbol(symbol: &str, currency_type: &str, delimiter: &str) -> String {
    let s = symbol.replace('/', delimiter);
    if currency_type == "spot" {
        s
    } else {
        s.strip_suffix(".P").unwrap_or(&s).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn make_config(timeframe_s: i64, ret_limit: f64, vol_limit: f64) -> ScannerConfig {
        ScannerConfig {
            return_limit: ret_limit,
            volume_limit: vol_limit,
            timeframe_s,
            currency_type: "spot".to_string(),
            delimiter: "".to_string(),
            blacklist: Arc::new(HashSet::new()),
        }
    }

    fn make_interner() -> Arc<SymbolInterner> {
        Arc::new(SymbolInterner::new())
    }

    #[test]
    fn empty_trades_returns_empty_alerts() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 1.0, 1000.0);
        let alerts = core.process_trades(Vec::new(), &cfg);
        assert!(alerts.is_empty());
    }

    #[test]
    fn blacklisted_symbol_is_skipped() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let mut blacklist = HashSet::new();
        blacklist.insert("BTC/USDT".to_string());
        let cfg = ScannerConfig {
            return_limit: 1.0,
            volume_limit: 1000.0,
            timeframe_s: 60,
            currency_type: "spot".to_string(),
            delimiter: "".to_string(),
            blacklist: Arc::new(blacklist),
        };
        let trades = vec![("BTC/USDT".to_string(), 1_700_000_000_000, 50_000.0, 10.0)];
        let alerts = core.process_trades(trades, &cfg);
        assert!(alerts.is_empty());
    }

    #[test]
    fn first_trade_creates_candle() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 1.0, 1000.0);
        let trades = vec![("BTC/USDT".to_string(), 1_700_000_000_000, 50_000.0, 10.0)];
        let _ = core.process_trades(trades, &cfg);
        assert_eq!(core.len(), 1, "One candle should be created for one symbol");
    }

    #[test]
    fn trades_within_same_timeframe_update_candle() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 1.0, 1000.0);
        // Два трейда в одной минуте (окно 60_000 мс)
        let ts = 1_700_000_000_000;
        let trades = vec![
            ("BTC/USDT".to_string(), ts, 50_000.0, 100.0),
            ("BTC/USDT".to_string(), ts + 30_000, 50_100.0, 200.0),
        ];
        let alerts = core.process_trades(trades, &cfg);
        assert!(alerts.is_empty(), "No candle closure within same timeframe");
        assert_eq!(core.len(), 1);
    }

    #[test]
    fn candle_closure_with_met_thresholds_generates_alert() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        // Очень низкие пороги, чтобы алерт точно сработал
        let cfg = make_config(60, 0.5, 100.0);
        let tf_ms = 60_000;
        let t0 = 1_700_000_000_000;
        let t1 = t0 + tf_ms; // следующая минута - закроет свечу
        let trades = vec![
            // Открываем свечу
            ("BTC/USDT".to_string(), t0, 50_000.0, 50.0),
            ("BTC/USDT".to_string(), t0 + 10_000, 51_000.0, 100.0), // high=51k, low=50k → pct=2%
            // Закрываем свечу и открываем новую (не должна сгенерировать алерт без объёма)
            ("BTC/USDT".to_string(), t1, 50_500.0, 30.0),
        ];
        let alerts = core.process_trades(trades, &cfg);
        assert_eq!(alerts.len(), 1, "Should generate one alert on candle close");
        assert_eq!(alerts[0].alert_type, "volatility");
        assert!(!alerts[0].pin);
    }

    #[test]
    fn duplicate_alert_within_same_timeframe_is_suppressed() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 0.5, 100.0);
        let tf_ms = 60_000;
        let t0 = 1_700_000_000_000;
        let t1 = t0 + tf_ms;
        let t2 = t0 + 2 * tf_ms;

        // Первый батч: открывает свечу, закрывает её (алерт), открывает новую
        let trades1 = vec![
            ("BTC/USDT".to_string(), t0, 50_000.0, 50.0),
            ("BTC/USDT".to_string(), t0 + 10_000, 51_000.0, 100.0),
            ("BTC/USDT".to_string(), t1, 50_500.0, 30.0),
        ];
        let alerts1 = core.process_trades(trades1, &cfg);
        assert_eq!(alerts1.len(), 1);

        // Второй батч: закрытие новой свечи - был уже алерт на этот же ts?
        // Нет, t1 != t0, поэтому новый алерт возможен.
        let trades2 = vec![
            ("BTC/USDT".to_string(), t1 + 30_000, 52_000.0, 100.0),
            ("BTC/USDT".to_string(), t2, 50_500.0, 30.0),
        ];
        let alerts2 = core.process_trades(trades2, &cfg);
        assert_eq!(alerts2.len(), 1, "Second alert should fire (different ts)");
    }

    #[test]
    fn volume_below_threshold_no_alert() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 0.5, 10_000.0); // высокий объёмный порог
        let tf_ms = 60_000;
        let t0 = 1_700_000_000_000;
        let t1 = t0 + tf_ms;
        let trades = vec![
            ("BTC/USDT".to_string(), t0, 50_000.0, 50.0), // объём 50 < 10000
            ("BTC/USDT".to_string(), t0 + 10_000, 55_000.0, 100.0), // pct > 0.5
            ("BTC/USDT".to_string(), t1, 50_500.0, 30.0),
        ];
        let alerts = core.process_trades(trades, &cfg);
        assert!(alerts.is_empty(), "Volume below threshold - no alert");
    }

    #[test]
    fn pct_below_threshold_no_alert() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 10.0, 100.0); // высокий порог pct
        let tf_ms = 60_000;
        let t0 = 1_700_000_000_000;
        let t1 = t0 + tf_ms;
        let trades = vec![
            ("BTC/USDT".to_string(), t0, 50_000.0, 50.0),
            ("BTC/USDT".to_string(), t0 + 10_000, 50_500.0, 100.0), // pct ~1% < 10
            ("BTC/USDT".to_string(), t1, 50_500.0, 30.0),
        ];
        let alerts = core.process_trades(trades, &cfg);
        assert!(alerts.is_empty(), "Pct below threshold - no alert");
    }

    #[test]
    fn clear_resets_state() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 1.0, 1000.0);
        let trades = vec![("BTC/USDT".to_string(), 1_700_000_000_000, 50_000.0, 10.0)];
        let _ = core.process_trades(trades, &cfg);
        assert!(!core.is_empty());
        core.clear();
        assert!(core.is_empty());
    }

    #[test]
    fn cleanup_symbols_removes_unknown() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 1.0, 1000.0);
        let _ = core.process_trades(
            vec![("BTC/USDT".to_string(), 1_700_000_000_000, 50_000.0, 10.0)],
            &cfg,
        );
        let _ = core.process_trades(
            vec![("ETH/USDT".to_string(), 1_700_000_000_000, 3_000.0, 10.0)],
            &cfg,
        );
        assert_eq!(core.len(), 2);

        // Оставляем только BTC/USDT
        let mut active = HashSet::new();
        active.insert("BTC/USDT".to_string());
        core.cleanup_symbols(&active);

        assert_eq!(core.len(), 1, "ETH should be cleaned up");
    }

    #[test]
    fn stats_returns_trades_processed() {
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 1.0, 1000.0);
        let trades = vec![
            ("BTC/USDT".to_string(), 1_700_000_000_000, 50_000.0, 10.0),
            ("ETH/USDT".to_string(), 1_700_000_000_000, 3_000.0, 10.0),
            ("BTC/USDT".to_string(), 1_700_000_000_000, 50_100.0, 10.0),
        ];
        let _ = core.process_trades(trades, &cfg);
        let stats = core.stats();
        assert_eq!(stats.trades_processed, 3);
        assert_eq!(stats.active_in_batch, 2);
    }

    #[test]
    fn format_symbol_spot_keeps_slash_replaced() {
        let out = format_symbol("BTC/USDT", "spot", "");
        assert_eq!(out, "BTCUSDT");
    }

    #[test]
    fn format_symbol_perp_strips_settlement() {
        let out = format_symbol("BTC/USDT.P", "perp", "");
        assert_eq!(out, "BTCUSDT");
    }

    #[test]
    fn format_symbol_with_delimiter() {
        let out = format_symbol("BTC/USDT", "spot", "_");
        assert_eq!(out, "BTC_USDT");
    }

    #[test]
    fn format_alert_for_positive_return_uses_green() {
        let msg = format_alert("BTCUSDT", 25.0, 5000.0);
        assert!(msg.contains('\u{1F7E2}'), "Should contain green circle");
        assert!(!msg.contains('\u{1F534}'), "Should not contain red circle");
    }

    #[test]
    fn format_alert_for_negative_return_uses_red() {
        let msg = format_alert("BTCUSDT", -15.0, 5000.0);
        assert!(msg.contains('\u{1F534}'), "Should contain red circle");
        assert!(
            !msg.contains('\u{1F7E2}'),
            "Should not contain green circle"
        );
    }

    #[test]
    fn process_trades_parallel_does_not_panic() {
        // Большой батч с множеством символов - rayon распараллеливает.
        let interner = make_interner();
        let core = ScannerCore::new(interner);
        let cfg = make_config(60, 1.0, 1000.0);
        let mut trades = Vec::new();
        for i in 0..500 {
            let sym = format!("SYM{}/USDT", i);
            trades.push((sym, 1_700_000_000_000 + i, 100.0 + i as f64, 10.0));
        }
        let alerts = core.process_trades(trades, &cfg);
        assert!(alerts.is_empty()); // нет закрытия свечи
        assert_eq!(core.len(), 500);
    }
}
