//! Процессор трейдов: связывает broadcast-канал фида с ядром сканера.
//!
//! Каждый сканер имеет свой `TradeProcessor`, который:
//! 1. Читает трейды из broadcast-канала (батчингом).
//! 2. Фильтрует по котировке и blacklist-у.
//! 3. Передаёт батч в `ScannerCore` (rayon-параллелизм внутри).
//! 4. Отправляет сгенерированные алерты в mpsc-канал `AlertRouter`.

use crate::config::model::ScannerRuntimeConfig;
use crate::exchanges::normalized::NormalizedTrade;
use crate::scanner::core::{Alert, ScannerConfig, ScannerCore};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

/// Процессор трейдов одного сканера.
///
/// Связывает broadcast-приёмник фида с ядром сканера и каналом алертов.
/// Запускается как отдельная tokio-таска через [`Self::run`].
pub struct TradeProcessor {
    /// ID сканера (для логов и маршрутизации алертов).
    scanner_id: String,
    /// Ядро сканера (разделяемое, потоко-безопасное).
    core: Arc<ScannerCore>,
    /// Текущий конфиг сканера (с hot-reload через RwLock).
    config: Arc<tokio::sync::RwLock<ScannerRuntimeConfig>>,
    /// Канал отправки алертов в `AlertRouter`.
    alert_tx: mpsc::Sender<(String, Alert)>,
}

impl TradeProcessor {
    /// Создаёт новый процессор.
    pub fn new(
        scanner_id: String,
        core: Arc<ScannerCore>,
        config: Arc<tokio::sync::RwLock<ScannerRuntimeConfig>>,
        alert_tx: mpsc::Sender<(String, Alert)>,
    ) -> Self {
        Self {
            scanner_id,
            core,
            config,
            alert_tx,
        }
    }

    /// Главный цикл обработки трейдов. Запускается как tokio-таска.
    ///
    /// # Алгоритм
    ///
    /// 1. Ждём первый трейде с timeout 100мс (если нет - пересоздаём батч).
    /// 2. Неблокирующе drain-им остальные доступные трейды (до 2048).
    /// 3. Читаем текущий конфиг, фильтруем трейды по котировке/blacklist.
    /// 4. Строим `ScannerConfig` (с кэшированием если blacklist не изменился).
    /// 5. Вызываем `ScannerCore::process_trades` - rayon-параллелизм внутри.
    /// 6. Отправляем алерты в `alert_tx`.
    ///
    /// # Кэширование
    ///
    /// `cached_scanner_config` хранит последний построенный `ScannerConfig`.
    /// Если `blacklist` не изменился (по хэшу), переиспользуем клон
    /// структуры - экономим на `Arc::clone` для blacklist.
    pub async fn run(self, mut trade_rx: tokio::sync::broadcast::Receiver<NormalizedTrade>) {
        debug!("TradeProcessor started for scanner: {}", self.scanner_id);

        // Кэшируем хэш blacklist и последний построенный ScannerConfig
        let mut cached_blacklist_hash: u64 = 0;
        let mut cached_scanner_config: Option<ScannerConfig> = None;

        loop {
            // Drain доступных трейдов из канала (батчинг)
            let mut trades = Vec::with_capacity(512);

            // Ждём первый трейд с timeout
            tokio::select! {
                result = trade_rx.recv() => {
                    match result {
                        Ok(trade) => trades.push(trade),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(
                                "Trade channel lagged by {} messages for scanner: {}",
                                n, self.scanner_id
                            );
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            debug!("Trade channel closed for scanner: {}", self.scanner_id);
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
            }

            // Добавляем остальные доступные трейды. Не блокирует
            while let Ok(trade) = trade_rx.try_recv() {
                trades.push(trade);
                if trades.len() >= 2048 {
                    break;
                }
            }

            if trades.is_empty() {
                continue;
            }

            // Читаем текущий конфиг
            let config = self.config.read().await;

            // Фильтруем по котировке и блеклисту
            let filtered: Vec<(String, i64, f64, f64)> = trades
                .iter()
                .filter(|t| {
                    // Извлекаем котировку: `BTC/USDT` -> `USDT`, `BTC/USDT.P` -> `USDT`.
                    let quote = t
                        .symbol
                        .split('/')
                        .nth(1)
                        .map(|s| s.strip_suffix(".P").unwrap_or(s))
                        .unwrap_or("");
                    config.quote_aliases.iter().any(|q| q == quote)
                        && !config.blacklist.contains(&t.symbol)
                })
                .map(|t| (t.symbol.clone(), t.timestamp_ms, t.price, t.cost))
                .collect();

            if filtered.is_empty() {
                drop(config);
                continue;
            }

            // Строим ScannerConfig - только перестраиваем если блеклист изменился
            let blacklist_hash = hash_blacklist(&config.blacklist);
            let scanner_config =
                if cached_blacklist_hash != blacklist_hash || cached_scanner_config.is_none() {
                    let sc = ScannerConfig {
                        return_limit: config.alert_settings.return_limit,
                        volume_limit: config.alert_settings.volume_limit,
                        timeframe_s: config.alert_settings.trange,
                        currency_type: config.market_type.to_string(),
                        delimiter: config.alert_settings.delimiter.clone(),
                        blacklist: Arc::new(config.blacklist.clone()),
                    };
                    cached_blacklist_hash = blacklist_hash;
                    let cloned = sc.clone();
                    cached_scanner_config = Some(sc);
                    cloned
                } else if let Some(sc) = cached_scanner_config.as_mut() {
                    // Обновляем только лимиты (блеклист не изменился)
                    sc.return_limit = config.alert_settings.return_limit;
                    sc.volume_limit = config.alert_settings.volume_limit;
                    sc.timeframe_s = config.alert_settings.trange;
                    sc.delimiter = config.alert_settings.delimiter.clone();
                    sc.clone()
                } else {
                    // Недостижимо: проверили is_none() выше.
                    unreachable!("cached_scanner_config must be Some after the first branch")
                };

            drop(config); // Освобождаем read-lock

            // Обработка через ScannerCore (rayon-параллелизм внутри)
            let alerts = self.core.process_trades(filtered, &scanner_config);

            // Отправляем алерты
            let alert_count = alerts.len();
            for alert in alerts {
                if let Err(e) = self.alert_tx.send((self.scanner_id.clone(), alert)).await {
                    warn!("Failed to send alert: {}", e);
                }
            }

            trace!(
                "Processed {} trades, {} alerts for scanner: {}",
                trades.len(),
                alert_count,
                self.scanner_id
            );
        }

        debug!("TradeProcessor stopped for scanner: {}", self.scanner_id);
    }
}

/// Считает хэш blacklist-сета для быстрого сравнения.
///
/// # Примечание
///
/// Порядок итерации `HashSet` не детерминирован, поэтому одинаковые
/// сеты могут давать разные хэши. Это нормально для нашей задачи -
/// если хэш изменился, мы перестраховываемся и перестраиваем
/// `ScannerConfig`. Если не изменился - конфиг точно не поменялся.
fn hash_blacklist(set: &std::collections::HashSet<String>) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    for s in set {
        s.hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_blacklist_same_content_same_hash() {
        // Тот же сет (в том же порядке итерации) - тот же хэш.
        // Поскольку HashSet итерация не детерминирована, проверяем
        // что повторный вызов на том же экземпляре даёт тот же хэш.
        let mut set = std::collections::HashSet::new();
        set.insert("BTC".to_string());
        set.insert("ETH".to_string());
        let h1 = hash_blacklist(&set);
        let h2 = hash_blacklist(&set);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_blacklist_different_content_different_hash() {
        let mut s1 = std::collections::HashSet::new();
        s1.insert("BTC".to_string());
        let mut s2 = std::collections::HashSet::new();
        s2.insert("ETH".to_string());
        // Не детерминировано, но практически всегда разные хэши
        assert_ne!(hash_blacklist(&s1), hash_blacklist(&s2));
    }

    #[test]
    fn hash_blacklist_empty_set_returns_some_hash() {
        let set = std::collections::HashSet::new();
        let h = hash_blacklist(&set);
        // Любой хэш - валидный, главное что не panic
        let _ = h;
    }
}
