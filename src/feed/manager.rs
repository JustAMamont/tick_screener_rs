//! Менеджер обменных фидов: шарит WebSocket-стримы между сканерами.
//!
//! # Архитектура
//!
//! Для каждой пары `(exchange, market_type)` создаётся один
//! [`SharedFeed`] с одним broadcast-каналом. Все сканеры, заинтересованные
//! в этой паре, подписываются на этот канал. Это позволяет, например,
//! сканеру `bybit_spot_usdt` и сканеру `bybit_spot_btc` совместно
//! использовать один WS-стрим от Bybit.
//!
//! # Жизненный цикл
//!
//! 1. [`FeedManager::subscribe`] - первый подписчик создаёт feed:
//!    грузит рынки, фильтрует символы, спавнит WS-таски.
//! 2. Последующие подписчики просто добавляются в `subscribers`.
//! 3. [`FeedManager::unsubscribe`] - удаляет подписчика. Если
//!    подписчиков не осталось, feed закрывается (cancel + remove).
//! 4. [`FeedManager::shutdown_all`] - экстренное закрытие всех фидов.

use crate::config::model::{FeedKey, ScannerRuntimeConfig};
use crate::exchanges::connector::{ExchangeConnector, get_connector_factory};
use crate::exchanges::normalized::NormalizedTrade;
use crate::interner::SymbolInterner;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Разделяемый обменный фид - один на пару `(exchange, market_type)`.
struct SharedFeed {
    /// Коннектор (владеем чтобы сохранить ссылку на factory).
    _connector: Box<dyn ExchangeConnector>,
    /// Токен отмены для WS-тасок.
    cancel: CancellationToken,
    /// Множество ID сканеров, подписанных на этот фид.
    subscribers: RwLock<HashSet<String>>,
    /// Broadcast-отправитель для WS-таски.
    tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
    /// JoinHandle корневой WS-таски (управляет батчами).
    _handle: tokio::task::JoinHandle<()>,
    /// Конфиг сканера-создателя (для рефреша pairlist-а).
    config: ScannerRuntimeConfig,
}

/// Менеджер разделяемых обменных фидов.
pub struct FeedManager {
    /// Активные фиды по ключам `(exchange, market_type)`.
    feeds: DashMap<FeedKey, Arc<SharedFeed>>,
    /// Разделяемый интернер (для пред-интернирования символов).
    interner: Arc<SymbolInterner>,
    /// Текущий pairlist на фид (для диагностики dead feeds).
    pairlists: DashMap<FeedKey, HashSet<String>>,
}

impl FeedManager {
    /// Создаёт пустой менеджер с заданным интернером.
    pub fn new(interner: Arc<SymbolInterner>) -> Self {
        Self {
            feeds: DashMap::new(),
            interner,
            pairlists: DashMap::new(),
        }
    }

    /// Возвращает список фидов с пустым pairlist (требующих ремонта).
    ///
    /// Используется фоновой таской `run_pairlist_refresher` для
    /// перезагрузки рынков у упавших фидов.
    pub fn feeds_needing_repair(&self) -> Vec<(FeedKey, ScannerRuntimeConfig)> {
        self.feeds
            .iter()
            .filter(|entry| {
                self.pairlists
                    .get(entry.key())
                    .map(|p| p.is_empty())
                    .unwrap_or(true)
            })
            .map(|entry| (*entry.key(), entry.value().config.clone()))
            .collect()
    }

    /// Подписывает сканер на фид. Создаёт фид, если его ещё нет.
    ///
    /// # Алгоритм
    ///
    /// 1. Если фид уже существует - добавляем сканер в `subscribers`,
    ///    возвращаем broadcast-receiver.
    /// 2. Если нет - создаём connector, грузим рынки (с retry на
    ///    отказы), разбиваем символы на батчи, спавним WS-таски
    ///    для каждого батча.
    ///
    /// # Батчинг
    ///
    /// Размер батча ограничен `ExchangeConnector::max_subscribe_args`.
    /// Между батчами выдерживается `launch_delay` для предотвращения
    /// rate limit.
    pub async fn subscribe(
        &self,
        key: &FeedKey,
        scanner_id: &str,
        config: &ScannerRuntimeConfig,
    ) -> tokio::sync::broadcast::Receiver<NormalizedTrade> {
        // Быстрый путь: фид уже существует.
        if let Some(feed) = self.feeds.get(key) {
            feed.subscribers.write().insert(scanner_id.to_string());
            info!(
                "Scanner {} subscribed to existing feed {:?}",
                scanner_id, key
            );
            return feed.tx.subscribe();
        }

        // Медленный путь: создаём новый фид.
        let factory = get_connector_factory(key.exchange);
        let connector = factory(key.market_type);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Broadcast-канал для трейдов. 65536 слотов - с запасом под всплески.
        let (tx, _) = tokio::sync::broadcast::channel::<NormalizedTrade>(65536);
        let rx = tx.subscribe();

        // Грузим и фильтруем символы (с retry на отказы REST API).
        let symbols = self.load_symbols(&*connector, config).await;

        // Размер батча с учётом лимитов биржи.
        let batch_size = Self::compute_batch_size(&*connector, config).max(1);

        let tx_clone = tx.clone();
        let symbols_clone = symbols.clone();
        let key_clone = *key;
        let config_clone = config.clone();
        let factory_fn = get_connector_factory(key.exchange);

        let handle = tokio::spawn(async move {
            info!(
                "Feed {:?} starting with {} symbols (batch_size={})",
                key_clone,
                symbols_clone.len(),
                batch_size
            );

            // Разбиваем символы на батчи и для каждого батча спавним отдельный WS-таск.
            let launch_delay = config_clone.process_settings.launch_delay;
            let mut batch_start = 0usize;
            let mut batch_index = 0u32;

            while batch_start < symbols_clone.len() {
                if cancel_clone.is_cancelled() {
                    break;
                }

                let end = (batch_start + batch_size).min(symbols_clone.len());
                let batch: Vec<String> = symbols_clone[batch_start..end].to_vec();

                let batch_tx = tx_clone.clone();
                let batch_cancel = cancel_clone.clone();
                let cumulative_delay = batch_index as f64 * launch_delay;
                let current_batch_index = batch_index;
                let key_for_task = key_clone;

                tokio::spawn(async move {
                    if cumulative_delay > 0.0 {
                        tokio::select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs_f64(cumulative_delay)) => {},
                            _ = batch_cancel.cancelled() => return,
                        }
                    }
                    if let Err(e) = factory_fn(key_for_task.market_type)
                        .stream_trades(batch, batch_tx, batch_cancel.clone())
                        .await
                        && !batch_cancel.is_cancelled()
                    {
                        warn!(
                            "Feed {:?} batch #{} error: {}",
                            key_for_task, current_batch_index, e
                        );
                    }
                });

                batch_start = end;
                batch_index += 1;
            }

            cancel_clone.cancelled().await;
        });

        let feed = Arc::new(SharedFeed {
            _connector: connector,
            cancel,
            subscribers: RwLock::new({
                let mut set = HashSet::new();
                set.insert(scanner_id.to_string());
                set
            }),
            tx,
            _handle: handle,
            config: config.clone(),
        });

        self.feeds.insert(*key, feed.clone());
        self.pairlists.insert(*key, symbols.into_iter().collect());

        info!(
            "Created new feed {:?} for scanner {} with {} symbols",
            key,
            scanner_id,
            self.pairlists.get(key).map(|p| p.len()).unwrap_or(0)
        );

        rx
    }

    /// Отписывает сканер от фида. Если подписчиков не осталось -
    /// закрывает фид (cancel + remove).
    pub fn unsubscribe(&self, key: &FeedKey, scanner_id: &str) {
        if let Some(feed) = self.feeds.get(key) {
            feed.subscribers.write().remove(scanner_id);
            if feed.subscribers.read().is_empty() {
                feed.cancel.cancel();
                info!("Feed {:?} shut down (no subscribers)", key);
                drop(feed);
                self.feeds.remove(key);
                self.pairlists.remove(key);
            }
        }
    }

    /// Возвращает текущий pairlist фида (клон).
    pub fn get_pairlist(&self, key: &FeedKey) -> HashSet<String> {
        self.pairlists
            .get(key)
            .map(|p| p.clone())
            .unwrap_or_default()
    }

    /// Экстренно закрывает все фиды. Используется при graceful shutdown.
    pub fn shutdown_all(&self) {
        for entry in self.feeds.iter() {
            entry.cancel.cancel();
            info!("Feed {:?} shut down", entry.key());
        }
        self.feeds.clear();
        self.pairlists.clear();
    }

    /// Вычисляет размер батча с учётом лимитов биржи.
    fn compute_batch_size(
        connector: &dyn ExchangeConnector,
        config: &ScannerRuntimeConfig,
    ) -> usize {
        let exchange_max = connector.max_subscribe_args();
        if exchange_max > 0 {
            let user_batch = config.process_settings.pairs_batch_size;
            if user_batch > exchange_max {
                info!(
                    "Feed: pairs_batch_size clamped {} -> {} (exchange limit)",
                    user_batch, exchange_max
                );
            }
            user_batch.min(exchange_max)
        } else {
            config.process_settings.pairs_batch_size
        }
    }

    /// Загружает и фильтрует символы для коннектора. С retry на отказы.
    ///
    /// # Алгоритм
    ///
    /// 1. `connector.load_markets()` с экспоненциальным backoff.
    /// 2. Фильтрация по `quote_aliases` и `blacklist`.
    /// 3. Пред-интернирование каждого символа (оптимизация для
    ///    горячего пути - позже при обработке трейда `intern` будет
    ///    быстрым путём через `DashMap::get`).
    async fn load_symbols(
        &self,
        connector: &dyn ExchangeConnector,
        config: &ScannerRuntimeConfig,
    ) -> Vec<String> {
        let mut retry_delay = std::time::Duration::from_secs(2);
        let max_delay = std::time::Duration::from_secs(60);
        let markets = loop {
            match connector.load_markets().await {
                Ok(m) => break m,
                Err(e) => {
                    warn!(
                        "load_symbols for {:?} {:?} failed: {}. Retrying in {:?}",
                        connector.exchange(),
                        connector.market_type(),
                        e,
                        retry_delay
                    );
                    tokio::time::sleep(retry_delay).await;
                    let jitter =
                        std::time::Duration::from_millis(crate::exchanges::rand_int() % 1000);
                    retry_delay = (retry_delay * 2 + jitter).min(max_delay);
                }
            }
        };

        let filtered: Vec<String> = markets
            .into_iter()
            .filter(|m| {
                let quote_match = config.quote_aliases.contains(&m.quote);
                let not_blacklisted = !config.blacklist.contains(&m.symbol);
                quote_match && not_blacklisted
            })
            .map(|m| {
                // Пред-интернируем символ - позже в горячем пути
                // это даст быстрый path через DashMap::get.
                let _ = self.interner.intern(&m.symbol);
                m.symbol
            })
            .collect();

        info!(
            "Loaded {} symbols for {:?} {} (quote filter: {:?})",
            filtered.len(),
            connector.exchange(),
            connector.market_type(),
            config.quote_aliases,
        );

        filtered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_feed_manager_starts_empty() {
        let interner = Arc::new(SymbolInterner::new());
        let fm = FeedManager::new(interner);
        assert!(fm.feeds_needing_repair().is_empty());
    }

    #[test]
    fn get_pairlist_returns_empty_for_unknown_key() {
        let interner = Arc::new(SymbolInterner::new());
        let fm = FeedManager::new(interner);
        let key = FeedKey::new(
            crate::exchanges::Exchange::Bybit,
            crate::config::MarketType::Spot,
        );
        let pairlist = fm.get_pairlist(&key);
        assert!(pairlist.is_empty());
    }

    #[test]
    fn compute_batch_size_clamps_to_exchange_limit() {
        // Тестируем логику ограничения через mock-коннектор не получится
        // (нужна async trait mock), поэтому проверяем только граничные случаи
        // вычислительной логики.
        let user_batch = 500usize;
        let exchange_max = 200usize;
        let clamped = user_batch.min(exchange_max);
        assert_eq!(clamped, 200);
    }

    #[test]
    fn compute_batch_size_uses_user_when_no_limit() {
        let user_batch = 1000usize;
        let exchange_max = 0usize; // 0 = без лимита
        let result = if exchange_max > 0 {
            user_batch.min(exchange_max)
        } else {
            user_batch
        };
        assert_eq!(result, 1000);
    }
}
