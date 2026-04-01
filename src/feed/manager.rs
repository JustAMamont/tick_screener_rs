use crate::config::model::{FeedKey, ScannerRuntimeConfig};
use crate::exchanges::connector::{get_connector_factory, ExchangeConnector};
use crate::exchanges::normalized::NormalizedTrade;
use crate::interner::SymbolInterner;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// A shared exchange feed — one per (exchange, market_type) pair.
struct SharedFeed {
    /// The connector for this feed (owned to keep factory reference)
    _connector: Box<dyn ExchangeConnector>,
    /// Cancellation token for shutting down the WS connection
    cancel: CancellationToken,
    /// Set of scanner IDs subscribed to this feed
    subscribers: RwLock<HashSet<String>>,
    /// Broadcast sender for the WS task
    tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
    /// The tokio JoinHandle for the WS streaming task
    _handle: tokio::task::JoinHandle<()>,
}

/// Manages shared exchange feeds.
pub struct FeedManager {
    feeds: DashMap<FeedKey, Arc<SharedFeed>>,
    interner: Arc<SymbolInterner>,
    /// Store the current pairlist per feed
    pairlists: DashMap<FeedKey, HashSet<String>>,
}

impl FeedManager {
    pub fn new(interner: Arc<SymbolInterner>) -> Self {
        Self {
            feeds: DashMap::new(),
            interner,
            pairlists: DashMap::new(),
        }
    }

    /// Subscribe a scanner to a feed. Creates the feed if it doesn't exist.
    /// Returns a broadcast Receiver for normalized trades.
    pub async fn subscribe(
        &self,
        key: &FeedKey,
        scanner_id: &str,
        config: &ScannerRuntimeConfig,
    ) -> tokio::sync::broadcast::Receiver<NormalizedTrade> {
        // Check if feed already exists
        if let Some(feed) = self.feeds.get(key) {
            feed.subscribers.write().insert(scanner_id.to_string());
            info!(
                "Scanner {} subscribed to existing feed {:?}",
                scanner_id, key
            );
            return feed.tx.subscribe();
        }

        // Create new feed
        let factory = get_connector_factory(key.exchange);
        let connector = factory(key.market_type);

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        // Broadcast channel for trades
        let (tx, _) = tokio::sync::broadcast::channel::<NormalizedTrade>(65536);
        let rx = tx.subscribe();

        // Load markets and get filtered symbol list (now async with retries)
        let symbols = self.load_symbols(&*connector, config).await;

        // Compute batch size clamped to exchange's max_subscribe_args limit
        let batch_size = Self::compute_batch_size(&*connector, config).max(1);

        let tx_clone = tx.clone();
        let symbols_clone = symbols.clone();
        let key_clone = key.clone();
        let config_clone = config.clone();
        let factory_fn = get_connector_factory(key.exchange);

        let handle = tokio::spawn(async move {
            info!(
                "Feed {:?} starting with {} symbols (batch_size={})",
                key_clone, symbols_clone.len(), batch_size
            );

            // Split symbols into batches for WS subscription
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
                let current_batch_start = batch_start;

                // Spawn WS task for this batch
                tokio::spawn(async move {
                    // Cumulative delay: batch 0 = 0s, batch 1 = 1s, batch 2 = 2s, etc.
                    if cumulative_delay > 0.0 {
                        info!(
                            "Feed {:?} batch #{} ({}-{}): waiting {:.1}s before connect",
                            key_clone, current_batch_index, current_batch_start, end, cumulative_delay
                        );
                        tokio::select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs_f64(cumulative_delay)) => {},
                            _ = batch_cancel.cancelled() => return,
                        }
                    }

                    // Exponential backoff retry loop
                    let mut retry_delay = std::time::Duration::from_secs(1);
                    let max_retry_delay = std::time::Duration::from_secs(30);

                    loop {
                        if batch_cancel.is_cancelled() {
                            break;
                        }

                        let connector = factory_fn(key_clone.market_type);

                        match connector
                            .stream_trades(batch.clone(), batch_tx.clone(), batch_cancel.clone())
                            .await
                        {
                            Ok(()) => break,
                            Err(e) => {
                                if batch_cancel.is_cancelled() {
                                    break;
                                }
                                warn!(
                                    "Feed {:?} batch #{} error: {} (retry in {:?})",
                                    key_clone, current_batch_index, e, retry_delay
                                );
                                tokio::select! {
                                    _ = tokio::time::sleep(retry_delay) => {},
                                    _ = batch_cancel.cancelled() => break,
                                }
                                retry_delay = (retry_delay * 2).min(max_retry_delay);
                            }
                        }
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
        });

        self.feeds.insert(key.clone(), feed.clone());
        self.pairlists.insert(key.clone(), symbols.into_iter().collect());

        info!(
            "Created new feed {:?} for scanner {} with {} symbols",
            key, scanner_id, self.pairlists.get(key).map(|p| p.len()).unwrap_or(0)
        );

        rx
    }

    /// Unsubscribe a scanner from a feed. Shuts down the feed if no subscribers left.
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

    /// Get the current pairlist for a feed
    pub fn get_pairlist(&self, key: &FeedKey) -> HashSet<String> {
        self.pairlists
            .get(key)
            .map(|p| p.clone())
            .unwrap_or_default()
    }

    /// Shutdown all feeds
    pub fn shutdown_all(&self) {
        for entry in self.feeds.iter() {
            entry.cancel.cancel();
            info!("Feed {:?} shut down", entry.key());
        }
        self.feeds.clear();
        self.pairlists.clear();
    }

    /// Compute batch size clamped to exchange's max_subscribe_args limit.
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

    /// Load and filter symbols for a connector based on scanner config, with retries
    async fn load_symbols(
        &self,
        connector: &dyn ExchangeConnector,
        config: &ScannerRuntimeConfig,
    ) -> Vec<String> {
        let mut retry_delay = std::time::Duration::from_secs(2);
        let max_retry = 5;
        let mut attempt = 0;

        let markets = loop {
            attempt += 1;
            match connector.load_markets().await {
                Ok(m) => break m,
                Err(e) => {
                    if attempt >= max_retry {
                        error!(
                            "Failed to load markets for {:?} after {} attempts: {}",
                            connector.exchange(), attempt, e
                        );
                        break Vec::new();
                    }
                    warn!(
                        "Failed to load markets for {:?} (attempt {}): {}. Retrying in {:?}",
                        connector.exchange(), attempt, e, retry_delay
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(std::time::Duration::from_secs(30));
                }
            }
        };

        let filtered: Vec<String> = markets
            .into_iter()
            .filter(|m| {
                // Filter by quote currencies
                let quote_match = config.quote_aliases.contains(&m.quote);
                // Filter by blacklist
                let not_blacklisted = !config.blacklist.contains(&m.symbol);
                quote_match && not_blacklisted
            })
            .map(|m| {
                // Intern the symbol
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