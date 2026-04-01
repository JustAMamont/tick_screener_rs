use crate::config::model::ScannerRuntimeConfig;
use crate::exchanges::normalized::NormalizedTrade;
use crate::scanner::core::{Alert, ScannerConfig, ScannerCore};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

/// Processes trades from a broadcast channel, runs ScannerCore, and sends alerts via mpsc.
pub struct TradeProcessor {
    scanner_id: String,
    core: Arc<ScannerCore>,
    config: Arc<tokio::sync::RwLock<ScannerRuntimeConfig>>,
    alert_tx: mpsc::Sender<(String, Alert)>,
}

impl TradeProcessor {
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

    /// Run the trade processing loop. This should be spawned as a tokio task.
    pub async fn run(self, mut trade_rx: tokio::sync::broadcast::Receiver<NormalizedTrade>) {
        debug!("TradeProcessor started for scanner: {}", self.scanner_id);

        // Cache the last blacklist set to avoid rebuilding ScannerConfig when unchanged
        let mut cached_blacklist_hash: u64 = 0;
        let mut cached_scanner_config: Option<ScannerConfig> = None;

        loop {
            // Drain available trades from the channel (batch processing)
            let mut trades = Vec::with_capacity(512);

            // Wait for first trade with timeout
            tokio::select! {
                result = trade_rx.recv() => {
                    match result {
                        Ok(trade) => trades.push(trade),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Trade channel lagged by {} messages for scanner: {}", n, self.scanner_id);
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

            // Drain remaining available trades (non-blocking)
            while let Ok(trade) = trade_rx.try_recv() {
                trades.push(trade);
                if trades.len() >= 2048 {
                    break;
                }
            }

            if trades.is_empty() {
                continue;
            }

            // Read current config
            let config = self.config.read().await;

            // Filter by quote currencies
            let filtered: Vec<(String, i64, f64, f64)> = trades
                .iter()
                .filter(|t| {
                    let quote = t
                        .symbol
                        .split(':')
                        .next()
                        .and_then(|s| s.split('/').nth(1))
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

            // Build ScannerConfig — only rebuild if blacklist/limits changed
            let blacklist_hash = hash_blacklist(&config.blacklist);
            let scanner_config = if cached_blacklist_hash != blacklist_hash || cached_scanner_config.is_none() {
                let sc = ScannerConfig {
                    return_limit: config.alert_settings.return_limit,
                    volume_limit: config.alert_settings.volume_limit,
                    timeframe_s: config.alert_settings.trange,
                    currency_type: config.market_type.to_string(),
                    delimiter: config.alert_settings.delimiter.clone(),
                    blacklist: Arc::new(config.blacklist.clone()),
                };
                cached_blacklist_hash = blacklist_hash;
                cached_scanner_config = Some(sc);
                cached_scanner_config.as_ref().unwrap().clone()
            } else {
                // Update limits in case they changed (but blacklist didn't)
                let sc = cached_scanner_config.as_mut().unwrap();
                sc.return_limit = config.alert_settings.return_limit;
                sc.volume_limit = config.alert_settings.volume_limit;
                sc.timeframe_s = config.alert_settings.trange;
                sc.delimiter = config.alert_settings.delimiter.clone();
                sc.clone()
            };

            drop(config); // Release read lock

            // Process through ScannerCore (rayon parallel inside)
            let alerts = self.core.process_trades(filtered, &scanner_config);

            // Send alerts
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

/// Simple hash for blacklist set comparison
fn hash_blacklist(set: &std::collections::HashSet<String>) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    for s in set {
        s.hash(&mut hasher);
    }
    hasher.finish()
}
