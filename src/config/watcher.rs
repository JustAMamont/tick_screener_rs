use crate::config::model::{ConfigSnapshot, RawConfig, ScannerRuntimeConfig, FeedKey};
use crate::exchanges::Exchange;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const DEBOUNCE_MS: u64 = 200;
/// Minimum interval between config reloads (prevents notify event storms)
const RELOAD_COOLDOWN_MS: u64 = 2000;

pub struct ConfigWatcher {
    path: PathBuf,
    tx: broadcast::Sender<Arc<ConfigSnapshot>>,
}

impl ConfigWatcher {
    pub fn new(path: PathBuf) -> (Self, broadcast::Receiver<Arc<ConfigSnapshot>>) {
        let (tx, rx) = broadcast::channel(16);
        (Self { path, tx }, rx)
    }

    /// Load config initially (blocking)
    pub fn load_initial(&self) -> Result<Arc<ConfigSnapshot>, String> {
        let content = std::fs::read_to_string(&self.path)
            .map_err(|e| format!("Failed to read config {}: {}", self.path.display(), e))?;
        let raw: RawConfig = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse config: {}", e))?;
        let snapshot = Self::resolve(raw);
        let arc = Arc::new(snapshot);
        let _ = self.tx.send(arc.clone());
        Ok(arc)
    }

    /// Start watching for file changes (async)
    pub async fn run(self, cancel: CancellationToken) {
        let path = self.path.clone();
        let tx = self.tx.clone();

        // Spawn the file system watcher
        let watcher_cancel = cancel.clone();
        let watch_path = path.clone();

        tokio::task::spawn_blocking(move || {
            let (signal_tx, signal_rx) = crossbeam_channel::bounded::<()>(1);
            let watch_path_inner = watch_path.clone();

            let mut debouncer = match notify_debouncer_mini::new_debouncer(
                Duration::from_millis(DEBOUNCE_MS),
                move |res: Result<Vec<notify_debouncer_mini::DebouncedEvent>, notify_debouncer_mini::notify::Error>| {
                    if res.is_ok() {
                        let _ = signal_tx.try_send(());
                    }
                },
            ) {
                Ok(d) => d,
                Err(e) => {
                    error!("Failed to create file watcher: {}", e);
                    return;
                }
            };

            if let Err(e) = debouncer.watcher().watch(&watch_path_inner, notify_debouncer_mini::notify::RecursiveMode::NonRecursive) {
                error!("Failed to watch config file: {}", e);
                return;
            }

            // Read events in a loop
            let mut last_reload = std::time::Instant::now()
                .checked_sub(Duration::from_secs(10)).unwrap(); // allow first reload immediately

            loop {
                if watcher_cancel.is_cancelled() {
                    break;
                }

                match signal_rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(_) => {
                        // Cooldown: skip if we reloaded recently
                        let elapsed = last_reload.elapsed();
                        if elapsed < Duration::from_millis(RELOAD_COOLDOWN_MS) {
                            let remaining = Duration::from_millis(RELOAD_COOLDOWN_MS) - elapsed;
                            std::thread::sleep(remaining);
                            continue;
                        }

                        // File changed, reload config
                        match std::fs::read_to_string(&watch_path) {
                            Ok(content) => {
                                match serde_json::from_str::<RawConfig>(&content) {
                                    Ok(raw) => {
                                        let snapshot = Self::resolve(raw);
                                        let arc = Arc::new(snapshot);
                                        if tx.send(arc).is_err() {
                                            // No receivers
                                        } else {
                                            info!("Config reloaded successfully");
                                        }
                                        last_reload = std::time::Instant::now();
                                    }
                                    Err(e) => {
                                        warn!("Failed to parse config after change: {}", e);
                                        last_reload = std::time::Instant::now();
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to read config after change: {}", e);
                            }
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        continue;
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        break;
                    }
                }
            }
        });

        // Wait for cancellation
        cancel.cancelled().await;
    }

    /// Resolve raw config into runtime configs
    fn resolve(raw: RawConfig) -> ConfigSnapshot {
        let mut feed_scanners: HashMap<FeedKey, Vec<String>> = HashMap::new();
        let mut scanners = Vec::with_capacity(raw.len());

        for scan in raw {
            let exchange = match Exchange::from_scan_name(&scan.scan) {
                Some(ex) => ex,
                None => {
                    warn!("Unknown exchange in scan name: {}", scan.scan);
                    continue;
                }
            };

            let quote_aliases = resolve_quote_aliases(&scan.quote);

            let runtime_config = ScannerRuntimeConfig {
                scanner_id: scan.scan.clone(),
                exchange,
                market_type: scan.currency_type,
                quote: scan.quote.clone(),
                quote_aliases,
                blacklist: scan.blacklist.into_iter().collect(),
                alert_settings: scan.alert_settings,
                process_settings: scan.process_settings,
            };

            let feed_key = FeedKey::new(exchange, scan.currency_type);
            feed_scanners.entry(feed_key).or_default().push(scan.scan.clone());

            scanners.push(runtime_config);
        }

        // We could log feed sharing info here
        for (key, scanner_ids) in &feed_scanners {
            if scanner_ids.len() > 1 {
                info!(
                    "Feed sharing: {:?} {} -> scanners: {:?}",
                    key.exchange, key.market_type, scanner_ids
                );
            }
        }

        ConfigSnapshot { scanners }
    }
}

/// Resolve quote filter: "USDT" -> ["USDT"], "*USD" -> ["USDT", "USDC", "BUSD", "FDUSD", ...]
fn resolve_quote_aliases(quote: &str) -> Vec<String> {
    match quote {
        "*USD" => vec![
            "USDT".to_string(),
            "USDC".to_string(),
            "BUSD".to_string(),
            "FDUSD".to_string(),
            "TUSD".to_string(),
            "USDP".to_string(),
            "DAI".to_string(),
        ],
        "*BTC" => vec![
            "BTC".to_string(),
            "WBTC".to_string(),
        ],
        _ => vec![quote.to_string()],
    }
}
