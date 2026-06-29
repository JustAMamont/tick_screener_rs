//! Точка входа в приложение: разбор аргументов, инициализация и запуск.
//!
//! Создаёт `App`, инициализирует конфиг-вотчер, фид-менеджер, сканеры,
//! AlertRouter и фоновые таски (мониторинг, рефреш pairlist-ов, hot-reload).
//! Завершение по `Ctrl+C` с корректным graceful shutdown-ом.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tick_screener::{
    alert::{AlertRouter, BotPool},
    config::{
        ConfigRegistry, ConfigWatcher,
        model::{ConfigSnapshot, FeedKey, ScannerRuntimeConfig},
    },
    feed::FeedManager,
    interner::SymbolInterner,
    logging,
    scanner::{Alert, ScannerCore, TradeProcessor},
};

use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Состояние приложения, разделяемое между компонентами.
///
/// Все поля - `Arc` или `Arc<RwLock<...>>`, что позволяет безопасно
/// делить состояние между tokio-тасками.
struct App {
    /// Глобальный токен отмены. Завершает все фоновые таски при `Ctrl+C`.
    cancel: CancellationToken,
    /// Путь к `config.json`. Используется только для диагностики.
    #[allow(dead_code)]
    config_path: PathBuf,
    /// Реестр конфигурации (текущий снэпшот + вычисление diff).
    registry: Arc<ConfigRegistry>,
    /// Ядра сканеров по `scanner_id`. Каждый сканер имеет свой `ScannerCore`.
    scanner_cores: Arc<RwLock<HashMap<String, Arc<ScannerCore>>>>,
    /// Текущие конфиги сканеров. Читается `AlertRouter`-ом для маршрутизации алертов.
    scanner_configs: Arc<RwLock<Vec<ScannerRuntimeConfig>>>,
    /// Arc-обёртки конфигов - позволяют hot-reload-ить отдельные сканеры.
    scanner_config_arcs: Arc<RwLock<HashMap<String, Arc<RwLock<ScannerRuntimeConfig>>>>>,
    /// Менеджер разделяемых фидов (`(exchange, market_type)` -> WS-стрим).
    feed_manager: Arc<FeedManager>,
    /// Канал отправки алертов: `(scanner_id, Alert)` -> `AlertRouter`.
    alert_tx: mpsc::Sender<(String, Alert)>,
    /// Пул Telegram-ботов (дедуплицированный по токену).
    bot_pool: BotPool,
    /// JoinHandle-ы тасок процессоров трейдов (для graceful shutdown).
    processor_handles: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
    /// Разделяемый интернер символов.
    interner: Arc<SymbolInterner>,
    /// Runtime логирования: позволяет применять новый `log_level`
    /// и `log_retention_days` при hot-reload.
    log_runtime: logging::LogRuntime,
}

impl App {
    fn new(
        config_path: PathBuf,
        initial_snapshot: Arc<ConfigSnapshot>,
        alert_tx: mpsc::Sender<(String, Alert)>,
        interner: Arc<SymbolInterner>,
        log_runtime: logging::LogRuntime,
    ) -> Self {
        let feed_manager = Arc::new(FeedManager::new(Arc::clone(&interner)));
        let registry = Arc::new(ConfigRegistry::new(initial_snapshot));

        Self {
            cancel: CancellationToken::new(),
            config_path,
            registry,
            scanner_cores: Arc::new(RwLock::new(HashMap::new())),
            scanner_configs: Arc::new(RwLock::new(Vec::new())),
            scanner_config_arcs: Arc::new(RwLock::new(HashMap::new())),
            feed_manager,
            alert_tx,
            bot_pool: BotPool::new(),
            processor_handles: Arc::new(RwLock::new(HashMap::new())),
            interner,
            log_runtime,
        }
    }

    async fn build_topology(&mut self, snapshot: &ConfigSnapshot) {
        info!("Building topology for {} scanners", snapshot.scanners.len());
        *self.scanner_configs.write().await = snapshot.scanners.clone();
        let mut config_arcs = self.scanner_config_arcs.write().await;
        config_arcs.clear();

        for scanner_config in &snapshot.scanners {
            self.spawn_scanner(scanner_config, &mut config_arcs).await;
        }
    }

    async fn spawn_scanner(
        &self,
        scanner_config: &ScannerRuntimeConfig,
        config_arcs: &mut HashMap<String, Arc<RwLock<ScannerRuntimeConfig>>>,
    ) {
        let scanner_id = scanner_config.scanner_id.clone();
        let feed_key = FeedKey::new(scanner_config.exchange, scanner_config.market_type);

        let core = Arc::new(ScannerCore::new(Arc::clone(&self.interner)));
        self.scanner_cores
            .write()
            .await
            .insert(scanner_id.clone(), Arc::clone(&core));

        // Добавлен .await, так как функция теперь асинхронная
        let trade_rx = self
            .feed_manager
            .subscribe(&feed_key, &scanner_id, scanner_config)
            .await;

        let config_arc = Arc::new(RwLock::new(scanner_config.clone()));
        config_arcs.insert(scanner_id.clone(), Arc::clone(&config_arc));

        let processor =
            TradeProcessor::new(scanner_id.clone(), core, config_arc, self.alert_tx.clone());

        let handle = tokio::spawn(async move {
            processor.run(trade_rx).await;
        });

        self.processor_handles
            .write()
            .await
            .insert(scanner_id.clone(), handle);

        info!(
            "Scanner '{}' started (exchange={}, market={}, quote={})",
            scanner_id, scanner_config.exchange, scanner_config.market_type, scanner_config.quote,
        );
    }

    async fn apply_diff(&self, snapshot: Arc<ConfigSnapshot>) {
        let diff = self.registry.update(snapshot.clone());

        // Применяем изменения global_params (log_level, log_retention_days)
        // в рантайме - не зависит от топологии сканеров.
        if diff.global_params_changed {
            info!(
                "Applying global_params update: log_level={}, log_retention_days={}",
                snapshot.global_params.log_level, snapshot.global_params.log_retention_days
            );
            self.log_runtime.apply_global_params(&snapshot.global_params);
        }

        if diff.is_empty() {
            info!("Config reloaded but no changes detected");
            return;
        }

        info!(
            "Config diff: added={:?} removed={:?} modified={:?} feeds_added={:?} feeds_removed={:?} global_params_changed={}",
            diff.added, diff.removed, diff.modified, diff.feeds_added, diff.feeds_removed,
            diff.global_params_changed,
        );

        *self.scanner_configs.write().await = snapshot.scanners.clone();

        // Обновление конфигурационных параметров для каждого сканера, 
        // чтобы TradeProcessor увидел новые значения при обработке следующего батча.
        {
            let config_arcs = self.scanner_config_arcs.write().await;
            for scanner_config in &snapshot.scanners {
                if let Some(arc) = config_arcs.get(&scanner_config.scanner_id) {
                    *arc.write().await = scanner_config.clone();
                }
            }
        }

        for scanner_id in &diff.removed {
            self.remove_scanner(scanner_id).await;
        }

        let mut config_arcs = self.scanner_config_arcs.write().await;
        for scanner_id in &diff.added {
            if let Some(config) = snapshot
                .scanners
                .iter()
                .find(|s| &s.scanner_id == scanner_id)
            {
                self.spawn_scanner(config, &mut config_arcs).await;
                info!("Scanner '{}' added via hot-reload", scanner_id);
            }
        }

        for scanner_id in &diff.modified {
            // Дампим в лог, что именно изменилось в конфигурации сканера.
            if let Some(new_cfg) = snapshot
                .scanners
                .iter()
                .find(|s| &s.scanner_id == scanner_id)
            {
                info!(
                    "Scanner '{}' config updated via hot-reload: return_limit={} volume_limit={} trange={} pairs_batch_size={}",
                    scanner_id,
                    new_cfg.alert_settings.return_limit,
                    new_cfg.alert_settings.volume_limit,
                    new_cfg.alert_settings.trange,
                    new_cfg.process_settings.pairs_batch_size,
                );
            } else {
                info!("Scanner '{}' config updated via hot-reload", scanner_id);
            }
        }

        let active_tokens: HashSet<String> = snapshot
            .scanners
            .iter()
            .map(|c| c.alert_settings.telegram.bot_token.clone())
            .filter(|t| !t.is_empty())
            .collect();
        self.bot_pool.cleanup(&active_tokens);
    }

    async fn remove_scanner(&self, scanner_id: &str) {
        let feed_key = {
            let configs = self.scanner_configs.read().await;
            configs
                .iter()
                .find(|c| c.scanner_id == *scanner_id)
                .map(|c| FeedKey::new(c.exchange, c.market_type))
        };

        if let Some(key) = feed_key {
            self.feed_manager.unsubscribe(&key, scanner_id);
        }

        if let Some(handle) = self.processor_handles.write().await.remove(scanner_id) {
            handle.abort();
        }

        self.scanner_cores.write().await.remove(scanner_id);
        self.scanner_config_arcs.write().await.remove(scanner_id);

        info!("Scanner '{}' removed via hot-reload", scanner_id);
    }
}

async fn run_pairlist_refresher(
    alert_tx: mpsc::Sender<(String, Alert)>,
    scanner_configs: Arc<RwLock<Vec<ScannerRuntimeConfig>>>,
    feed_manager: Arc<FeedManager>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(120));
    let mut historical_pairlists: HashMap<FeedKey, HashSet<String>> = HashMap::new();

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let configs = scanner_configs.read().await;
                let mut feeds_to_check = HashSet::new();

                for config in configs.iter() {
                    feeds_to_check.insert(FeedKey::new(config.exchange, config.market_type));
                }

                for key in feeds_to_check {
                    let factory = tick_screener::exchanges::connector::get_connector_factory(key.exchange);
                    let connector = factory(key.market_type);

                    match connector.load_markets().await {
                        Ok(markets) => {
                            let current_symbols: HashSet<String> = markets.into_iter().map(|m| m.symbol).collect();

                            if let Some(previous) = historical_pairlists.get(&key) {
                                let new_pairs: Vec<String> = current_symbols.difference(previous).cloned().collect();

                                for pair in new_pairs {
                                    info!("⚡️ NEW LISTING detected: {} on {:?}", pair, key);

                                    for scanner_config in configs.iter() {
                                        let sk = FeedKey::new(scanner_config.exchange, scanner_config.market_type);
                                        if sk == key {
                                            // Извлекаем котировку из unified-символа
                                            // `BTC/USDT` -> `USDT`, `BTC/USDT.P` -> `USDT`.
                                            let quote = pair
                                                .split('/')
                                                .nth(1)
                                                .map(|s| s.strip_suffix(".P").unwrap_or(s))
                                                .unwrap_or("");
                                            if !scanner_config.quote_aliases.iter().any(|q| q == quote) {
                                                continue;
                                            }

                                            // Для отображения отрезаем `.P` и заменяем `/` на разделитель.
                                            let display_with_delim = pair.replace('/', &scanner_config.alert_settings.delimiter);
                                            let display = display_with_delim
                                                .strip_suffix(".P")
                                                .unwrap_or(&display_with_delim)
                                                .to_string();

                                            let msg = format!(
                                                "⚡️ ⚡️ ⚡️ *NEW LISTING* ⚡️ ⚡️ ⚡️\n\n`{}`\nExchange: *{}*\nMarket: *{}*",
                                                display, key.exchange, key.market_type
                                            );

                                            let listing_alert = Alert {
                                                symbol: pair.clone(),
                                                ts: chrono::Utc::now().timestamp_millis(),
                                                message: msg,
                                                alert_type: "listing".to_string(),
                                                pin: true,
                                            };
                                            let _ = alert_tx.send((scanner_config.scanner_id.clone(), listing_alert)).await;
                                        }
                                    }
                                }
                            }

                            historical_pairlists.insert(key, current_symbols);
                        }
                        Err(e) => {
                            tracing::warn!("Refresher: failed to load markets for {:?}: {}", key, e);
                        }
                    }
                }
                drop(configs);

                // Проверка мертвых фидов (вебсокетов) 
                for (feed_key, config) in feed_manager.feeds_needing_repair() {
                    warn!(
                        "Dead feed detected: {:?} has empty pairlist, triggering repair",
                        feed_key
                    );
                    let factory = tick_screener::exchanges::connector::get_connector_factory(feed_key.exchange);
                    let connector = factory(feed_key.market_type);
                    match connector.load_markets().await {
                        Ok(markets) => {
                            let symbols: HashSet<String> = markets.into_iter()
                                .filter(|m| config.quote_aliases.contains(&m.quote) && !config.blacklist.contains(&m.symbol))
                                .map(|m| m.symbol)
                                .collect();
                            if !symbols.is_empty() {
                                info!("Dead feed {:?} repaired with {} symbols", feed_key, symbols.len());
                                feed_manager.unsubscribe(&feed_key, "__repair__");
                            }
                        }
                        Err(e) => {
                            warn!("Dead feed {:?} repair failed: {}", feed_key, e);
                        }
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

#[tokio::main]
async fn main() {
    let log_runtime = logging::init_logger();
    info!("tick-screener starting (Rust edition 2024)");

    let config_path = PathBuf::from("config.json");

    let (watcher, mut config_rx) = ConfigWatcher::new(config_path.clone());

    let initial_snapshot = match watcher.load_initial() {
        Ok(s) => {
            info!("Initial config loaded: {} scanners", s.scanners.len());
            // Применяем начальные global_params (log_level, retention)
            // к runtime логирования.
            log_runtime.apply_global_params(&s.global_params);
            s
        }
        Err(e) => {
            error!("Failed to load initial config: {}", e);
            std::process::exit(1);
        }
    };

    // Создаём разделяемый канал алертов
    let (alert_tx, alert_rx) = mpsc::channel(1024);

    // Создаём разделяемый интернер
    let interner = Arc::new(SymbolInterner::new());

    // Создаём приложение
    let mut app = App::new(
        config_path,
        initial_snapshot,
        alert_tx.clone(),
        Arc::clone(&interner),
        log_runtime.clone(),
    );
    app.build_topology(&app.registry.snapshot()).await;

    // Запускаем фоновую таску очистки старых логов.
    // Крутится всё время работы приложения, читает актуальный retention_days
    // из log_runtime (включая hot-reload) каждые 5 минут.
    let cleanup_cancel = app.cancel.clone();
    logging::spawn_log_cleanup_task(log_runtime.clone(), cleanup_cancel);

    // Оборачиваем приложение в Arc<Mutex> для доступа из hot-reload
    let app = Arc::new(Mutex::new(app));

    // Запускаем вотчер конфига
    let config_cancel = CancellationToken::new();
    let config_cancel_clone = config_cancel.clone();
    let config_handle = tokio::spawn(async move {
        watcher.run(config_cancel_clone).await;
    });

    // Запускаем роутер алертов
    let scanner_configs_for_router = {
        let a = app.lock().await;
        Arc::clone(&a.scanner_configs)
    };
    let bot_pool_for_router = {
        let a = app.lock().await;
        a.bot_pool.clone()
    };
    let alert_router = AlertRouter::new(bot_pool_for_router, alert_rx, scanner_configs_for_router);
    let mut alert_handle = tokio::spawn(async move {
        alert_router.run().await;
    });

    // Запускаем монитор
    let monitor_cancel = {
        let a = app.lock().await;
        a.cancel.clone()
    };
    let monitor_cores = {
        let a = app.lock().await;
        Arc::clone(&a.scanner_cores)
    };
    let monitor_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;

        let mut prev_trades: HashMap<String, u64> = HashMap::new();
        let mut prev_alerts: HashMap<String, u64> = HashMap::new();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let cores = monitor_cores.read().await;
                    for (scanner_id, core) in cores.iter() {
                        let stats = core.stats(); // flushes thread-local to global

                        let prev_t = prev_trades.get(scanner_id).copied().unwrap_or(0);
                        let prev_a = prev_alerts.get(scanner_id).copied().unwrap_or(0);

                        let trades_delta = stats.trades_processed.saturating_sub(prev_t);
                        let alerts_delta = stats.alerts_generated.saturating_sub(prev_a);

                        tracing::info!(
                            "📊 Monitor [{scanner_id}] trades={trades_delta} alerts={alerts_delta} candles={} active={} | closed={} vol_ok={} pct_ok={} suppressed={} max_vol={:.0}$ max_pct={:.3}%",
                            stats.symbols_count, stats.active_in_batch,
                            stats.candle_stats.candles_closed,
                            stats.candle_stats.candles_vol_ok,
                            stats.candle_stats.candles_pct_ok,
                            stats.candle_stats.candles_suppressed,
                            stats.candle_stats.max_volume,
                            stats.candle_stats.max_pct,
                        );

                        prev_trades.insert(scanner_id.clone(), stats.trades_processed);
                        prev_alerts.insert(scanner_id.clone(), stats.alerts_generated);
                    }
                }
                _ = monitor_cancel.cancelled() => break,
            }
        }
    });

    // Старт обновлятора списка пар
    let refresh_cancel = {
        let a = app.lock().await;
        a.cancel.clone()
    };
    let refresh_configs = {
        let a = app.lock().await;
        Arc::clone(&a.scanner_configs)
    };
    let refresh_feed_manager = {
        let a = app.lock().await;
        Arc::clone(&a.feed_manager)
    };
    let refresh_handle = tokio::spawn(run_pairlist_refresher(
        alert_tx.clone(),
        refresh_configs,
        refresh_feed_manager,
        refresh_cancel,
    ));

    // Hot-reload конфига (изменение параметров сканеров, добавление/удаление сканеров в процессе работы)
    let reload_cancel = {
        let a = app.lock().await;
        a.cancel.clone()
    };
    let app_for_reload = Arc::clone(&app);
    let reload_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = config_rx.recv() => {
                    match result {
                        Ok(snapshot) => {
                            info!("Config update received, applying topology diff...");
                            let app = app_for_reload.lock().await;
                            app.apply_diff(snapshot).await;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("Config channel lagged by {} messages", n);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = reload_cancel.cancelled() => break,
            }
        }
    });

    // Хендлеры остановки программы
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
    }

    // Корректное завершение работы
    info!("Initiating graceful shutdown...");

    // 1. Отменяем вотчер конфига
    config_cancel.cancel();
    config_handle.abort();

    // 2. Отменяем глобальный токен (monitor, refresh, reload используют его клоны)
    {
        let app = app.lock().await;
        app.cancel.cancel();
        app.feed_manager.shutdown_all();

        let handles = app.processor_handles.read().await;
        for handle in handles.values() {
            handle.abort();
        }
    }

    // 3. Абортим фоновые таски напрямую
    monitor_handle.abort();
    refresh_handle.abort();
    reload_handle.abort();

    // 4. Дропаем alert_tx, чтобы alert_rx закрылся и AlertRouter завершился
    drop(alert_tx);
    match tokio::time::timeout(Duration::from_secs(3), &mut alert_handle).await {
        Ok(_) => {}
        Err(_) => alert_handle.abort(),
    }

    info!("tick-screener stopped");
}
