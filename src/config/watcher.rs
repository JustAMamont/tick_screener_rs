use crate::config::model::{ConfigSnapshot, FeedKey, GlobalParams, RawConfig, ScannerRuntimeConfig};
use crate::exchanges::Exchange;
use crate::io_util::read_file;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Задержка debounce для файлового вотчера (мс). События файловой
/// системы приходят пачками по несколько штук за одно сохранение -
/// debounce предотвращает множественные перезагрузки конфига.
const DEBOUNCE_MS: u64 = 200;

/// Минимальный интервал между перезагрузками конфига (мс). Защита от
/// notify event storm - даже если файловая система шлёт много событий,
/// перезагрузка не чаще чем раз в 2 секунды.
const RELOAD_COOLDOWN_MS: u64 = 2000;

/// Вотчер конфигурационного файла с поддержкой hot-reload.
///
/// Использует `notify-debouncer-mini` для отслеживания изменений
/// `config.json`. При обнаружении изменения читает файл (через
/// [`crate::io_util::read_file`] - io_uring или fallback), парсит
/// JSON, валидирует и отправляет новый [`ConfigSnapshot`] в
/// broadcast-канал для последующей обработки в `App::apply_diff`.
///
/// Поддерживается два формата `config.json`:
/// 1. **Объект** (рекомендуется): `{"global_params": {...}, "scanners": [...]}`.
/// 2. **Массив** (legacy, обратная совместимость): `[...]` - тогда
///    `global_params` берётся по умолчанию.
pub struct ConfigWatcher {
    path: PathBuf,
    tx: broadcast::Sender<Arc<ConfigSnapshot>>,
}

impl ConfigWatcher {
    /// Создаёт новый вотчер. Возвращает сам вотчер и `Receiver` для
    /// получения обновлений конфига.
    pub fn new(path: PathBuf) -> (Self, broadcast::Receiver<Arc<ConfigSnapshot>>) {
        let (tx, rx) = broadcast::channel(16);
        (Self { path, tx }, rx)
    }

    /// Синхронно загружает начальный конфиг. Должен вызываться до
    /// запуска асинхронного цикла [`Self::run`].
    ///
    /// # Ошибки
    ///
    /// Возвращает `Err(message)` если файл не существует, нечитаем
    /// или содержит невалидный JSON. В этом случае приложение должно
    /// завершиться с ошибкой.
    pub fn load_initial(&self) -> Result<Arc<ConfigSnapshot>, String> {
        let content = std::fs::read_to_string(&self.path)
            .map_err(|e| format!("Failed to read config {}: {}", self.path.display(), e))?;
        let raw = Self::parse_config(&content)?;
        let snapshot = Self::resolve(raw);
        let arc = Arc::new(snapshot);
        let _ = self.tx.send(arc.clone());
        Ok(arc)
    }

    /// Парсит содержимое `config.json`, поддерживая оба формата
    /// (объект с `global_params`/`scanners` или legacy-массив).
    ///
    /// # Ошибки
    ///
    /// Возвращает `Err(message)` если JSON невалиден или не соответствует
    /// ни одному из ожидаемых форматов.
    fn parse_config(content: &str) -> Result<RawConfig, String> {
        // Сначала пробуем распарсить как объект с global_params и scanners.
        // Это рекомендуемый формат.
        let trimmed = content.trim_start();
        if trimmed.starts_with('{') {
            let raw: RawConfig =
                serde_json::from_str(content).map_err(|e| format!("Failed to parse config: {}", e))?;
            return Ok(raw);
        }

        // Legacy-формат: голый массив scan-записей. Для обратной
        // совместимости оборачиваем в RawConfig с дефолтными global_params.
        if trimmed.starts_with('[') {
            let scanners: Vec<crate::config::model::ScanConfig> =
                serde_json::from_str(content).map_err(|e| format!("Failed to parse config: {}", e))?;
            return Ok(RawConfig {
                global_params: GlobalParams::default(),
                scanners,
            });
        }

        Err("Config must be a JSON object or array".to_string())
    }

    /// Запускает асинхронный цикл слежения за файлом. Работает до
    /// отмены через `cancel`.
    ///
    /// # Реализация
    ///
    /// Файловый вотчер `notify` блокирующий, поэтому он запускается в
    /// `spawn_blocking`. Чтение файла при срабатывании - через
    /// [`crate::io_util::read_file`] (io_uring если включён feature,
    /// иначе `tokio::fs`).
    pub async fn run(self, cancel: CancellationToken) {
        let path = self.path.clone();
        let tx = self.tx.clone();
        let watcher_cancel = cancel.clone();
        let watch_path = path.clone();

        // Блокирующий файловый вотчер в отдельном потоке.
        tokio::task::spawn_blocking(move || {
            let (signal_tx, signal_rx) = crossbeam_channel::bounded::<()>(1);
            let watch_path_inner = watch_path.clone();

            let mut debouncer = match notify_debouncer_mini::new_debouncer(
                Duration::from_millis(DEBOUNCE_MS),
                move |res: Result<
                    Vec<notify_debouncer_mini::DebouncedEvent>,
                    notify_debouncer_mini::notify::Error,
                >| {
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

            if let Err(e) = debouncer.watcher().watch(
                &watch_path_inner,
                notify_debouncer_mini::notify::RecursiveMode::NonRecursive,
            ) {
                error!("Failed to watch config file: {}", e);
                return;
            }

            // Вспомогательный runtime для асинхронного чтения файла
            // из блокирующего потока. Используем current_thread.
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    error!("Failed to create runtime for config reads: {}", e);
                    return;
                }
            };

            let mut last_reload = std::time::Instant::now()
                .checked_sub(Duration::from_secs(10))
                .unwrap(); // позволяем первую перезагрузку сразу

            loop {
                if watcher_cancel.is_cancelled() {
                    break;
                }

                match signal_rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(_) => {
                        // Cooldown: пропускаем если недавно перезагружали
                        let elapsed = last_reload.elapsed();
                        if elapsed < Duration::from_millis(RELOAD_COOLDOWN_MS) {
                            let remaining = Duration::from_millis(RELOAD_COOLDOWN_MS) - elapsed;
                            std::thread::sleep(remaining);
                            continue;
                        }

                        // Файл изменился - перезагружаем конфиг через io_util (io_uring или fallback)
                        let read_result = rt.block_on(read_file(&watch_path));
                        match read_result {
                            Ok(content) => {
                                match Self::parse_config(&content) {
                                    Ok(raw) => {
                                        // Валидируем global_params перед применением.
                                        if let Err(e) = raw.global_params.validate() {
                                            warn!("Invalid global_params in config: {}", e);
                                            last_reload = std::time::Instant::now();
                                            continue;
                                        }
                                        let snapshot = Self::resolve(raw);
                                        let arc = Arc::new(snapshot);
                                        if tx.send(arc).is_err() {
                                            // Нет получателей - приложение завершилось
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
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        // Ждём отмены
        cancel.cancelled().await;
    }

    /// Преобразует сырой конфиг из JSON в рантайм-снэпшот.
    ///
    /// Разрешает алиасы котировок (`*USD` -> `["USDT", "USDC", ...]`),
    /// преобразует `scan` строку в [`Exchange`] и формирует
    /// [`ScannerRuntimeConfig`] для каждой записи.
    fn resolve(raw: RawConfig) -> ConfigSnapshot {
        let mut feed_scanners: HashMap<FeedKey, Vec<String>> = HashMap::new();
        let mut scanners = Vec::with_capacity(raw.scanners.len());

        for scan in raw.scanners {
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
            feed_scanners
                .entry(feed_key)
                .or_default()
                .push(scan.scan.clone());

            scanners.push(runtime_config);
        }

        // Логируем шаринг фидов между сканерами
        for (key, scanner_ids) in &feed_scanners {
            if scanner_ids.len() > 1 {
                info!(
                    "Feed sharing: {:?} {} -> scanners: {:?}",
                    key.exchange, key.market_type, scanner_ids
                );
            }
        }

        ConfigSnapshot {
            global_params: raw.global_params,
            scanners,
        }
    }
}

/// Разрешает wildcard алиасы котировок.
///
/// `*USD` разворачивается во все стейблкоины (USDT, USDC, BUSD, FDUSD,
/// TUSD, USDP, DAI). `*BTC` - во все BTC-вариации. Любое другое
/// значение превращается в синглтон `[quote]`.
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
        "*BTC" => vec!["BTC".to_string(), "WBTC".to_string()],
        _ => vec![quote.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn make_test_config_object() -> &'static str {
        r#"{
            "global_params": {
                "log_level": "tick_screener=debug,warn",
                "log_retention_days": 14
            },
            "scanners": [
                {
                    "scan": "bybit_spot",
                    "blacklist": [],
                    "currency_type": "spot",
                    "quote": "*USD",
                    "alert_settings": {
                        "return_limit": 1.0,
                        "volume_limit": 2000.0,
                        "trange": 60,
                        "telegram": { "bot_token": "test", "chat_id": -100 },
                        "delimiter": ""
                    },
                    "process_settings": {
                        "pairs_batch_size": 100,
                        "launch_delay": 1.0
                    }
                }
            ]
        }"#
    }

    fn make_test_config_legacy_array() -> &'static str {
        r#"[
            {
                "scan": "bybit_spot",
                "blacklist": [],
                "currency_type": "spot",
                "quote": "*USD",
                "alert_settings": {
                    "return_limit": 1.0,
                    "volume_limit": 2000.0,
                    "trange": 60,
                    "telegram": { "bot_token": "test", "chat_id": -100 },
                    "delimiter": ""
                },
                "process_settings": {
                    "pairs_batch_size": 100,
                    "launch_delay": 1.0
                }
            }
        ]"#
    }

    fn make_test_config_object_default_params() -> &'static str {
        r#"{
            "scanners": [
                {
                    "scan": "bybit_spot",
                    "blacklist": [],
                    "currency_type": "spot",
                    "quote": "USDT",
                    "alert_settings": { "return_limit": 1.0, "volume_limit": 1.0, "trange": 60,
                        "telegram": {"bot_token":"", "chat_id":0}, "delimiter":"" },
                    "process_settings": { "pairs_batch_size": 100, "launch_delay": 1.0 }
                }
            ]
        }"#
    }

    #[test]
    fn resolve_quote_aliases_usd_wildcard() {
        let aliases = resolve_quote_aliases("*USD");
        assert!(aliases.contains(&"USDT".to_string()));
        assert!(aliases.contains(&"USDC".to_string()));
        assert!(aliases.contains(&"BUSD".to_string()));
        assert!(aliases.contains(&"FDUSD".to_string()));
        assert!(aliases.contains(&"TUSD".to_string()));
        assert!(aliases.contains(&"USDP".to_string()));
        assert!(aliases.contains(&"DAI".to_string()));
        assert_eq!(aliases.len(), 7);
    }

    #[test]
    fn resolve_quote_aliases_btc_wildcard() {
        let aliases = resolve_quote_aliases("*BTC");
        assert_eq!(aliases, vec!["BTC".to_string(), "WBTC".to_string()]);
    }

    #[test]
    fn resolve_quote_aliases_single() {
        let aliases = resolve_quote_aliases("USDT");
        assert_eq!(aliases, vec!["USDT".to_string()]);
    }

    #[test]
    fn parse_config_object_format_with_global_params() {
        let raw = ConfigWatcher::parse_config(make_test_config_object()).expect("parse");
        assert_eq!(raw.scanners.len(), 1);
        assert_eq!(raw.global_params.log_level, "tick_screener=debug,warn");
        assert_eq!(raw.global_params.log_retention_days, 14);
    }

    #[test]
    fn parse_config_legacy_array_format_uses_default_global_params() {
        let raw = ConfigWatcher::parse_config(make_test_config_legacy_array()).expect("parse");
        assert_eq!(raw.scanners.len(), 1);
        assert_eq!(raw.global_params.log_level, GlobalParams::default_log_level());
        assert_eq!(
            raw.global_params.log_retention_days,
            GlobalParams::default_log_retention_days()
        );
    }

    #[test]
    fn parse_config_object_without_global_params_uses_defaults() {
        let raw =
            ConfigWatcher::parse_config(make_test_config_object_default_params()).expect("parse");
        assert_eq!(raw.scanners.len(), 1);
        assert_eq!(raw.global_params.log_level, GlobalParams::default_log_level());
        assert_eq!(
            raw.global_params.log_retention_days,
            GlobalParams::default_log_retention_days()
        );
    }

    #[test]
    fn parse_config_rejects_non_object_non_array() {
        let result = ConfigWatcher::parse_config("\"just a string\"");
        assert!(result.is_err());
        let result = ConfigWatcher::parse_config("42");
        assert!(result.is_err());
    }

    #[test]
    fn parse_config_rejects_invalid_json() {
        let result = ConfigWatcher::parse_config("not valid json at all");
        assert!(result.is_err());
    }

    #[test]
    fn resolve_object_format_preserves_global_params() {
        let raw = ConfigWatcher::parse_config(make_test_config_object()).unwrap();
        let snapshot = ConfigWatcher::resolve(raw);
        assert_eq!(snapshot.scanners.len(), 1);
        assert_eq!(snapshot.scanners[0].exchange, Exchange::Bybit);
        assert_eq!(snapshot.scanners[0].scanner_id, "bybit_spot");
        assert_eq!(snapshot.global_params.log_level, "tick_screener=debug,warn");
        assert_eq!(snapshot.global_params.log_retention_days, 14);
    }

    #[test]
    fn resolve_drops_unknown_exchange() {
        let raw: RawConfig = serde_json::from_str(
            r#"{
                "scanners": [
                    { "scan": "unknown_exchange", "blacklist": [], "currency_type": "spot",
                      "quote": "USDT",
                      "alert_settings": { "return_limit": 1.0, "volume_limit": 1.0, "trange": 60,
                        "telegram": {"bot_token":"", "chat_id":0}, "delimiter":"" },
                      "process_settings": { "pairs_batch_size": 100, "launch_delay": 1.0 } }
                ]
            }"#,
        )
        .unwrap();
        let snapshot = ConfigWatcher::resolve(raw);
        assert_eq!(
            snapshot.scanners.len(),
            0,
            "Unknown exchange should be skipped"
        );
    }

    #[test]
    fn load_initial_reads_valid_config_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.json");
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(make_test_config_object().as_bytes()).expect("write");
        drop(f);

        let (watcher, _rx) = ConfigWatcher::new(path);
        let snap = watcher.load_initial().expect("load");
        assert_eq!(snap.scanners.len(), 1);
        assert_eq!(snap.global_params.log_retention_days, 14);
    }

    #[test]
    fn load_initial_reads_legacy_array_config() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.json");
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(make_test_config_legacy_array().as_bytes()).expect("write");
        drop(f);

        let (watcher, _rx) = ConfigWatcher::new(path);
        let snap = watcher.load_initial().expect("load");
        assert_eq!(snap.scanners.len(), 1);
        assert_eq!(
            snap.global_params.log_retention_days,
            GlobalParams::default_log_retention_days()
        );
    }

    #[test]
    fn load_initial_fails_for_missing_file() {
        let (watcher, _rx) = ConfigWatcher::new(PathBuf::from("/nonexistent/config.json"));
        assert!(watcher.load_initial().is_err());
    }

    #[test]
    fn load_initial_fails_for_invalid_json() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.json");
        std::fs::write(&path, "not valid json").expect("write");
        let (watcher, _rx) = ConfigWatcher::new(path);
        assert!(watcher.load_initial().is_err());
    }

    #[test]
    fn resolve_quote_aliases_for_perp_market() {
        let raw: RawConfig = serde_json::from_str(
            r#"{
                "scanners": [
                    { "scan": "binance_perp", "blacklist": [], "currency_type": "perp",
                      "quote": "USDT",
                      "alert_settings": { "return_limit": 1.0, "volume_limit": 1.0, "trange": 60,
                        "telegram": {"bot_token":"", "chat_id":0}, "delimiter":"" },
                      "process_settings": { "pairs_batch_size": 100, "launch_delay": 1.0 } }
                ]
            }"#,
        )
        .unwrap();
        let snapshot = ConfigWatcher::resolve(raw);
        assert_eq!(
            snapshot.scanners[0].market_type,
            crate::config::MarketType::Perp
        );
        assert_eq!(snapshot.scanners[0].exchange, Exchange::Binance);
    }

    // --- Тесты на GlobalParams ---

    #[test]
    fn global_params_defaults_are_sensible() {
        let params = GlobalParams::default();
        assert!(!params.log_level.is_empty());
        assert!(params.log_retention_days > 0);
    }

    #[test]
    fn global_params_validate_rejects_negative_retention() {
        let params = GlobalParams {
            log_retention_days: -1,
            ..Default::default()
        };
        assert!(params.validate().is_err());
    }

    #[test]
    fn global_params_validate_rejects_empty_log_level() {
        let params = GlobalParams {
            log_level: "   ".to_string(),
            ..Default::default()
        };
        assert!(params.validate().is_err());
    }

    #[test]
    fn global_params_validate_accepts_valid_params() {
        let params = GlobalParams::default();
        assert!(params.validate().is_ok());
    }

    #[test]
    fn global_params_validate_accepts_zero_retention() {
        // 0 = не удалять логи (хотя это и не рекомендуется)
        let params = GlobalParams {
            log_retention_days: 0,
            ..Default::default()
        };
        assert!(params.validate().is_ok());
    }

    #[test]
    fn global_params_partial_deserialize_uses_defaults() {
        // Только log_level, без log_retention_days - должно взять дефолт.
        let json = r#"{"log_level": "debug"}"#;
        let params: GlobalParams = serde_json::from_str(json).expect("parse");
        assert_eq!(params.log_level, "debug");
        assert_eq!(
            params.log_retention_days,
            GlobalParams::default_log_retention_days()
        );
    }

    #[test]
    fn global_params_empty_object_uses_all_defaults() {
        let json = r#"{}"#;
        let params: GlobalParams = serde_json::from_str(json).expect("parse");
        assert_eq!(params.log_level, GlobalParams::default_log_level());
        assert_eq!(
            params.log_retention_days,
            GlobalParams::default_log_retention_days()
        );
    }
}
