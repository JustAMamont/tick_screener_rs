//! Реестр конфигурации: хранение текущего снэпшота и вычисление diff.
//!
//! При hot-reload конфига `ConfigRegistry::update` вычисляет, какие
//! сканеры добавились, удалились или изменились - это позволяет
//! применять точечные изменения без полного пересоздания топологии.
//! Также детектируются изменения в `global_params` (log_level,
//! log_retention_days) - они применяются в рантайме через
//! специализированные хендлеры в `logging.rs`.

use crate::config::model::{ConfigSnapshot, FeedKey, GlobalParams, ScannerRuntimeConfig};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Хранит текущий конфиг и вычисляет diff между старым и новым снэпшотами.
///
/// Использует `RwLock<Arc<ConfigSnapshot>>` для lock-free чтения
/// текущего состояния: клонирование `Arc` дешевле, чем блокировка.
pub struct ConfigRegistry {
    current: RwLock<Arc<ConfigSnapshot>>,
}

impl ConfigRegistry {
    /// Создаёт реестр с начальным снэпшотом.
    pub fn new(initial: Arc<ConfigSnapshot>) -> Self {
        Self {
            current: RwLock::new(initial),
        }
    }

    /// Применяет новый снэпшот, возвращает diff изменений.
    ///
    /// # Алгоритм
    ///
    /// 1. Сравниваем `global_params` (log_level, log_retention_days).
    /// 2. Сравниваем ID сканеров (добавленные/удалённые).
    /// 3. Для общих ID сравниваем поля (изменённые).
    /// 4. Сравниваем наборы `FeedKey` (фиды добавлены/удалены).
    ///
    /// # Порядок памяти
    ///
    /// Запись в `current` идёт через `RwLock::write`, что обеспечивает
    /// взаимное исключение и visibility через `parking_lot`.
    pub fn update(&self, new_snapshot: Arc<ConfigSnapshot>) -> ConfigDiff {
        let old = self.current.read().clone();

        let old_ids: HashSet<&str> = old.scanners.iter().map(|s| s.scanner_id.as_str()).collect();
        let new_ids: HashSet<&str> = new_snapshot
            .scanners
            .iter()
            .map(|s| s.scanner_id.as_str())
            .collect();

        let added: Vec<String> = new_ids
            .difference(&old_ids)
            .map(|s| s.to_string())
            .collect();
        let removed: Vec<String> = old_ids
            .difference(&new_ids)
            .map(|s| s.to_string())
            .collect();

        // Находим изменённые сканеры (тот же ID, другой конфиг)
        let new_map: HashMap<&str, &ScannerRuntimeConfig> = new_snapshot
            .scanners
            .iter()
            .map(|s| (s.scanner_id.as_str(), s))
            .collect();

        let old_map: HashMap<&str, &ScannerRuntimeConfig> = old
            .scanners
            .iter()
            .map(|s| (s.scanner_id.as_str(), s))
            .collect();

        let mut modified = Vec::new();
        for id in new_ids.intersection(&old_ids) {
            let old_cfg = old_map[id];
            let new_cfg = new_map[id];
            if old_cfg.quote != new_cfg.quote
                || old_cfg.blacklist != new_cfg.blacklist
                || old_cfg.alert_settings != new_cfg.alert_settings
            {
                modified.push(id.to_string());
            }
        }

        // Считаем изменения фидов
        let old_feeds: HashSet<FeedKey> = old
            .scanners
            .iter()
            .map(|s| FeedKey::new(s.exchange, s.market_type))
            .collect();
        let new_feeds: HashSet<FeedKey> = new_snapshot
            .scanners
            .iter()
            .map(|s| FeedKey::new(s.exchange, s.market_type))
            .collect();

        let feeds_added: Vec<FeedKey> = new_feeds.difference(&old_feeds).copied().collect();
        let feeds_removed: Vec<FeedKey> = old_feeds.difference(&new_feeds).copied().collect();

        // Детектируем изменения в global_params.
        let global_params_changed = old.global_params != new_snapshot.global_params;

        *self.current.write() = new_snapshot;

        ConfigDiff {
            added,
            removed,
            modified,
            feeds_added,
            feeds_removed,
            global_params_changed,
        }
    }

    /// Возвращает клон текущего снэпшота (lock-free через Arc).
    pub fn snapshot(&self) -> Arc<ConfigSnapshot> {
        self.current.read().clone()
    }

    /// Возвращает клон текущих `global_params`.
    pub fn global_params(&self) -> GlobalParams {
        self.current.read().global_params.clone()
    }

    /// Возвращает рантайм-конфиг конкретного сканера по ID.
    pub fn get_scanner_config(&self, scanner_id: &str) -> Option<ScannerRuntimeConfig> {
        self.current
            .read()
            .scanners
            .iter()
            .find(|s| s.scanner_id == scanner_id)
            .cloned()
    }
}

/// Результат diff двух снэпшотов конфигурации.
#[derive(Debug, Clone)]
pub struct ConfigDiff {
    /// Добавленные сканеры (ID).
    pub added: Vec<String>,
    /// Удалённые сканеры (ID).
    pub removed: Vec<String>,
    /// Изменённые сканеры (ID).
    pub modified: Vec<String>,
    /// Добавленные фиды (exchange + market_type).
    pub feeds_added: Vec<FeedKey>,
    /// Удалённые фиды.
    pub feeds_removed: Vec<FeedKey>,
    /// Изменились ли `global_params` (log_level, log_retention_days).
    pub global_params_changed: bool,
}

impl ConfigDiff {
    /// `true` если diff пустой (ничего не изменилось).
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.modified.is_empty()
            && self.feeds_added.is_empty()
            && self.feeds_removed.is_empty()
            && !self.global_params_changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MarketType;
    use crate::config::model::{
        AlertSettings, GlobalParams, ProcessSettings, ScannerRuntimeConfig, TelegramSettings,
    };
    use crate::exchanges::Exchange;
    use std::collections::HashSet;

    fn make_global_params() -> GlobalParams {
        GlobalParams::default()
    }

    fn make_scanner(id: &str, exchange: Exchange, mt: MarketType) -> ScannerRuntimeConfig {
        ScannerRuntimeConfig {
            scanner_id: id.to_string(),
            exchange,
            market_type: mt,
            quote: "USDT".to_string(),
            quote_aliases: vec!["USDT".to_string()],
            blacklist: HashSet::new(),
            alert_settings: AlertSettings {
                return_limit: 1.0,
                volume_limit: 1000.0,
                trange: 60,
                telegram: TelegramSettings {
                    bot_token: "token".to_string(),
                    chat_id: -100,
                },
                delimiter: "".to_string(),
            },
            process_settings: ProcessSettings {
                pairs_batch_size: 100,
                launch_delay: 1.0,
            },
        }
    }

    fn make_snapshot(scanners: Vec<ScannerRuntimeConfig>) -> Arc<ConfigSnapshot> {
        Arc::new(ConfigSnapshot {
            global_params: make_global_params(),
            scanners,
        })
    }

    fn make_snapshot_with_params(
        scanners: Vec<ScannerRuntimeConfig>,
        params: GlobalParams,
    ) -> Arc<ConfigSnapshot> {
        Arc::new(ConfigSnapshot {
            global_params: params,
            scanners,
        })
    }

    #[test]
    fn empty_diff_when_same_snapshot() {
        let snap = make_snapshot(vec![make_scanner(
            "bybit_spot",
            Exchange::Bybit,
            MarketType::Spot,
        )]);
        let reg = ConfigRegistry::new(snap.clone());
        let diff = reg.update(snap);
        assert!(diff.is_empty());
    }

    #[test]
    fn added_detected() {
        let snap1 = make_snapshot(vec![]);
        let reg = ConfigRegistry::new(snap1);
        let snap2 = make_snapshot(vec![make_scanner(
            "bybit_spot",
            Exchange::Bybit,
            MarketType::Spot,
        )]);
        let diff = reg.update(snap2);
        assert_eq!(diff.added, vec!["bybit_spot".to_string()]);
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn removed_detected() {
        let snap1 = make_snapshot(vec![make_scanner(
            "bybit_spot",
            Exchange::Bybit,
            MarketType::Spot,
        )]);
        let reg = ConfigRegistry::new(snap1);
        let snap2 = make_snapshot(vec![]);
        let diff = reg.update(snap2);
        assert_eq!(diff.removed, vec!["bybit_spot".to_string()]);
        assert!(diff.added.is_empty());
    }

    #[test]
    fn modified_detected_on_alert_change() {
        let s1 = make_scanner("bybit_spot", Exchange::Bybit, MarketType::Spot);
        let mut s2 = s1.clone();
        s2.alert_settings.return_limit = 5.0;
        let reg = ConfigRegistry::new(make_snapshot(vec![s1]));
        let diff = reg.update(make_snapshot(vec![s2]));
        assert_eq!(diff.modified, vec!["bybit_spot".to_string()]);
    }

    #[test]
    fn modified_not_detected_on_unchanged() {
        let s1 = make_scanner("bybit_spot", Exchange::Bybit, MarketType::Spot);
        let reg = ConfigRegistry::new(make_snapshot(vec![s1.clone()]));
        let diff = reg.update(make_snapshot(vec![s1]));
        assert!(diff.modified.is_empty());
    }

    #[test]
    fn feeds_added_removed_detected() {
        let snap1 = make_snapshot(vec![
            make_scanner("bybit_spot", Exchange::Bybit, MarketType::Spot),
            make_scanner("binance_perp", Exchange::Binance, MarketType::Perp),
        ]);
        let reg = ConfigRegistry::new(snap1);
        let snap2 = make_snapshot(vec![
            make_scanner("bybit_spot", Exchange::Bybit, MarketType::Spot),
            make_scanner("mexc_perp", Exchange::Mexc, MarketType::Perp),
        ]);
        let diff = reg.update(snap2);
        assert!(
            diff.feeds_added
                .contains(&FeedKey::new(Exchange::Mexc, MarketType::Perp))
        );
        assert!(
            diff.feeds_removed
                .contains(&FeedKey::new(Exchange::Binance, MarketType::Perp))
        );
    }

    #[test]
    fn get_scanner_config_returns_existing() {
        let snap = make_snapshot(vec![make_scanner(
            "bybit_spot",
            Exchange::Bybit,
            MarketType::Spot,
        )]);
        let reg = ConfigRegistry::new(snap);
        let cfg = reg.get_scanner_config("bybit_spot");
        assert!(cfg.is_some());
        assert_eq!(cfg.unwrap().scanner_id, "bybit_spot");
    }

    #[test]
    fn get_scanner_config_returns_none_for_missing() {
        let snap = make_snapshot(vec![]);
        let reg = ConfigRegistry::new(snap);
        assert!(reg.get_scanner_config("nonexistent").is_none());
    }

    #[test]
    fn snapshot_returns_current_state() {
        let snap = make_snapshot(vec![make_scanner(
            "bybit_spot",
            Exchange::Bybit,
            MarketType::Spot,
        )]);
        let reg = ConfigRegistry::new(snap.clone());
        let retrieved = reg.snapshot();
        assert_eq!(retrieved.scanners.len(), 1);
        assert_eq!(retrieved.scanners[0].scanner_id, "bybit_spot");
    }

    // --- Тесты на global_params diff ---

    #[test]
    fn global_params_change_detected_on_log_level() {
        let s1 = make_scanner("bybit_spot", Exchange::Bybit, MarketType::Spot);
        let reg = ConfigRegistry::new(make_snapshot(vec![s1.clone()]));
        let mut new_params = make_global_params();
        new_params.log_level = "debug".to_string();
        let snap2 = make_snapshot_with_params(vec![s1], new_params);
        let diff = reg.update(snap2);
        assert!(diff.global_params_changed);
        assert!(!diff.is_empty());
    }

    #[test]
    fn global_params_change_detected_on_retention_days() {
        let s1 = make_scanner("bybit_spot", Exchange::Bybit, MarketType::Spot);
        let reg = ConfigRegistry::new(make_snapshot(vec![s1.clone()]));
        let mut new_params = make_global_params();
        new_params.log_retention_days = 30;
        let snap2 = make_snapshot_with_params(vec![s1], new_params);
        let diff = reg.update(snap2);
        assert!(diff.global_params_changed);
    }

    #[test]
    fn global_params_no_change_when_same() {
        let s1 = make_scanner("bybit_spot", Exchange::Bybit, MarketType::Spot);
        let params = make_global_params();
        let snap1 = make_snapshot_with_params(vec![s1.clone()], params.clone());
        let snap2 = make_snapshot_with_params(vec![s1], params);
        let reg = ConfigRegistry::new(snap1);
        let diff = reg.update(snap2);
        assert!(!diff.global_params_changed);
        assert!(diff.is_empty());
    }

    #[test]
    fn global_params_change_with_scanner_changes_both_reported() {
        let s1 = make_scanner("bybit_spot", Exchange::Bybit, MarketType::Spot);
        let mut s2 = s1.clone();
        s2.alert_settings.return_limit = 5.0;
        let mut new_params = make_global_params();
        new_params.log_level = "debug".to_string();
        let reg = ConfigRegistry::new(make_snapshot(vec![s1]));
        let diff = reg.update(make_snapshot_with_params(vec![s2], new_params));
        assert!(diff.global_params_changed);
        assert_eq!(diff.modified, vec!["bybit_spot".to_string()]);
    }

    #[test]
    fn global_params_accessor_returns_current() {
        let mut params = make_global_params();
        params.log_retention_days = 14;
        let snap = make_snapshot_with_params(vec![], params.clone());
        let reg = ConfigRegistry::new(snap);
        assert_eq!(reg.global_params(), params);
    }
}
