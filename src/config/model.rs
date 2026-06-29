//! Модель данных конфигурации.
//!
//! Конфиг представляет собой JSON-объект с двумя полями:
//! * `global_params` - глобальные настройки (логирование, retention логов).
//! * `scanners` - массив scan-записей, каждая описывает один сканер
//!   для одной биржи и одного типа рынка.
//!
//! Для обратной совместимости конфиг может быть и голым массивом
//! scan-записей (тогда `global_params` берётся по умолчанию).

use serde::Deserialize;
use std::collections::HashSet;

/// Сырой конфиг: верхнеуровневый JSON-объект.
#[derive(Debug, Clone, Deserialize)]
pub struct RawConfig {
    /// Глобальные параметры приложения. Опционально - если отсутствует,
    /// используются значения по умолчанию.
    #[serde(default)]
    pub global_params: GlobalParams,
    /// Список scan-записей.
    #[serde(default)]
    pub scanners: Vec<ScanConfig>,
}

/// Глобальные параметры приложения, не привязанные к конкретному сканеру.
///
/// Эти параметры поддерживают hot-reload: изменение в `config.json`
/// применяется в рантайме без перезапуска (где это технически возможно).
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct GlobalParams {
    /// Уровень логирования в формате `tracing` `EnvFilter`
    /// (например, `"tick_screener=info,warn"` или `"debug"`).
    /// Применяется в рантайме через `reload::Handle`.
    #[serde(default = "GlobalParams::default_log_level")]
    pub log_level: String,
    /// Сколько дней хранить лог-файлы. Применяется фоновой таской
    /// очистки: новый порог подхватывается на следующем тике.
    #[serde(default = "GlobalParams::default_log_retention_days")]
    pub log_retention_days: i64,
}

impl Default for GlobalParams {
    fn default() -> Self {
        Self {
            log_level: Self::default_log_level(),
            log_retention_days: Self::default_log_retention_days(),
        }
    }
}

impl GlobalParams {
    /// Уровень логирования по умолчанию.
    pub fn default_log_level() -> String {
        "tick_screener=info,warn".to_string()
    }

    /// Срок хранения логов по умолчанию (в днях).
    pub fn default_log_retention_days() -> i64 {
        7
    }

    /// Валидирует параметры. Возвращает `Err(message)` если что-то некорректно.
    ///
    /// Некорректный `log_level` обнаруживается только при попытке построить
    /// `EnvFilter`, поэтому здесь проверяем только `log_retention_days`.
    pub fn validate(&self) -> Result<(), String> {
        if self.log_retention_days < 0 {
            return Err(format!(
                "log_retention_days must be >= 0, got {}",
                self.log_retention_days
            ));
        }
        if self.log_level.trim().is_empty() {
            return Err("log_level must not be empty".to_string());
        }
        Ok(())
    }
}

/// Одна scan-запись из `config.json`.
#[derive(Debug, Clone, Deserialize)]
pub struct ScanConfig {
    /// Идентификатор сканера. Формат: `<exchange>_<market_type>`,
    /// например `bybit_spot`, `binance_perp`, `mexc_fut`.
    pub scan: String,
    /// Список исключённых символов в unified-формате (`BTC/USDT`).
    #[serde(default)]
    pub blacklist: Vec<String>,
    /// Тип рынка: spot или perp.
    pub currency_type: MarketType,
    /// Котировка для фильтрации пар: `"USDT"`, `"*USD"`, `"*BTC"`.
    pub quote: String,
    /// Настройки алертов (пороги, Telegram).
    pub alert_settings: AlertSettings,
    /// Настройки обработки (размер батча, задержки).
    pub process_settings: ProcessSettings,
}

/// Тип рынка.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum MarketType {
    /// Спот-рынок.
    Spot,
    /// Бессрочный фьючерс.
    Perp,
}

impl std::fmt::Display for MarketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketType::Spot => write!(f, "spot"),
            MarketType::Perp => write!(f, "perp"),
        }
    }
}

/// Настройки генерации алертов.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AlertSettings {
    /// Порог изменения цены (%) для срабатывания алерта.
    pub return_limit: f64,
    /// Порог объёма ($) для срабатывания алерта.
    pub volume_limit: f64,
    /// Таймфрейм свечи в секундах.
    pub trange: i64,
    /// Настройки Telegram-бота для доставки алертов.
    pub telegram: TelegramSettings,
    /// Разделитель для отображения пары (например, `""` для `BTCUSDT` или `"/"` для `BTC/USDT`).
    /// Нужен только для отображения в алертах, не влияет на фильтры.
    pub delimiter: String,
}

/// Настройки Telegram-канала доставки алертов.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TelegramSettings {
    /// Токен бота (от @BotFather). Пустая строка = алерты отключены.
    pub bot_token: String,
    /// ID чата для доставки.
    pub chat_id: i64,
}

/// Настройки процесса обработки трейдов.
#[derive(Debug, Clone, Deserialize)]
pub struct ProcessSettings {
    /// Количество символов в одном WS-SUBSCRIBE сообщении. Ограничено
    /// `ExchangeConnector::max_subscribe_args`.
    pub pairs_batch_size: usize,
    /// Задержка между отправкой батчей (сек). Предотвращает rate limit.
    pub launch_delay: f64,
}

/// Рантайм-конфиг одного сканера (с разрешёнными типами).
#[derive(Debug, Clone)]
pub struct ScannerRuntimeConfig {
    /// Идентификатор сканера (например, `bybit_spot`).
    pub scanner_id: String,
    /// Биржа.
    pub exchange: crate::exchanges::Exchange,
    /// Тип рынка.
    pub market_type: MarketType,
    /// Котировка-фильтр (исходная строка).
    pub quote: String,
    /// Разрешённые алиасы котировок (`*USD` → `[USDT, USDC, ...]`).
    pub quote_aliases: Vec<String>,
    /// Чёрный список символов.
    pub blacklist: HashSet<String>,
    /// Настройки алертов.
    pub alert_settings: AlertSettings,
    /// Настройки обработки.
    pub process_settings: ProcessSettings,
}

/// Ключ для дедупликации фидов. Два сканера с одним `(exchange, market_type)`
/// шарят один WebSocket-стрим.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct FeedKey {
    /// Биржа.
    pub exchange: crate::exchanges::Exchange,
    /// Тип рынка.
    pub market_type: MarketType,
}

impl FeedKey {
    /// Создаёт новый ключ фида.
    pub fn new(exchange: crate::exchanges::Exchange, market_type: MarketType) -> Self {
        Self {
            exchange,
            market_type,
        }
    }
}

/// Снимок конфигурации после парсинга и разрешения имён.
#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    /// Глобальные параметры приложения.
    pub global_params: GlobalParams,
    /// Список рантайм-конфигов сканеров.
    pub scanners: Vec<ScannerRuntimeConfig>,
}
