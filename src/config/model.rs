use serde::Deserialize;
use std::collections::HashSet;

/// Top-level config: array of scan entries
pub type RawConfig = Vec<ScanConfig>;

/// A single scan entry from config.json
#[derive(Debug, Clone, Deserialize)]
pub struct ScanConfig {
    pub scan: String,
    #[serde(default)]
    pub blacklist: Vec<String>,
    pub currency_type: MarketType,
    pub quote: String,
    pub alert_settings: AlertSettings,
    pub process_settings: ProcessSettings,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum MarketType {
    Spot,
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

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AlertSettings {
    pub return_limit: f64,
    pub volume_limit: f64,
    pub trange: i64,
    pub telegram: TelegramSettings,
    pub delimiter: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TelegramSettings {
    pub bot_token: String,
    pub chat_id: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessSettings {
    pub pairs_batch_size: usize,
    pub launch_delay: f64,
}

/// Parsed runtime config for a scanner (with resolved types)
#[derive(Debug, Clone)]
pub struct ScannerRuntimeConfig {
    pub scanner_id: String,
    pub exchange: crate::exchanges::Exchange,
    pub market_type: MarketType,
    pub quote: String,
    pub quote_aliases: Vec<String>,
    pub blacklist: HashSet<String>,
    pub alert_settings: AlertSettings,
    pub process_settings: ProcessSettings,
}

/// Key used to deduplicate exchange feeds
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct FeedKey {
    pub exchange: crate::exchanges::Exchange,
    pub market_type: MarketType,
}

impl FeedKey {
    pub fn new(exchange: crate::exchanges::Exchange, market_type: MarketType) -> Self {
        Self { exchange, market_type }
    }
}

/// Parsed config snapshot after loading and resolving scan names
#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    pub scanners: Vec<ScannerRuntimeConfig>,
}
