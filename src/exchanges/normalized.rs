/// Unified trade data from any exchange. This is the common data type
/// that flows through the entire pipeline.
#[derive(Debug, Clone)]
pub struct NormalizedTrade {
    /// Unified symbol: "BTC/USDT" for spot, "BTC/USDT:USDT" for perp
    pub symbol: String,
    /// Trade timestamp in milliseconds
    pub timestamp_ms: i64,
    /// Trade price
    pub price: f64,
    /// Trade cost = price * amount (volume in quote currency)
    pub cost: f64,
    /// Which exchange this trade came from
    pub exchange: Exchange,
}

/// Supported exchanges
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum Exchange {
    Bybit,
    Kucoin,
    Bitget,
    Gate,
    Mexc,
}

impl Exchange {
    /// Parse exchange name from scan config key (e.g., "bybit_spot" -> Bybit)
    pub fn from_scan_name(name: &str) -> Option<Self> {
        let lower = name.to_lowercase();
        if lower.starts_with("bybit") {
            Some(Exchange::Bybit)
        } else if lower.starts_with("kucoin") {
            Some(Exchange::Kucoin)
        } else if lower.starts_with("bitget") {
            Some(Exchange::Bitget)
        } else if lower.starts_with("gate") {
            Some(Exchange::Gate)
        } else if lower.starts_with("mexc") {
            Some(Exchange::Mexc)
        } else {
            None
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Exchange::Bybit => "bybit",
            Exchange::Kucoin => "kucoin",
            Exchange::Bitget => "bitget",
            Exchange::Gate => "gate",
            Exchange::Mexc => "mexc",
        }
    }
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Market info returned by load_markets
#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub symbol: String,      // unified: "BTC/USDT"
    pub base: String,        // "BTC"
    pub quote: String,       // "USDT"
    pub active: bool,
    pub market_type: crate::config::MarketType,
    pub raw_symbol: String,  // exchange-native: "BTCUSDT"
}
