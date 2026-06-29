//! Нормализованные типы данных, единые для всех бирж.
//!
//! Все коннекторы преобразуют свои биржа-специфичные структуры в
//! эти типы, что позволяет остальным компонентам приложения работать
//! агностически к конкретной бирже.

/// Нормализованный трейд из любой биржи. Общий тип данных, который
/// протекает через весь конвейер обработки.
#[derive(Debug, Clone)]
pub struct NormalizedTrade {
    /// Unified-символ: `"BTC/USDT"` для spot, `"BTC/USDT.P"` для perp.
    pub symbol: String,
    /// Метка времени сделки в миллисекундах.
    pub timestamp_ms: i64,
    /// Цена сделки.
    pub price: f64,
    /// Объём сделки в котируемой валюте (`price * amount`).
    pub cost: f64,
    /// Биржа-источник сделки.
    pub exchange: Exchange,
}

/// Поддерживаемые биржи.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum Exchange {
    /// Binance (spot + perp).
    Binance,
    /// Bybit (spot + perp).
    Bybit,
    /// Kucoin (spot + perp).
    Kucoin,
    /// Bitget (spot + perp).
    Bitget,
    /// Gate (spot + perp).
    Gate,
    /// MEXC (spot + perp).
    Mexc,
}

impl Exchange {
    /// Разбирает имя биржи из ключа scan-конфига (например, `"bybit_spot"` -> `Exchange::Bybit`).
    ///
    /// # Возвращаемое значение
    /// `Some(variant)` если префикс имени совпал с одной из поддерживаемых бирж,
    /// иначе `None`. Сопоставление префиксов регистронезависимое.
    pub fn from_scan_name(name: &str) -> Option<Self> {
        let lower = name.to_lowercase();
        if lower.starts_with("binance") {
            Some(Exchange::Binance)
        } else if lower.starts_with("bybit") {
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

    /// Строковое представление биржи (используется в логах и алертах).
    pub fn as_str(&self) -> &'static str {
        match self {
            Exchange::Binance => "binance",
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

/// Информация о рынке, возвращаемая `load_markets`.
#[derive(Debug, Clone)]
pub struct MarketInfo {
    /// Unified-символ: `"BTC/USDT"` для spot, `"BTC/USDT.P"` для perp.
    pub symbol: String,
    /// Базовая валюта: `"BTC"`.
    pub base: String,
    /// Котируемая валюта: `"USDT"`.
    pub quote: String,
    /// Активен ли рынок для торговли.
    pub active: bool,
    /// Тип рынка (spot/perp).
    pub market_type: crate::config::MarketType,
    /// Native-символ биржи: `"BTCUSDT"`.
    pub raw_symbol: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_scan_name_binance() {
        assert_eq!(
            Exchange::from_scan_name("binance_spot"),
            Some(Exchange::Binance)
        );
        assert_eq!(
            Exchange::from_scan_name("binance_perp"),
            Some(Exchange::Binance)
        );
    }

    #[test]
    fn from_scan_name_bybit() {
        // Регрессионный тест: ранее bybit ошибочно возвращал Binance
        assert_eq!(
            Exchange::from_scan_name("bybit_spot"),
            Some(Exchange::Bybit)
        );
        assert_eq!(
            Exchange::from_scan_name("bybit_perp"),
            Some(Exchange::Bybit)
        );
    }

    #[test]
    fn from_scan_name_all_exchanges() {
        assert_eq!(
            Exchange::from_scan_name("kucoin_spot"),
            Some(Exchange::Kucoin)
        );
        assert_eq!(
            Exchange::from_scan_name("bitget_perp"),
            Some(Exchange::Bitget)
        );
        assert_eq!(Exchange::from_scan_name("gate_spot"), Some(Exchange::Gate));
        assert_eq!(Exchange::from_scan_name("mexc_fut"), Some(Exchange::Mexc));
    }

    #[test]
    fn from_scan_name_case_insensitive() {
        assert_eq!(
            Exchange::from_scan_name("BYBIT_SPOT"),
            Some(Exchange::Bybit)
        );
        assert_eq!(
            Exchange::from_scan_name("Binance_Perp"),
            Some(Exchange::Binance)
        );
    }

    #[test]
    fn from_scan_name_unknown_returns_none() {
        assert!(Exchange::from_scan_name("unknown_exchange").is_none());
        assert!(Exchange::from_scan_name("").is_none());
    }

    #[test]
    fn as_str_returns_lowercase_name() {
        assert_eq!(Exchange::Binance.as_str(), "binance");
        assert_eq!(Exchange::Bybit.as_str(), "bybit");
        assert_eq!(Exchange::Kucoin.as_str(), "kucoin");
        assert_eq!(Exchange::Bitget.as_str(), "bitget");
        assert_eq!(Exchange::Gate.as_str(), "gate");
        assert_eq!(Exchange::Mexc.as_str(), "mexc");
    }

    #[test]
    fn display_matches_as_str() {
        assert_eq!(format!("{}", Exchange::Binance), "binance");
        assert_eq!(format!("{}", Exchange::Bybit), "bybit");
    }

    #[test]
    fn exchange_eq_and_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Exchange::Bybit);
        set.insert(Exchange::Bybit);
        set.insert(Exchange::Binance);
        assert_eq!(set.len(), 2);
    }
}
