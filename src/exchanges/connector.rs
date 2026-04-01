use crate::config::MarketType;
use crate::exchanges::normalized::{Exchange, MarketInfo, NormalizedTrade};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Factory function type for creating exchange connectors
pub type ConnectorFactory = fn(MarketType) -> Box<dyn ExchangeConnector>;

/// Trait that all exchange WebSocket connectors must implement.
#[async_trait]
pub trait ExchangeConnector: Send + Sync {
    /// The exchange this connector handles
    fn exchange(&self) -> Exchange;

    /// The market type (spot or perp)
    fn market_type(&self) -> MarketType;

    /// Load all available markets via REST API.
    /// Returns a list of market info that can be filtered by quote, type, etc.
    async fn load_markets(&self) -> anyhow::Result<Vec<MarketInfo>>;

    /// Connect to WebSocket and stream normalized trades.
    /// Sends trades through the broadcast channel.
    /// Runs until cancelled or an unrecoverable error occurs.
    async fn stream_trades(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>;

    /// Format a unified symbol (e.g., "BTC/USDT") into exchange-native format (e.g., "BTCUSDT")
    fn to_native_symbol(&self, unified: &str) -> String;

    /// Parse an exchange-native symbol back to unified format
    fn to_unified_symbol(&self, native: &str) -> Option<String>;

    /// Maximum args allowed in a single WebSocket subscribe message.
    /// Returns 0 if no known limit (effectively unlimited).
    fn max_subscribe_args(&self) -> usize;
}

/// Get a connector factory for a given exchange
pub fn get_connector_factory(exchange: Exchange) -> ConnectorFactory {
    match exchange {
        Exchange::Bybit => |mt| Box::new(crate::exchanges::bybit::BybitConnector::new(mt)),
        Exchange::Kucoin => |mt| Box::new(crate::exchanges::kucoin::KucoinConnector::new(mt)),
        Exchange::Bitget => |mt| Box::new(crate::exchanges::bitget::BitgetConnector::new(mt)),
        Exchange::Gate => |mt| Box::new(crate::exchanges::gate::GateConnector::new(mt)),
        Exchange::Mexc => |mt| Box::new(crate::exchanges::mexc::MexcConnector::new(mt)),
    }
}
