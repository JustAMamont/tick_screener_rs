use crate::config::MarketType;
use crate::exchanges::connector::ExchangeConnector;
use crate::exchanges::normalized::{Exchange, MarketInfo, NormalizedTrade};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use std::time::Instant;
use tracing::{debug, error, info, warn};

pub struct BybitConnector {
    market_type: MarketType,
    ws_base: String,
    rest_base: String,
    client: reqwest::Client,
}

impl BybitConnector {
    pub fn new(market_type: MarketType) -> Self {
        let (ws_base, rest_base) = match market_type {
            MarketType::Spot => (
                "wss://stream.bybit.com/v5/public/spot".to_string(),
                "https://api.bybit.com".to_string(),
            ),
            MarketType::Perp => (
                "wss://stream.bybit.com/v5/public/linear".to_string(),
                "https://api.bybit.com".to_string(),
            ),
        };

        Self {
            market_type,
            ws_base,
            rest_base,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    fn format_bybit_symbol(base: &str, quote: &str) -> String {
        format!("{}{}", base, quote)
    }

    fn parse_bybit_symbol(raw: &str) -> Option<(String, String)> {
        // Bybit uses BTCUSDT format
        // Try common quote currencies
        let quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH", "FDUSD"];
        for q in quotes {
            if let Some(base) = raw.strip_suffix(q) {
                if !base.is_empty() {
                    return Some((base.to_string(), q.to_string()));
                }
            }
        }
        None
    }
}

#[derive(Debug, Deserialize)]
struct BybitMarketsResponse {
    result: BybitMarketsResult,
}

#[derive(Debug, Deserialize)]
struct BybitMarketsResult {
    #[allow(dead_code)]
    category: String,
    list: Vec<BybitMarket>,
}

#[derive(Debug, Deserialize)]
struct BybitMarket {
    symbol: String,
    #[allow(dead_code)]
    #[serde(rename = "baseCoin")]
    base_coin: String,
    #[allow(dead_code)]
    #[serde(rename = "quoteCoin")]
    quote_coin: String,
    status: String,
}

#[async_trait]
impl ExchangeConnector for BybitConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Bybit
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    async fn load_markets(&self) -> anyhow::Result<Vec<MarketInfo>> {
        let category = match self.market_type {
            MarketType::Spot => "spot",
            MarketType::Perp => "linear",
        };

        let url = format!(
            "{}/v5/market/instruments-info?category={}&limit=1000",
            self.rest_base, category
        );

        let resp_text = self.client.get(&url).send().await?.text().await?;
        let resp: BybitMarketsResponse = match serde_json::from_str(&resp_text) {
            Ok(r) => r,
            Err(e) => {
                error!("Bybit API response parse error: {}", e);
                error!("Raw response (first 500 chars): {}", &resp_text[..resp_text.len().min(500)]);
                anyhow::bail!("Failed to parse Bybit response: {}", e);
            }
        };

        let markets: Vec<MarketInfo> = resp
            .result
            .list
            .into_iter()
            .filter_map(|m| {
                if m.status != "Trading" {
                    return None;
                }
                let (base, quote) = BybitConnector::parse_bybit_symbol(&m.symbol)?;
                let unified = format!("{}/{}", base, quote);
                let unified = if self.market_type == MarketType::Perp {
                    format!("{}:{}", unified, quote)
                } else {
                    unified
                };

                Some(MarketInfo {
                    symbol: unified,
                    base,
                    quote,
                    active: true,
                    market_type: self.market_type,
                    raw_symbol: m.symbol,
                })
            })
            .collect();

        info!("Bybit {} loaded {} markets", self.market_type, markets.len());
        Ok(markets)
    }

    async fn stream_trades(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let bybit_symbols: Vec<String> = symbols
            .iter()
            .filter_map(|s| {
                let without_settle = s.split(':').next()?;
                let (base, quote) = without_settle.split_once('/')?;
                Some(BybitConnector::format_bybit_symbol(base, quote))
            })
            .collect();

        let args: Vec<String> = bybit_symbols
            .iter()
            .map(|s| format!("publicTrade.{}", s))
            .collect();

        let mut retry_delay = std::time::Duration::from_secs(1);
        let max_retry_delay = std::time::Duration::from_secs(30);

        loop {
            if cancel.is_cancelled() {
                break Ok(());
            }

            match Self::ws_loop(&self.ws_base, &args, &self.market_type, &tx, &cancel).await {
                Ok(()) => break Ok(()),
                Err(e) => {
                    if cancel.is_cancelled() {
                        break Ok(());
                    }
                    warn!("Bybit WS error, retrying in {:?}: {}", retry_delay, e);
                    tokio::select! {
                        _ = tokio::time::sleep(retry_delay) => {},
                        _ = cancel.cancelled() => break Ok(()),
                    }
                    let jitter = std::time::Duration::from_millis(crate::exchanges::rand_int() % 1000);
                    retry_delay = (retry_delay * 2 + jitter).min(max_retry_delay);
                }
            }
        }
    }

    fn to_native_symbol(&self, unified: &str) -> String {
        let without_settle = unified.split(':').next().unwrap_or(unified);
        if let Some((base, quote)) = without_settle.split_once('/') {
            BybitConnector::format_bybit_symbol(base, quote)
        } else {
            unified.to_string()
        }
    }

    fn to_unified_symbol(&self, native: &str) -> Option<String> {
        let (base, quote) = BybitConnector::parse_bybit_symbol(native)?;
        let unified = format!("{}/{}", base, quote);
        let unified = if self.market_type == MarketType::Perp {
            format!("{}:{}", unified, quote)
        } else {
            unified
        };
        Some(unified)
    }

    fn max_subscribe_args(&self) -> usize {
        // Spot: max 10 args per subscribe message (docs)
        // Linear/Inverse (Perp): no limit
        match self.market_type {
            MarketType::Spot => 10,
            MarketType::Perp => 0,
        }
    }
}

impl BybitConnector {
    /// Single WS connection loop with ping heartbeat (20s), subscribe, and 24h reconnect.
    async fn ws_loop(
        ws_base: &str,
        args: &[String],
        market_type: &MarketType,
        tx: &tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let (ws_stream, _) = tokio_tungstenite::connect_async_with_config(ws_base, None, true).await?;
        let (mut write, mut read) = ws_stream.split();

        // Bybit Spot: max 10 args per subscribe request (futures/perp: no limit)
        let chunk_size = if *market_type == MarketType::Spot { 10 } else { args.len().max(1) };
        for chunk in args.chunks(chunk_size) {
            let subscribe_msg = serde_json::json!({
                "op": "subscribe",
                "args": chunk
            });
            write.send(Message::Text(subscribe_msg.to_string().into())).await?;
        }
        info!(
            "Bybit {} WS subscribed: {} topics (chunk={})",
            market_type, args.len(), chunk_size
        );

        // Bybit requires ping every 20s or connection gets closed
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(20));
        let connected_since = Instant::now();
        const MAX_CONN_LIFETIME: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if write.send(Message::Ping(vec![].into())).await.is_err() {
                        anyhow::bail!("Failed to send ping");
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Some(trades) = Self::parse_trade_message(&text) {
                                for trade in trades {
                                    if tx.send(trade).is_err() {
                                        debug!("Trade broadcast failed (no receivers)");
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                        Some(Ok(Message::Close(_))) => {
                            warn!("Bybit WS closed by server");
                            anyhow::bail!("WS closed by server");
                        }
                        Some(Err(e)) => anyhow::bail!("Bybit WS error: {}", e),
                        None => anyhow::bail!("Bybit WS stream ended"),
                    }
                }
                _ = cancel.cancelled() => break,
            }

            if connected_since.elapsed() > MAX_CONN_LIFETIME {
                info!("Bybit WS connection lifetime reached, reconnecting...");
                anyhow::bail!("connection lifetime expired");
            }
        }

        Ok(())
    }

    fn parse_trade_message(text: &str) -> Option<Vec<NormalizedTrade>> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;

        // Subscription confirmation
        if v.get("op").and_then(|o| o.as_str()) == Some("subscribe") {
            debug!("Bybit WS subscription confirmed");
            return None;
        }

        // Only process trade data topics
        let topic = v.get("topic").and_then(|t| t.as_str()).unwrap_or("");
        if !topic.starts_with("publicTrade.") {
            return None;
        }

        // Trade data: {"topic":"publicTrade.BTCUSDT","type":"delta","data":[{"T":"...","s":"BTCUSDT","p":"...","v":"..."}]}
        let data = v.get("data")?.as_array()?;
        let mut trades = Vec::with_capacity(data.len());

        for item in data {
            // Fields are directly on each item, NOT nested under a "d" key
            let symbol = item.get("s")?.as_str()?.to_string();

            // Parse the raw Bybit symbol to unified
            let (base, quote) = Self::parse_bybit_symbol(&symbol)?;
            let unified = format!("{}/{}", base, quote);

            let price: f64 = item.get("p")?.as_str()?.parse().ok()?;
            let size: f64 = item.get("v")?.as_str()?.parse().ok()?;

            // Bybit sends timestamp as a string, e.g. "T": "1670608600000"
            let timestamp: i64 = item.get("T")
                .and_then(|t| t.as_str().and_then(|s| s.parse().ok()))
                .or_else(|| item.get("T").and_then(|t| t.as_i64()))?;

            trades.push(NormalizedTrade {
                symbol: unified,
                timestamp_ms: timestamp,
                price,
                cost: price * size,
                exchange: Exchange::Bybit,
            });
        }

        if trades.is_empty() {
            None
        } else {
            Some(trades)
        }
    }
}
