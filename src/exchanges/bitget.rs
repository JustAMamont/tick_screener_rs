use crate::config::MarketType;
use crate::exchanges::connector::ExchangeConnector;
use crate::exchanges::normalized::{Exchange, MarketInfo, NormalizedTrade};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;
use std::time::Instant;
use tracing::{info, warn};

pub struct BitgetConnector {
    market_type: MarketType,
    rest_base: String,
    ws_base: String,
    client: reqwest::Client,
}

impl BitgetConnector {
    pub fn new(market_type: MarketType) -> Self {
        let (ws_base, rest_base) = match market_type {
            MarketType::Spot => (
                "wss://ws.bitget.com/v2/ws/public".to_string(),
                "https://api.bitget.com".to_string(),
            ),
            MarketType::Perp => (
                "wss://ws.bitget.com/v2/ws/public".to_string(),
                "https://api.bitget.com".to_string(),
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

    fn parse_bitget_symbol(raw: &str) -> Option<(String, String)> {
        // Bitget uses BTCUSDT or BTCUSDT_SPT (spot) / BTCUSDT_UMCBL (perp)
        let clean = raw.split('_').next().unwrap_or(raw);
        let quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH"];
        for q in quotes {
            if let Some(base) = clean.strip_suffix(q) {
                if !base.is_empty() {
                    return Some((base.to_string(), q.to_string()));
                }
            }
        }
        None
    }
}

#[derive(Debug, Deserialize)]
struct BitgetMarketsResponse {
    data: Vec<BitgetMarket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BitgetMarket {
    symbol: String,
    #[allow(dead_code)]
    base_coin: String,
    #[allow(dead_code)]
    quote_coin: String,
    status: String,
}

#[async_trait]
impl ExchangeConnector for BitgetConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Bitget
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    async fn load_markets(&self) -> anyhow::Result<Vec<MarketInfo>> {
        let product = match self.market_type {
            MarketType::Spot => "SPOT",
            MarketType::Perp => "USDT-FUTURES",
        };

        let url = format!(
            "{}/api/v2/spot/public/symbols?productType={}",
            self.rest_base, product
        );

        let resp: BitgetMarketsResponse = self.client.get(&url).send().await?.json().await?;

        let markets: Vec<MarketInfo> = resp
            .data
            .into_iter()
            .filter_map(|m| {
                if m.status != "online" {
                    return None;
                }
                let (base, quote) = BitgetConnector::parse_bitget_symbol(&m.symbol)?;
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

        info!("Bitget {} loaded {} markets", self.market_type, markets.len());
        Ok(markets)
    }

    async fn stream_trades(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let inst_type = match self.market_type {
            MarketType::Spot => "SPOT",
            MarketType::Perp => "USDT-FUTURES",
        };

        let args: Vec<serde_json::Value> = symbols
            .iter()
            .filter_map(|s| {
                let without_settle = s.split(':').next()?;
                let (base, quote) = without_settle.split_once('/')?;
                Some(serde_json::json!({
                    "instType": inst_type,
                    "channel": "trade",
                    "instId": format!("{}{}", base, quote)
                }))
            })
            .collect();

        let mut retry_delay = std::time::Duration::from_secs(1);
        let max_retry_delay = std::time::Duration::from_secs(30);

        loop {
            if cancel.is_cancelled() {
                break Ok(());
            }

            match self.connect_and_stream(&args, &tx, &cancel).await {
                Ok(()) => break Ok(()),
                Err(e) => {
                    if cancel.is_cancelled() {
                        break Ok(());
                    }
                    warn!("Bitget WS error, retrying in {:?}: {}", retry_delay, e);
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
            format!("{}{}", base, quote)
        } else {
            unified.to_string()
        }
    }

    fn to_unified_symbol(&self, native: &str) -> Option<String> {
        let (base, quote) = BitgetConnector::parse_bitget_symbol(native)?;
        let unified = format!("{}/{}", base, quote);
        let unified = if self.market_type == MarketType::Perp {
            format!("{}:{}", unified, quote)
        } else {
            unified
        };
        Some(unified)
    }

    fn max_subscribe_args(&self) -> usize {
        match self.market_type {
            MarketType::Spot => 100,
            MarketType::Perp => 200,
        }
    }
}

impl BitgetConnector {
    async fn connect_and_stream(
        &self,
        args: &[serde_json::Value],
        tx: &tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let (ws_stream, _) = tokio_tungstenite::connect_async_with_config(&self.ws_base, None, true).await?;
        let (mut write, mut read) = ws_stream.split();

        // Bitget: total payload per subscribe cannot exceed 4096 bytes.
        // Chunk args into smaller batches and send multiple subscribe messages.
        const MAX_ARGS_PER_MSG: usize = 50; // 4096 / ~70 bytes per arg ≈ 58, use 50 for safety
        for chunk in args.chunks(MAX_ARGS_PER_MSG) {
            let subscribe_msg = serde_json::json!({
                "op": "subscribe",
                "args": chunk
            });
            write.send(Message::Text(subscribe_msg.to_string().into())).await?;
        }
        info!(
            "Bitget {} WS subscribed: {} topics",
            self.market_type, args.len()
        );

        // Bitget requires pong on "ping" text messages; also send proactive "ping" every 20s
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(20));

        let connected_since = Instant::now();
        const MAX_CONN_LIFETIME: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if write.send(Message::Text("ping".into())).await.is_err() {
                        anyhow::bail!("Failed to send ping");
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if text == "ping" {
                                let _ = write.send(Message::Text("pong".into())).await;
                                continue;
                            }
                            if let Some(trades) = Self::parse_trade_message(&text) {
                                for trade in trades {
                                    if tx.send(trade).is_err() {}
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            anyhow::bail!("Bitget WS closed");
                        }
                        Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => anyhow::bail!("Bitget WS error: {}", e),
                        None => anyhow::bail!("Bitget WS ended"),
                    }
                }
                _ = cancel.cancelled() => break,
            }

            if connected_since.elapsed() > MAX_CONN_LIFETIME {
                info!("Bitget WS connection lifetime reached, reconnecting...");
                anyhow::bail!("connection lifetime expired");
            }
        }
        Ok(())
    }

    fn parse_trade_message(text: &str) -> Option<Vec<NormalizedTrade>> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;

        if v.get("event").is_some() {
            return None; // Subscription confirmation
        }

        let data = v.get("data").and_then(|d| d.as_array())?;
        let arg = v.get("arg")?;
        let inst_id = arg.get("instId")?.as_str()?;

        let (base, quote) = Self::parse_bitget_symbol(inst_id)?;
        let unified = format!("{}/{}", base, quote);

        let mut trades = Vec::with_capacity(data.len());
        for item in data {
            // Bitget V2: "price"/"size"/"ts" (all strings)
            let price: f64 = item.get("price")?.as_str()?.parse().ok()?;
            let size: f64 = item.get("size")?.as_str()?.parse().ok()?;
            // Bitget sends ts as a string, e.g. "ts": "1670608600000"
            let ts: i64 = item.get("ts")
                .and_then(|t| t.as_str().and_then(|s| s.parse().ok()))
                .or_else(|| item.get("ts").and_then(|t| t.as_i64()))?;

            trades.push(NormalizedTrade {
                symbol: unified.clone(),
                timestamp_ms: ts,
                price,
                cost: price * size,
                exchange: Exchange::Bitget,
            });
        }

        if trades.is_empty() { None } else { Some(trades) }
    }
}
