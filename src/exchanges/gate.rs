use crate::config::MarketType;
use crate::exchanges::connector::ExchangeConnector;
use crate::exchanges::normalized::{Exchange, MarketInfo, NormalizedTrade};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

pub struct GateConnector {
    market_type: MarketType,
    rest_base: String,
    ws_base: String,
    client: reqwest::Client,
}

impl GateConnector {
    pub fn new(market_type: MarketType) -> Self {
        let (ws_base, rest_base) = match market_type {
            MarketType::Spot => (
                "wss://api.gateio.ws/ws/v4/".to_string(),
                "https://api.gateio.ws/api/v4".to_string(),
            ),
            MarketType::Perp => (
                "wss://fx-ws.gateio.ws/v4/ws/usdt".to_string(),
                "https://api.gateio.ws/api/v4".to_string(),
            ),
        };
        Self {
            market_type,
            rest_base,
            ws_base,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    fn parse_gate_symbol(raw: &str) -> Option<(String, String)> {
        // У Gate формат тикеров - BTC_USDT
        let parts: Vec<&str> = raw.split('_').collect();
        if parts.len() >= 2 {
            Some((parts[0].to_string(), parts[1].to_string()))
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize)]
struct GateSpotMarket {
    id: String,
    #[allow(dead_code)]
    base: String,
    #[allow(dead_code)]
    quote: String,
    trade_status: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    market_type: String,
}

#[derive(Debug, Deserialize)]
struct GateFuturesMarket {
    name: String,
    status: String,
}

#[async_trait]
impl ExchangeConnector for GateConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Gate
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    async fn load_markets(&self) -> anyhow::Result<Vec<MarketInfo>> {
        let (url, _expected_type) = match self.market_type {
            MarketType::Spot => (format!("{}/spot/currency_pairs", self.rest_base), "spot"),
            MarketType::Perp => (format!("{}/futures/usdt/contracts", self.rest_base), "perp"),
        };

        let markets: Vec<MarketInfo> = match self.market_type {
            MarketType::Spot => {
                let resp: Vec<GateSpotMarket> = self.client.get(&url).send().await?.json().await?;
                resp.into_iter()
                    .filter_map(|m| {
                        if m.trade_status != "tradable" {
                            return None;
                        }
                        let (base, quote) = GateConnector::parse_gate_symbol(&m.id)?;
                        let unified = format!("{}/{}", base, quote);
                        Some(MarketInfo {
                            symbol: unified,
                            base,
                            quote,
                            active: true,
                            market_type: self.market_type,
                            raw_symbol: m.id,
                        })
                    })
                    .collect()
            }
            MarketType::Perp => {
                let resp: Vec<GateFuturesMarket> =
                    self.client.get(&url).send().await?.json().await?;
                resp.into_iter()
                    .filter_map(|m| {
                        if m.status != "trading" {
                            return None;
                        }
                        let (base, quote) = GateConnector::parse_gate_symbol(&m.name)?;
                        let unified = format!("{}/{}.P", base, quote);
                        Some(MarketInfo {
                            symbol: unified,
                            base,
                            quote,
                            active: true,
                            market_type: self.market_type,
                            raw_symbol: m.name,
                        })
                    })
                    .collect()
            }
        };

        info!("Gate {} loaded {} markets", self.market_type, markets.len());
        Ok(markets)
    }

    async fn stream_trades(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let gate_symbols: Vec<String> = symbols
            .iter()
            .filter_map(|s| {
                let without_settle = s.strip_suffix(".P").unwrap_or(s);
                let (base, quote) = without_settle.split_once('/')?;
                Some(format!("{}_{}", base, quote))
            })
            .collect();

        // Канал сделок Gate spot: spot.trades
        // Канал сделок Gate futures: futures.trades
        let channel = match self.market_type {
            MarketType::Spot => "spot.trades",
            MarketType::Perp => "futures.trades",
        };

        // Gate ожидает полезную нагрузку в виде массива тикеров.
        let subscribe_msg = serde_json::json!({
            "time": chrono::Utc::now().timestamp(),
            "channel": channel,
            "event": "subscribe",
            "payload": gate_symbols
        });

        let mut retry_delay = std::time::Duration::from_secs(1);
        let max_retry_delay = std::time::Duration::from_secs(30);

        loop {
            if cancel.is_cancelled() {
                break Ok(());
            }
            match self.connect_and_stream(&subscribe_msg, &tx, &cancel).await {
                Ok(()) => break Ok(()),
                Err(e) => {
                    if cancel.is_cancelled() {
                        break Ok(());
                    }
                    warn!("Gate WS error, retrying in {:?}: {}", retry_delay, e);
                    tokio::select! {
                        _ = tokio::time::sleep(retry_delay) => {},
                        _ = cancel.cancelled() => break Ok(()),
                    }
                    let jitter =
                        std::time::Duration::from_millis(crate::exchanges::rand_int() % 1000);
                    retry_delay = (retry_delay * 2 + jitter).min(max_retry_delay);
                }
            }
        }
    }

    fn to_native_symbol(&self, unified: &str) -> String {
        let without_settle = unified.strip_suffix(".P").unwrap_or(unified);
        if let Some((base, quote)) = without_settle.split_once('/') {
            format!("{}_{}", base, quote)
        } else {
            unified.to_string()
        }
    }

    fn to_unified_symbol(&self, native: &str) -> Option<String> {
        let (base, quote) = GateConnector::parse_gate_symbol(native)?;
        let suffix = if self.market_type == MarketType::Perp {
            ".P"
        } else {
            ""
        };
        Some(format!("{}/{}{}", base, quote, suffix))
    }

    fn max_subscribe_args(&self) -> usize {
        // Gate: нет известного лимита на количество аргументов в одном сообщении
        0
    }
}

impl GateConnector {
    async fn connect_and_stream(
        &self,
        subscribe_msg: &serde_json::Value,
        tx: &tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(&self.ws_base, None, true).await?;
        let (mut write, mut read) = ws_stream.split();

        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;

        // Gate требует периодический ping
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(30));

        let connected_since = Instant::now();
        const MAX_CONN_LIFETIME: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    write.send(Message::Ping(vec![].into())).await?;
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Some(trades) = Self::parse_trade_message(&text, self.market_type) {
                                for trade in trades {
                                    if tx.send(trade).is_err() {}
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) => anyhow::bail!("Gate WS closed"),
                        Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => anyhow::bail!("Gate WS error: {}", e),
                        None => anyhow::bail!("Gate WS ended"),
                    }
                }
                _ = cancel.cancelled() => break,
            }

            if connected_since.elapsed() > MAX_CONN_LIFETIME {
                info!("Gate WS connection lifetime reached, reconnecting...");
                anyhow::bail!("connection lifetime expired");
            }
        }
        Ok(())
    }

    fn parse_trade_message(text: &str, market_type: MarketType) -> Option<Vec<NormalizedTrade>> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;

        // Скипаем не нужные сообщения (subscription confirmations, etc)
        let event = v.get("event").and_then(|e| e.as_str()).unwrap_or("");
        if event != "update" {
            return None;
        }

        let channel = v.get("channel")?.as_str()?;
        if !channel.contains("trades") {
            return None;
        }

        match market_type {
            MarketType::Spot => {
                // Spot: результат - единственный объект
                // {"result":{"currency_pair":"BTC_USDT","amount":"0.028","price":"68178.5","create_time_ms":"1774993737638.792000"}}
                let result = v.get("result")?;
                let symbol = result.get("currency_pair")?.as_str()?.to_string();
                let (base, quote) = Self::parse_gate_symbol(&symbol)?;
                let unified = format!("{}/{}", base, quote);

                let price: f64 = result.get("price")?.as_str()?.parse().ok()?;
                let size: f64 = result.get("amount")?.as_str()?.parse().ok()?;
                let create_time_ms: f64 = result
                    .get("create_time_ms")
                    .and_then(|t| t.as_str().and_then(|s| s.parse().ok()))
                    .or_else(|| result.get("create_time_ms").and_then(|t| t.as_f64()))
                    .unwrap_or(0.0);

                Some(vec![NormalizedTrade {
                    symbol: unified,
                    timestamp_ms: create_time_ms as i64,
                    price,
                    cost: price * size,
                    exchange: Exchange::Gate,
                }])
            }
            MarketType::Perp => {
                // Futures: результат - массив объектов
                // {"result":[{"id":...,"size":-67,"create_time_ms":1775000128579,"price":"68244.1","contract":"BTC_USDT"}]}
                let results = v.get("result")?.as_array()?;
                let mut trades = Vec::with_capacity(results.len());

                for item in results {
                    let symbol = item.get("contract")?.as_str()?.to_string();
                    let (base, quote) = Self::parse_gate_symbol(&symbol)?;
                    let unified = format!("{}/{}.P", base, quote);

                    let price: f64 = item.get("price")?.as_str()?.parse().ok()?;
                    // размер может быть отрицательным (продажа) - для расчета стоимости берём значение по модулю
                    let size: f64 = item
                        .get("size")
                        .and_then(|s| s.as_i64())
                        .or_else(|| {
                            item.get("size")
                                .and_then(|s| s.as_str().and_then(|v| v.parse().ok()))
                        })
                        .map(|s| s.abs() as f64)
                        .unwrap_or(0.0);
                    let timestamp_ms: i64 = item
                        .get("create_time_ms")
                        .and_then(|t| t.as_i64())
                        .or_else(|| {
                            item.get("create_time_ms")
                                .and_then(|t| t.as_str().and_then(|s| s.parse().ok()))
                        })
                        .unwrap_or(0);

                    trades.push(NormalizedTrade {
                        symbol: unified,
                        timestamp_ms,
                        price,
                        cost: price * size,
                        exchange: Exchange::Gate,
                    });
                }

                if trades.is_empty() {
                    None
                } else {
                    Some(trades)
                }
            }
        }
    }
}
