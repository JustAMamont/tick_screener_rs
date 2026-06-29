use crate::config::MarketType;
use crate::exchanges::connector::ExchangeConnector;
use crate::exchanges::normalized::{Exchange, MarketInfo, NormalizedTrade};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

pub struct BinanceConnector {
    market_type: MarketType,
    rest_base: String,
    ws_base: String,
    client: reqwest::Client,
}

impl BinanceConnector {
    pub fn new(market_type: MarketType) -> Self {
        let (ws_base, rest_base) = match market_type {
            MarketType::Spot => (
                "wss://stream.binance.com:9443/stream".to_string(),
                "https://api.binance.com".to_string(),
            ),
            MarketType::Perp => (
                "wss://fstream.binance.com/stream".to_string(),
                "https://fapi.binance.com".to_string(),
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

    fn parse_binance_symbol(raw: &str) -> Option<(String, String)> {
        let quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH", "FDUSD", "TUSD"];
        for q in quotes {
            if let Some(base) = raw.strip_suffix(q)
                && !base.is_empty()
            {
                return Some((base.to_string(), q.to_string()));
            }
        }
        None
    }
}

// Структуры REST API биржи
#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbol>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BinanceSymbol {
    symbol: String,
    base_asset: String,
    quote_asset: String,
    status: String,
}

#[async_trait]
impl ExchangeConnector for BinanceConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Binance
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    async fn load_markets(&self) -> anyhow::Result<Vec<MarketInfo>> {
        let url = match self.market_type {
            MarketType::Spot => format!("{}/api/v3/exchangeInfo", self.rest_base),
            MarketType::Perp => format!("{}/fapi/v1/exchangeInfo", self.rest_base),
        };

        let resp: BinanceExchangeInfo = self.client.get(&url).send().await?.json().await?;

        let markets: Vec<MarketInfo> = resp
            .symbols
            .into_iter()
            .filter_map(|m| {
                if m.status != "TRADING" {
                    return None;
                }

                let unified = match self.market_type {
                    MarketType::Spot => format!("{}/{}", m.base_asset, m.quote_asset),
                    MarketType::Perp => {
                        format!("{}/{}.P", m.base_asset, m.quote_asset)
                    }
                };

                Some(MarketInfo {
                    symbol: unified,
                    base: m.base_asset,
                    quote: m.quote_asset,
                    active: true,
                    market_type: self.market_type,
                    raw_symbol: m.symbol,
                })
            })
            .collect();

        info!(
            "Binance {} loaded {} markets",
            self.market_type,
            markets.len()
        );
        Ok(markets)
    }

    async fn stream_trades(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        // Binance WS ожидает символы в нижнем регистре для имён стримов
        let binance_streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@aggTrade", self.to_native_symbol(s).to_lowercase()))
            .collect();

        let mut retry_delay = std::time::Duration::from_secs(1);
        let max_retry_delay = std::time::Duration::from_secs(30);

        loop {
            if cancel.is_cancelled() {
                break Ok(());
            }

            match self
                .connect_and_stream(&binance_streams, &tx, &cancel)
                .await
            {
                Ok(()) => break Ok(()),
                Err(e) => {
                    if cancel.is_cancelled() {
                        break Ok(());
                    }
                    warn!("Binance WS error, retrying in {:?}: {}", retry_delay, e);
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
        // Убираем .P-суффикс (если есть) перед разбором.
        let without_settle = unified.strip_suffix(".P").unwrap_or(unified);
        if let Some((base, quote)) = without_settle.split_once('/') {
            format!("{}{}", base, quote)
        } else {
            unified.to_string()
        }
    }

    fn to_unified_symbol(&self, native: &str) -> Option<String> {
        let native_upper = native.to_uppercase();
        let (base, quote) = BinanceConnector::parse_binance_symbol(&native_upper)?;
        let suffix = if self.market_type == MarketType::Perp {
            ".P"
        } else {
            ""
        };
        Some(format!("{}/{}{}", base, quote, suffix))
    }

    fn max_subscribe_args(&self) -> usize {
        // Binance допускает до 200 айтемов в одном SUBSCRIBE сообщении
        200
    }
}

impl BinanceConnector {
    async fn connect_and_stream(
        &self,
        streams: &[String],
        tx: &tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(&self.ws_base, None, true).await?;
        let (mut write, mut read) = ws_stream.split();

        // Подписываемся чанками, так как лимит 200 на одно сообщение
        for (i, chunk) in streams.chunks(self.max_subscribe_args()).enumerate() {
            let subscribe_msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": chunk,
                "id": i + 1
            });
            write
                .send(Message::Text(subscribe_msg.to_string().into()))
                .await?;
        }

        info!(
            "Binance {} WS subscribed to {} streams",
            self.market_type,
            streams.len()
        );

        // Binance рекомендует слать ping раз в 3-5 минут для поддержки соединения
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(180));
        let connected_since = Instant::now();
        const MAX_CONN_LIFETIME: std::time::Duration = std::time::Duration::from_secs(24 * 3600); // 24 часа

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    // Tungstenite автоматически отвечает на входящие PING, но мы можем отправлять PONG
                    // или пустой кадр PING сами, чтобы избежать отключения из-за неактивности
                    if write.send(Message::Ping(vec![].into())).await.is_err() {
                        anyhow::bail!("Failed to send ping");
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Some(trade) = Self::parse_agg_trade(&text, self.market_type)
                                && tx.send(trade).is_err() {
                                    // Каналы могут не иметь подписчиков временно, это норм
                                }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) => anyhow::bail!("Binance WS closed by server"),
                        Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => anyhow::bail!("Binance WS error: {}", e),
                        None => anyhow::bail!("Binance WS stream ended"),
                    }
                }
                _ = cancel.cancelled() => break,
            }

            // Binance сбрасывает соединение каждые 24 часа. Мы превентивно переподключаемся
            if connected_since.elapsed() > MAX_CONN_LIFETIME {
                info!("Binance WS connection lifetime reached, reconnecting...");
                anyhow::bail!("connection lifetime expired");
            }
        }

        Ok(())
    }

    fn parse_agg_trade(text: &str, market_type: MarketType) -> Option<NormalizedTrade> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;

        // Мы используем /stream endpoint, поэтому структура: {"stream": "...", "data": {...}}
        let data = v.get("data")?;

        let event_type = data.get("e")?.as_str()?;
        if event_type != "aggTrade" {
            return None;
        }

        let symbol = data.get("s")?.as_str()?; // Пример: "BTCUSDT"

        // Переводим в унифицированный формат
        let (base, quote) = Self::parse_binance_symbol(symbol)?;
        let unified = if market_type == MarketType::Perp {
            format!("{}/{}.P", base, quote)
        } else {
            format!("{}/{}", base, quote)
        };

        // Binance шлет числа строками
        let price: f64 = data.get("p")?.as_str()?.parse().ok()?;
        let qty: f64 = data.get("q")?.as_str()?.parse().ok()?;
        let timestamp_ms: i64 = data.get("T")?.as_i64()?;

        Some(NormalizedTrade {
            symbol: unified,
            timestamp_ms,
            price,
            cost: price * qty,
            exchange: Exchange::Binance,
        })
    }
}
