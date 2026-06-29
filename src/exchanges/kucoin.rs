use crate::config::MarketType;
use crate::exchanges::connector::ExchangeConnector;
use crate::exchanges::normalized::{Exchange, MarketInfo, NormalizedTrade};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

// У Kucoin разные форматы символов для spot (например "BTC-USDT") и perp (например "XBTUSDTM"),
// поэтому используем два отдельных кэша для spot и perp, чтобы они не перезаписывали друг друга.
static RAW_TO_UNIFIED: LazyLock<parking_lot::RwLock<HashMap<String, String>>> =
    LazyLock::new(|| parking_lot::RwLock::new(HashMap::new()));
// unified symbol -> raw_symbol (для WS-подписок)
static UNIFIED_TO_RAW: LazyLock<parking_lot::RwLock<HashMap<String, String>>> =
    LazyLock::new(|| parking_lot::RwLock::new(HashMap::new()));

pub struct KucoinConnector {
    market_type: MarketType,
    rest_base: String,
    #[allow(dead_code)]
    ws_public_url: String,
    #[allow(dead_code)]
    ws_private_url: String,
    client: reqwest::Client,
}

impl KucoinConnector {
    pub fn new(market_type: MarketType) -> Self {
        Self {
            market_type,
            rest_base: "https://api.kucoin.com".to_string(),
            ws_public_url: "wss://ws-api-spot.kucoin.com".to_string(),
            ws_private_url: "wss://ws-api-futures.kucoin.com".to_string(),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    async fn get_ws_token(&self, is_futures: bool) -> anyhow::Result<(String, String)> {
        let url = if is_futures {
            format!("{}/api/v1/bullet-public", "https://api-futures.kucoin.com")
        } else {
            format!("{}/api/v1/bullet-public", self.rest_base)
        };

        let resp_text = self.client.post(&url).send().await?.text().await?;
        let resp: KucoinBulletResponse = serde_json::from_str(&resp_text).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse bullet-public response ({}): {}",
                e,
                &resp_text[..resp_text.len().min(300)]
            )
        })?;
        let data = resp.data;
        Ok((data.instance_servers[0].endpoint.clone(), data.token))
    }

    /// Получение WS-токена с защитой от превышения лимита запросов (макс. 1 запрос каждые 2 секунды). 
    /// Возвращает кэшированный токен, если он был получен недавно. 
    ///
    /// FIX: Раздельные кэши для спота и фьючерсов, чтобы они не перезаписывали друг друга. 
    /// Используется parking_lot::Mutex (синхронный, без .await), поэтому блокировка не удерживается в точках ожидания (await points). 
    /// Блокировка снимается ПЕРЕД приостановкой выполнения (sleep), чтобы не блокировать другие задачи.
    async fn get_ws_token_rate_limited(
        &self,
        is_futures: bool,
    ) -> anyhow::Result<(String, String)> {
        // Раздельные кэши - тикеры спотового и фьючерсного рынков, они разного формата,
        // как уже упоминалось выше.
        static SPOT_CACHE: parking_lot::Mutex<Option<(String, String, Instant)>> =
            parking_lot::Mutex::new(None);
        static FUT_CACHE: parking_lot::Mutex<Option<(String, String, Instant)>> =
            parking_lot::Mutex::new(None);

        let cache: &parking_lot::Mutex<Option<(String, String, Instant)>> =
            if is_futures { &FUT_CACHE } else { &SPOT_CACHE };

        // 1. Проверка кэша (быстро, parking_lot - await не требуется)
        {
            let guard = cache.lock();
            if let Some((url, token, fetched_at)) = guard.as_ref()
                && fetched_at.elapsed() < std::time::Duration::from_secs(25)
            {
                return Ok((url.clone(), token.clone()));
            }
        }

        // 2. Ограничение частоты (rate limit): вычислить длительность паузы, ОСВОБОДИТЬ блокировку, затем приостановить выполнение
        // Это критически важно: старый код удерживал tokio::Mutex во время паузы,
        // что блокировало ВСЕ остальные задачи, ожидающие получения токена.
        {
            let sleep_dur = {
                let guard = cache.lock();
                if let Some((_, _, fetched_at)) = guard.as_ref() {
                    let elapsed = fetched_at.elapsed();
                    if elapsed < std::time::Duration::from_secs(2) {
                        std::time::Duration::from_secs(2) - elapsed
                    } else {
                        std::time::Duration::ZERO
                    }
                } else {
                    std::time::Duration::ZERO
                }
            }; // Снимаю блокировку - теперь другие задачи могут читать из кэша или записывать в него.

            if !sleep_dur.is_zero() {
                tokio::time::sleep(sleep_dur).await;
            }
        }

        // 3. Получить новый токен из API KuCoin
        let result = self.get_ws_token(is_futures).await?;

        // 4. Обновляем кэш
        {
            let mut guard = cache.lock();
            *guard = Some((result.0.clone(), result.1.clone(), Instant::now()));
        }

        Ok(result)
    }

    /// Резервный парсер для спотовых символов, таких как «BTC-USDT» -> («BTC», «USDT»)
    #[allow(dead_code)]
    fn parse_kucoin_symbol(raw: &str) -> Option<(String, String)> {
        let quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH", "FDUSD"];
        for q in quotes {
            if let Some(base) = raw.strip_suffix(&format!("-{}", q))
                && !base.is_empty()
            {
                return Some((base.to_string(), q.to_string()));
            }
        }
        // Пробуем без дефиса: BTCUSDT
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

#[derive(Debug, Deserialize)]
struct KucoinBulletResponse {
    data: KucoinBulletData,
}

#[derive(Debug, Deserialize)]
struct KucoinBulletData {
    token: String,
    #[serde(rename = "instanceServers")]
    instance_servers: Vec<KucoinServer>,
}

#[derive(Debug, Deserialize)]
struct KucoinServer {
    endpoint: String,
}

#[derive(Debug, Deserialize)]
struct KucoinMarketsResponse {
    data: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct KucoinSpotMarket {
    symbol: String,
    #[serde(rename = "baseCurrency")]
    base_currency: String,
    #[serde(rename = "quoteCurrency")]
    quote_currency: String,
    #[serde(rename = "enableTrading")]
    enable_trading: bool,
}

#[derive(Debug, Deserialize)]
struct KucoinFuturesMarket {
    symbol: String,
    #[serde(rename = "baseCurrency")]
    base_currency: String,
    #[serde(rename = "quoteCurrency")]
    quote_currency: String,
    status: String,
}

#[async_trait]
impl ExchangeConnector for KucoinConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Kucoin
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    async fn load_markets(&self) -> anyhow::Result<Vec<MarketInfo>> {
        let (url, _expected_type) = match self.market_type {
            MarketType::Spot => (format!("{}/api/v1/symbols", self.rest_base), "TRADE"),
            MarketType::Perp => (
                "https://api-futures.kucoin.com/api/v1/contracts/active".to_string(),
                "FUTURES",
            ),
        };

        let resp_text = self.client.get(&url).send().await?.text().await?;
        let markets: Vec<MarketInfo> = match self.market_type {
            MarketType::Spot => {
                let resp: KucoinMarketsResponse = serde_json::from_str(&resp_text)?;
                let spot_markets: Vec<KucoinSpotMarket> = resp
                    .data
                    .into_iter()
                    .filter_map(|v| serde_json::from_value(v).ok())
                    .collect();
                spot_markets
                    .into_iter()
                    .filter_map(|m| {
                        if !m.enable_trading {
                            return None;
                        }
                        let (base, quote) = KucoinConnector::parse_kucoin_symbol(&m.symbol)?;
                        let unified = format!("{}/{}", base, quote);
                        Some(MarketInfo {
                            symbol: unified.clone(),
                            base,
                            quote,
                            active: true,
                            market_type: self.market_type,
                            raw_symbol: m.symbol,
                        })
                    })
                    .collect()
            }
            MarketType::Perp => {
                let resp: KucoinMarketsResponse = serde_json::from_str(&resp_text)?;
                let futures_markets: Vec<KucoinFuturesMarket> = resp
                    .data
                    .into_iter()
                    .filter_map(|v| serde_json::from_value(v).ok())
                    .collect();
                futures_markets
                    .into_iter()
                    .filter_map(|m| {
                        if m.status != "Open" {
                            return None;
                        }
                        let base = m.base_currency;
                        let quote = m.quote_currency;
                        let unified = format!("{}/{}.P", base, quote);
                        Some(MarketInfo {
                            symbol: unified.clone(),
                            base,
                            quote,
                            active: true,
                            market_type: self.market_type,
                            raw_symbol: m.symbol,
                        })
                    })
                    .collect()
            }
        };

        // Заполнить таблицы соответствия символов для подписок WS и разбора данных о трейдах.
        {
            let mut r2u = RAW_TO_UNIFIED.write();
            let mut u2r = UNIFIED_TO_RAW.write();
            for m in &markets {
                r2u.insert(m.raw_symbol.clone(), m.symbol.clone());
                u2r.insert(m.symbol.clone(), m.raw_symbol.clone());
            }
        }

        info!(
            "Kucoin {} loaded {} markets",
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
        let is_futures = self.market_type == MarketType::Perp;
        let (ws_url, ws_token) = self.get_ws_token_rate_limited(is_futures).await?;
        let mut full_url = format!("{}?token={}", ws_url, ws_token);

        // Ищем тикеры Kuсoin, соответствующие унифицированным тикерам, с помощью таблицы соответствия.
        let kucoin_symbols: Vec<String> = {
            let u2r = UNIFIED_TO_RAW.read();
            symbols.iter().filter_map(|s| u2r.get(s).cloned()).collect()
        };

        if kucoin_symbols.is_empty() {
            warn!(
                "Kucoin {}: no native symbols found for {} unified symbols",
                self.market_type,
                symbols.len()
            );
            return Ok(());
        }

        let mut retry_delay = std::time::Duration::from_secs(1);
        let max_retry_delay = std::time::Duration::from_secs(30);

        loop {
            if cancel.is_cancelled() {
                break Ok(());
            }

            match self
                .connect_and_stream(&full_url, &kucoin_symbols, &tx, &cancel)
                .await
            {
                Ok(()) => break Ok(()),
                Err(e) => {
                    if cancel.is_cancelled() {
                        break Ok(());
                    }
                    warn!("Kucoin WS error, retrying in {:?}: {}", retry_delay, e);
                    tokio::select! {
                        _ = tokio::time::sleep(retry_delay) => {},
                        _ = cancel.cancelled() => break Ok(()),
                    }
                    let jitter =
                        std::time::Duration::from_millis(crate::exchanges::rand_int() % 1000);
                    retry_delay = (retry_delay * 2 + jitter).min(max_retry_delay);
                    // Обновляем токен для следующей попытки
                    if let Ok((new_url, new_token)) =
                        self.get_ws_token_rate_limited(is_futures).await
                    {
                        full_url = format!("{}?token={}", new_url, new_token);
                    }
                }
            }
        }
    }

    fn to_native_symbol(&self, unified: &str) -> String {
        // Сначала используем карту, при отсутствии - эвристический разбор
        let u2r = UNIFIED_TO_RAW.read();
        if let Some(raw) = u2r.get(unified) {
            return raw.clone();
        }
        drop(u2r);
        let without_settle = unified.strip_suffix(".P").unwrap_or(unified);
        if let Some((base, quote)) = without_settle.split_once('/') {
            format!("{}-{}", base, quote)
        } else {
            unified.to_string()
        }
    }

    fn to_unified_symbol(&self, native: &str) -> Option<String> {
        // Сначала используем карту, при отсутствии - эвристический разбор
        let r2u = RAW_TO_UNIFIED.read();
        if let Some(unified) = r2u.get(native) {
            return Some(unified.clone());
        }
        drop(r2u);
        let (base, quote) = KucoinConnector::parse_kucoin_symbol(native)?;
        let suffix = if self.market_type == MarketType::Perp {
            ".P"
        } else {
            ""
        };
        Some(format!("{}/{}{}", base, quote, suffix))
    }

    fn max_subscribe_args(&self) -> usize {
        75
    }
}

impl KucoinConnector {
    async fn connect_and_stream(
        &self,
        ws_url: &str,
        kucoin_symbols: &[String],
        tx: &tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(ws_url, None, true).await?;
        let (mut write, mut read) = ws_stream.split();

        // Kucoin: отправка индивидуального сообщения о подписке для каждого символа 
        // Spot: /market/match:BTC-USDT 
        // Perp: /contractMarket/execution:XBTUSDTM
        let topic_prefix = if self.market_type == MarketType::Perp {
            "/contractMarket/execution:"
        } else {
            "/market/match:"
        };
        for (i, symbol) in kucoin_symbols.iter().enumerate() {
            let msg = serde_json::json!({
                "id": i + 1,
                "type": "subscribe",
                "topic": format!("{}{}", topic_prefix, symbol),
                "privateChannel": false,
                "response": true
            });
            write.send(Message::Text(msg.to_string().into())).await?;
        }
        info!(
            "Kucoin {} WS connected and subscribed to {} symbols (topic={})",
            self.market_type,
            kucoin_symbols.len(),
            topic_prefix
        );

        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(15));
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
                            if text == "pong" {
                                continue;
                            }
                            if let Some(trades) = Self::parse_trade_message(&text) {
                                for trade in trades {
                                    if tx.send(trade).is_err() {
                    // Нет получателей
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            anyhow::bail!("Kucoin WS closed by server");
                        }
                        Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => {
                            anyhow::bail!("Kucoin WS error: {}", e);
                        }
                        None => {
                            anyhow::bail!("Kucoin WS stream ended");
                        }
                    }
                }
                _ = cancel.cancelled() => break,
            }

            if connected_since.elapsed() > MAX_CONN_LIFETIME {
                info!("Kucoin WS connection lifetime reached, reconnecting...");
                anyhow::bail!("connection lifetime expired");
            }
        }

        Ok(())
    }

    fn parse_trade_message(text: &str) -> Option<Vec<NormalizedTrade>> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;

        // Скип не нужных сообщений (welcome, ack, etc)
        let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
        if msg_type != "message" {
            return None;
        }

        let topic = v.get("topic")?.as_str()?;
        // Spot:  /market/match:BTC-USDT
        // Perp:  /contractMarket/execution:XBTUSDTM
        if !topic.starts_with("/market/match:") && !topic.starts_with("/contractMarket/execution:")
        {
            return None;
        }

        let data = v.get("data")?;
        let symbol = data.get("symbol")?.as_str()?.to_string();
        let is_futures = topic.starts_with("/contractMarket/execution:");

        // Находим унифицированный символ по исходному символу биржи с помощью таблицы соответствия
        let unified = {
            let map = RAW_TO_UNIFIED.read();
            map.get(&symbol).cloned()
        };
        let unified = if let Some(u) = unified {
            u
        } else {
            // Fallback: try heuristic parsing (spot only)
            let (base, quote) = Self::parse_kucoin_symbol(&symbol)?;
            format!("{}/{}", base, quote)
        };

        // Спот: цена и объем - строковые значения. Бессрочные фьючерсы: цена - строка, объем - целое число (контракты).
        let price: f64 = data
            .get("price")
            .and_then(|p| p.as_str()?.parse().ok())
            .or_else(|| data.get("price").and_then(|p| p.as_f64()))?;
        let size: f64 = if is_futures {
            // Фьючерсы: размер - целое число (количество контрактов)
            data.get("size")
                .and_then(|s| s.as_i64())
                .map(|s| s as f64)?
        } else {
            // Spot: размер - строка
            data.get("size").and_then(|s| s.as_str()?.parse().ok())?
        };

        // Spot:  data.time = наносекундная строка "1774993913317000000"
        // Perp:  data.ts   = наносекундное целое (например 1731898619520000000)
        let timestamp: i64 = if is_futures {
            data.get("ts")
                .and_then(|t| t.as_i64())
                .map(|ns| ns / 1_000_000)?
        } else {
            data.get("time")
                .and_then(|t| {
                    t.as_str()
                        .and_then(|s| s.parse::<i64>().ok().map(|ns| ns / 1_000_000))
                })
                .or_else(|| data.get("time").and_then(|t| t.as_i64()))?
        };

        Some(vec![NormalizedTrade {
            symbol: unified,
            timestamp_ms: timestamp,
            price,
            cost: price * size,
            exchange: Exchange::Kucoin,
        }])
    }
}
