use crate::config::MarketType;
use crate::exchanges::connector::ExchangeConnector;
use crate::exchanges::normalized::{Exchange, MarketInfo, NormalizedTrade};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

pub struct MexcConnector {
    market_type: MarketType,
    rest_base: String,
    #[allow(dead_code)]
    ws_base: String,
    client: reqwest::Client,
}

impl MexcConnector {
    pub fn new(market_type: MarketType) -> Self {
        Self {
            market_type,
            rest_base: "https://api.mexc.com".to_string(),
            ws_base: match market_type {
                MarketType::Spot => "wss://wbs-api.mexc.com/ws".to_string(),
                MarketType::Perp => "wss://contract.mexc.com/edge".to_string(),
            },
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    fn parse_mexc_symbol(raw: &str) -> Option<(String, String)> {
        if let Some((base, quote)) = raw.split_once('_')
            && !base.is_empty()
            && !quote.is_empty()
        {
            return Some((base.to_string(), quote.to_string()));
        }
        let quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH", "FDUSD"];
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
struct MexcSpotMarket {
    symbol: String,
    #[serde(rename = "baseAsset")]
    #[allow(dead_code)]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    #[allow(dead_code)]
    quote_asset: String,
    status: String,
    #[serde(rename = "isSpotTradingAllowed")]
    spot_trading: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct MexcExchangeInfo {
    symbols: Vec<MexcSpotMarket>,
}

#[derive(Debug, Deserialize)]
struct MexcFuturesResponse {
    success: bool,
    data: Vec<MexcFuturesMarket>,
}

#[derive(Debug, Deserialize)]
struct MexcFuturesMarket {
    symbol: String,
    #[serde(rename = "baseCoin")]
    #[allow(dead_code)]
    base_coin: String,
    #[serde(rename = "quoteCoin")]
    #[allow(dead_code)]
    quote_coin: String,
    /// Раньше использовался для `:USDT`-суффикса, сейчас не нужен -
    /// unified-формат использует `.P` для пометки perpetual.
    #[serde(rename = "settleCoin")]
    #[allow(dead_code)]
    settle_coin: String,
    state: i32,
    #[serde(rename = "futureType")]
    future_type: Option<i32>,
}

#[async_trait]
impl ExchangeConnector for MexcConnector {
    fn exchange(&self) -> Exchange {
        Exchange::Mexc
    }
    fn market_type(&self) -> MarketType {
        self.market_type
    }

    async fn load_markets(&self) -> anyhow::Result<Vec<MarketInfo>> {
        let markets: Vec<MarketInfo> = match self.market_type {
            MarketType::Spot => {
                let url = format!("{}/api/v3/exchangeInfo", self.rest_base);
                let resp: MexcExchangeInfo = self.client.get(&url).send().await?.json().await?;
                resp.symbols
                    .into_iter()
                    .filter_map(|m| {
                        if m.status != "1" {
                            return None;
                        }
                        if let Some(false) = m.spot_trading {
                            return None;
                        }
                        let (base, quote) = MexcConnector::parse_mexc_symbol(&m.symbol)?;
                        let unified = format!("{}/{}", base, quote);
                        Some(MarketInfo {
                            symbol: unified,
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
                let url = "https://contract.mexc.com/api/v1/contract/detail";
                let resp: MexcFuturesResponse = self.client.get(url).send().await?.json().await?;
                if !resp.success {
                    anyhow::bail!("MEXC futures API returned success=false");
                }
                resp.data
                    .into_iter()
                    .filter_map(|m| {
                        if m.state != 0 {
                            return None;
                        }
                        if m.future_type.is_some() && m.future_type != Some(1) {
                            return None;
                        }
                        let (base, quote) = MexcConnector::parse_mexc_symbol(&m.symbol)?;
                        let unified = format!("{}/{}.P", base, quote);
                        Some(MarketInfo {
                            symbol: unified,
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
        info!("MEXC {} loaded {} markets", self.market_type, markets.len());
        Ok(markets)
    }

    async fn stream_trades(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        match self.market_type {
            MarketType::Spot => self.stream_trades_spot(symbols, tx, cancel).await,
            MarketType::Perp => self.stream_trades_perp(symbols, tx, cancel).await,
        }
    }

    fn to_native_symbol(&self, unified: &str) -> String {
        let without_settle = unified.strip_suffix(".P").unwrap_or(unified);
        if let Some((base, quote)) = without_settle.split_once('/') {
            match self.market_type {
                MarketType::Spot => format!("{}{}", base, quote),
                MarketType::Perp => format!("{}_{}", base, quote),
            }
        } else {
            unified.to_string()
        }
    }

    fn to_unified_symbol(&self, native: &str) -> Option<String> {
        let (base, quote) = MexcConnector::parse_mexc_symbol(native)?;
        let suffix = if self.market_type == MarketType::Perp {
            ".P"
        } else {
            ""
        };
        Some(format!("{}/{}{}", base, quote, suffix))
    }

    fn max_subscribe_args(&self) -> usize {
        match self.market_type {
            MarketType::Spot => 30,
            MarketType::Perp => 30,
        }
    }
}

impl MexcConnector {
    async fn stream_trades_spot(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let mexc_symbols: Vec<String> = symbols
            .iter()
            .filter_map(|s| {
                let (b, q) = s.split_once('/')?;
                Some(format!("{}{}", b, q))
            })
            .collect();
        const MAX_SUBS: usize = 30;
        let mut batch_start = 0;
        loop {
            if batch_start >= mexc_symbols.len() || cancel.is_cancelled() {
                break;
            }
            let end = (batch_start + MAX_SUBS).min(mexc_symbols.len());
            let batch: Vec<String> = mexc_symbols[batch_start..end].to_vec();
            match self.stream_batch_spot(&batch, &tx, cancel.clone()).await {
                Ok(()) => {}
                Err(e) => {
                    if cancel.is_cancelled() {
                        break;
                    }
                    warn!("MEXC spot WS batch {}-{} ended: {}", batch_start, end, e);
                }
            }
            batch_start = end;
        }
        Ok(())
    }

    async fn stream_batch_spot(
        &self,
        symbols: &[String],
        tx: &tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let mut retry_delay = std::time::Duration::from_secs(1);
        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            match self.connect_spot(symbols, tx, &cancel).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if cancel.is_cancelled() {
                        return Ok(());
                    }
                    warn!("MEXC spot WS error, retrying in {:?}: {}", retry_delay, e);
                    tokio::select! {
                        _ = tokio::time::sleep(retry_delay) => {},
                        _ = cancel.cancelled() => return Ok(()),
                    }
                    let jitter =
                        std::time::Duration::from_millis(crate::exchanges::rand_int() % 1000);
                    retry_delay =
                        ((retry_delay * 2) + jitter).min(std::time::Duration::from_secs(30));
                }
            }
        }
    }

    async fn connect_spot(
        &self,
        symbols: &[String],
        tx: &tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let ws_url = "wss://wbs-api.mexc.com/ws";
        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(ws_url, None, true).await?;
        let (mut write, mut read) = ws_stream.split();

        let params: Vec<String> = symbols
            .iter()
            .map(|s| format!("spot@public.aggre.deals.v3.api.pb@100ms@{}", s))
            .collect();
        let subscribe_msg = serde_json::json!({ "method": "SUBSCRIPTION", "params": params });
        write
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await?;
        info!("MEXC spot WS subscribed to {} symbols", symbols.len());

        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(25));
        let connected_since = Instant::now();

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if write.send(Message::Text(r#"{"method":"PING"}"#.into())).await.is_err() {
                        anyhow::bail!("Failed to send MEXC PING");
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if text.contains(r#""msg":"PONG""#) { continue; }
                        }
                        Some(Ok(Message::Binary(data))) => {
                            if let Some(trades) = Self::parse_spot_pb(&data) {
                                for trade in trades { let _ = tx.send(trade); }
                            }
                        }
                        Some(Ok(Message::Ping(d))) => { let _ = write.send(Message::Pong(d)).await; }
                        Some(Ok(Message::Close(_))) => anyhow::bail!("MEXC spot WS closed"),
                        Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => anyhow::bail!("MEXC spot WS error: {}", e),
                        None => anyhow::bail!("MEXC spot WS ended"),
                    }
                }
                _ = cancel.cancelled() => return Ok(()),
            }
            if connected_since.elapsed() > std::time::Duration::from_secs(24 * 3600) {
                anyhow::bail!("MEXC spot WS connection lifetime expired");
            }
        }
    }

    async fn stream_trades_perp(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let mexc_symbols: Vec<String> = symbols
            .iter()
            .filter_map(|s| {
                let ws = s.strip_suffix(".P").unwrap_or(s);
                let (b, q) = ws.split_once('/')?;
                Some(format!("{}_{}", b, q))
            })
            .collect();
        if mexc_symbols.is_empty() {
            warn!(
                "MEXC perp: no native symbols found for {} unified symbols",
                symbols.len()
            );
            return Ok(());
        }
        let mut retry_delay = std::time::Duration::from_secs(1);
        loop {
            if cancel.is_cancelled() {
                break Ok(());
            }
            match self.connect_perp(&mexc_symbols, &tx, &cancel).await {
                Ok(()) => break Ok(()),
                Err(e) => {
                    if cancel.is_cancelled() {
                        break Ok(());
                    }
                    warn!("MEXC perp WS error, retrying in {:?}: {}", retry_delay, e);
                    tokio::select! {
                        _ = tokio::time::sleep(retry_delay) => {},
                        _ = cancel.cancelled() => break Ok(()),
                    }
                    let jitter =
                        std::time::Duration::from_millis(crate::exchanges::rand_int() % 1000);
                    retry_delay =
                        ((retry_delay * 2) + jitter).min(std::time::Duration::from_secs(30));
                }
            }
        }
    }

    async fn connect_perp(
        &self,
        symbols: &[String],
        tx: &tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<()> {
        let ws_url = "wss://contract.mexc.com/edge";
        let (ws_stream, _) =
            tokio_tungstenite::connect_async_with_config(ws_url, None, true).await?;
        let (mut write, mut read) = ws_stream.split();

        for symbol in symbols {
            let msg = serde_json::json!({ "method": "sub.deal", "param": { "symbol": symbol } });
            write.send(Message::Text(msg.to_string().into())).await?;
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        }
        info!("MEXC perp WS subscribed to {} symbols", symbols.len());

        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(15));
        let connected_since = Instant::now();

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if write.send(Message::Text(r#"{"method":"ping"}"#.into())).await.is_err() {
                        anyhow::bail!("Failed to send MEXC perp ping");
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if text.contains("pong") { continue; }
                            if let Some(trades) = Self::parse_perp_trade(&text) {
                                for trade in trades { let _ = tx.send(trade); }
                            }
                        }
                        Some(Ok(Message::Ping(d))) => { let _ = write.send(Message::Pong(d)).await; }
                        Some(Ok(Message::Close(_))) => anyhow::bail!("MEXC perp WS closed"),
                        Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => anyhow::bail!("MEXC perp WS error: {}", e),
                        None => anyhow::bail!("MEXC perp WS ended"),
                    }
                }
                _ = cancel.cancelled() => return Ok(()),
            }
            if connected_since.elapsed() > std::time::Duration::from_secs(24 * 3600) {
                anyhow::bail!("MEXC perp WS connection lifetime expired");
            }
        }
    }

    fn parse_spot_pb(data: &[u8]) -> Option<Vec<NormalizedTrade>> {
        if data.first() == Some(&b'{') {
            return None;
        }

        let (symbol, _send_time, deals_bytes) = pb_parse_wrapper(data)?;
        if deals_bytes.is_empty() {
            return None;
        }

        let (base, quote) = MexcConnector::parse_mexc_symbol(&symbol)?;
        let unified = format!("{}/{}", base, quote);

        let mut trades = Vec::new();
        let mut pos = 0;
        while pos < deals_bytes.len() {
            let (field_num, wire_type, val_bytes, consumed) = pb_read_field(&deals_bytes[pos..])?;
            pos += consumed;
            if wire_type != PB_WIRE_LEN || field_num != 1 {
                continue;
            }
            let item = pb_parse_deal_item(val_bytes)?;
            trades.push(NormalizedTrade {
                symbol: unified.clone(),
                timestamp_ms: item.time_ms,
                price: item.price,
                cost: item.price * item.quantity,
                exchange: Exchange::Mexc,
            });
        }
        if trades.is_empty() {
            None
        } else {
            Some(trades)
        }
    }

    fn parse_perp_trade(text: &str) -> Option<Vec<NormalizedTrade>> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;
        let channel = v.get("channel")?.as_str()?;
        if channel != "push.deal" && channel != "push.dealWithPrice" {
            return None;
        }

        let symbol = v.get("symbol")?.as_str()?.to_string();
        let (base, quote) = MexcConnector::parse_mexc_symbol(&symbol)?;
        let unified = format!("{}/{}.P", base, quote);

        let deals = v.get("data")?.as_array()?;
        if deals.is_empty() {
            return None;
        }

        let mut trades = Vec::with_capacity(deals.len());
        for deal in deals {
            let price: f64 = deal.get("p").and_then(|p| p.as_f64())?;
            let volume: f64 = deal
                .get("v")
                .and_then(|v| v.as_f64())
                .or_else(|| deal.get("v").and_then(|v| v.as_i64().map(|v| v as f64)))?;
            let timestamp: i64 = deal.get("t")?.as_i64()?;

            trades.push(NormalizedTrade {
                symbol: unified.clone(),
                timestamp_ms: timestamp,
                price,
                cost: price * volume,
                exchange: Exchange::Mexc,
            });
        }
        if trades.is_empty() {
            None
        } else {
            Some(trades)
        }
    }
}

// --- Minimal protobuf wire format parser ---

const PB_WIRE_VARINT: u8 = 0;
const PB_WIRE_LEN: u8 = 2;

struct DealItem {
    price: f64,
    quantity: f64,
    time_ms: i64,
}

fn pb_parse_wrapper(data: &[u8]) -> Option<(String, i64, &[u8])> {
    let mut channel = String::new();
    let mut symbol = String::new();
    let send_time: i64 = 0;
    let mut aggre_bytes: &[u8] = &[];

    let mut pos = 0;
    while pos < data.len() {
        let (field_num, wire_type, val, consumed) = pb_read_field(&data[pos..])?;
        pos += consumed;
        match (field_num, wire_type) {
            (1, PB_WIRE_LEN) => {
                channel = String::from_utf8(val.to_vec()).unwrap_or_default();
            }
            (3, PB_WIRE_LEN) => {
                symbol = String::from_utf8(val.to_vec()).ok()?;
            }
            (5, PB_WIRE_VARINT) | (6, PB_WIRE_VARINT) => {}
            (314, PB_WIRE_LEN) => {
                aggre_bytes = val;
            }
            _ => {}
        }
    }

    if symbol.is_empty()
        && !channel.is_empty()
        && let Some(last_part) = channel.rsplit('@').next()
        && !last_part.contains('.')
        && !last_part.starts_with("spot")
        && !last_part.starts_with("public")
    {
        symbol = last_part.to_string();
    }

    if symbol.is_empty() || aggre_bytes.is_empty() {
        return None;
    }
    Some((symbol, send_time, aggre_bytes))
}

fn pb_parse_deal_item(data: &[u8]) -> Option<DealItem> {
    let mut price = 0.0f64;
    let mut quantity = 0.0f64;
    let mut time_ms: i64 = 0;
    let mut pos = 0;
    while pos < data.len() {
        let (field_num, wire_type, val, consumed) = pb_read_field(&data[pos..])?;
        pos += consumed;
        match (field_num, wire_type) {
            (1, PB_WIRE_LEN) => {
                price = fast_f64(val);
            }
            (2, PB_WIRE_LEN) => {
                quantity = fast_f64(val);
            }
            (4, PB_WIRE_VARINT) => {
                time_ms = pb_decode_varint(val) as i64;
            }
            _ => {}
        }
    }
    Some(DealItem {
        price,
        quantity,
        time_ms,
    })
}

#[inline(always)]
fn fast_f64(bytes: &[u8]) -> f64 {
    if bytes.is_empty() {
        return 0.0;
    }
    let mut i = 0usize;
    let len = bytes.len();

    let sign: f64 = if bytes[i] == b'-' {
        i += 1;
        -1.0
    } else {
        1.0
    };

    let mut result: f64 = 0.0;
    let mut has_digits = false;
    while i < len && bytes[i].is_ascii_digit() {
        result = result * 10.0 + (bytes[i] - b'0') as f64;
        i += 1;
        has_digits = true;
    }

    if i < len && bytes[i] == b'.' {
        i += 1;
        let mut divisor = 10.0f64;
        while i < len && bytes[i].is_ascii_digit() {
            result += ((bytes[i] - b'0') as f64) / divisor;
            divisor *= 10.0;
            i += 1;
            has_digits = true;
        }
    }

    if i < len && (bytes[i] == b'e' || bytes[i] == b'E') {
        i += 1;
        let exp_sign: f64 = if i < len && bytes[i] == b'-' {
            i += 1;
            -1.0
        } else if i < len && bytes[i] == b'+' {
            i += 1;
            1.0
        } else {
            1.0
        };
        let mut exp: f64 = 0.0;
        while i < len && bytes[i].is_ascii_digit() {
            exp = exp * 10.0 + (bytes[i] - b'0') as f64;
            i += 1;
        }
        result *= 10.0f64.powf(exp * exp_sign);
    }

    if has_digits { result * sign } else { 0.0 }
}

fn pb_read_field(data: &[u8]) -> Option<(u64, u8, &[u8], usize)> {
    let (tag, pos) = pb_decode_varint_with_pos(data)?;
    let field_num = tag >> 3;
    let wire_type = (tag & 0x07) as u8;
    match wire_type {
        PB_WIRE_VARINT => {
            let (_val, consumed) = pb_decode_varint_with_pos(&data[pos..])?;
            let abs_end = pos + consumed;
            Some((field_num, wire_type, &data[pos..abs_end], abs_end))
        }
        PB_WIRE_LEN => {
            let (len, consumed) = pb_decode_varint_with_pos(&data[pos..])?;
            let len = len as usize;
            let data_start = pos + consumed;
            let data_end = data_start + len;
            if data_end > data.len() {
                return None;
            }
            Some((field_num, wire_type, &data[data_start..data_end], data_end))
        }
        _ => None,
    }
}

fn pb_decode_varint(data: &[u8]) -> u64 {
    let (val, _) = pb_decode_varint_with_pos(data).unwrap_or((0, 0));
    val
}

fn pb_decode_varint_with_pos(data: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in data.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        if shift >= 64 {
            return None;
        }
    }
    None
}
