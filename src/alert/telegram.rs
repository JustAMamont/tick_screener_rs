use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

const TG_API_BASE: &str = "https://api.telegram.org";

#[derive(serde::Deserialize)]
struct TgResponse {
    #[allow(dead_code)]
    ok: bool,
    result: Option<TgMessage>,
}

#[derive(serde::Deserialize)]
struct TgMessage {
    message_id: i64,
}

/// Pool of Telegram bots, deduplicated by bot_token.
#[derive(Clone)]
pub struct BotPool {
    bots: DashMap<String, Arc<TgBot>>,
}

impl BotPool {
    pub fn new() -> Self {
        Self {
            bots: DashMap::new(),
        }
    }

    pub fn get_or_create(&self, token: &str) -> Arc<TgBot> {
        if let Some(bot) = self.bots.get(token) {
            return bot.clone();
        }
        let bot = Arc::new(TgBot::new(token));
        self.bots.insert(token.to_string(), bot.clone());
        info!("Created Telegram bot instance for token: {}...", &token[..token.len().min(10)]);
        bot
    }

    pub fn cleanup(&self, active_tokens: &HashSet<String>) {
        self.bots.retain(|token, _| active_tokens.contains(token));
    }
}

impl Default for BotPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-chat state: cooldown + buffer.
struct ChatState {
    /// Buffered alert messages accumulated during cooldown. (text, pin_flag)
    buffer: Vec<(String, bool)>,
    /// When we're allowed to send again (set from 429 retry_after).
    cooldown_until: Instant,
    /// When we're cooling down due to network errors
    network_cooldown_until: Instant,
    /// Count of consecutive network errors (for exponential backoff)
    network_error_count: u32,
}

impl ChatState {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            cooldown_until: Instant::now(),
            network_cooldown_until: Instant::now(),
            network_error_count: 0,
        }
    }

    fn is_cooling_down(&self) -> bool {
        Instant::now() < self.cooldown_until
    }

    fn is_network_cooling_down(&self) -> bool {
        Instant::now() < self.network_cooldown_until
    }

    fn set_network_cooldown(&mut self, duration: Duration) {
        self.network_cooldown_until = Instant::now() + duration;
        self.network_error_count = self.network_error_count.saturating_add(1);
    }

    fn reset_network_cooldown(&mut self) {
        self.network_cooldown_until = Instant::now();
        self.network_error_count = 0;
    }

    fn buffer_alert(&mut self, text: &str, pin: bool) {
        self.buffer.push((text.to_string(), pin));
    }

    fn has_buffer(&self) -> bool {
        !self.buffer.is_empty()
    }

    fn take_buffer(&mut self) -> (String, bool) {
        let texts: Vec<String> = self.buffer.iter().map(|(t, _)| t.clone()).collect();
        let combined = texts.join("\n\n");
        let should_pin = self.buffer.iter().any(|(_, p)| *p);
        self.buffer.clear();
        self.buffer.shrink_to_fit();
        (combined, should_pin)
    }
}

/// A single Telegram bot with per-chat rate limiting and alert buffering.
pub struct TgBot {
    token: String,
    client: reqwest::Client,
    chat_state: DashMap<i64, Arc<Mutex<ChatState>>>,
}

impl TgBot {
    pub fn new(token: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(5)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            token: token.to_string(),
            client,
            chat_state: DashMap::new(),
        }
    }

    fn is_network_error(err_str: &str) -> bool {
        let lower = err_str.to_lowercase();
        lower.contains("timeout")
            || lower.contains("connection refused")
            || lower.contains("connection reset")
            || lower.contains("broken pipe")
            || lower.contains("timed out")
            || lower.contains("connect error")
            || lower.contains("temporary failure")
            || lower.contains("network error")
            || lower.contains("dns")
            || lower.contains("hyper")
            || lower.contains("tls")
    }

    fn compute_network_cooldown(error_count: u32) -> Duration {
        let base = 10u64;
        let max = 300u64;
        let secs = base.saturating_mul(2u64.saturating_pow(error_count.min(5)));
        Duration::from_secs(secs.min(max))
    }

    fn spawn_network_repair_timer(&self, chat_id: i64, state: Arc<Mutex<ChatState>>, cooldown_until: Instant) {
        let token = self.token.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            let delay = cooldown_until
                .saturating_duration_since(Instant::now())
                + Duration::from_millis(100);

            tokio::time::sleep(delay).await;

            // Check if buffer has items
            let has_items = {
                let s = state.lock().await;
                s.has_buffer()
            };

            if !has_items {
                return;
            }

            // Try to send a simple ping-like request to test connectivity
            let url = format!("{}/bot{}/getMe", TG_API_BASE, token);
            let resp = client.get(&url).send().await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    info!("TG network repair: connectivity restored for chat {}", chat_id);
                    let mut s = state.lock().await;
                    s.reset_network_cooldown();
                }
                Ok(r) => {
                    let body = r.text().await.unwrap_or_default();
                    warn!("TG network repair: getMe failed for chat {}: {}", chat_id, body);
                }
                Err(e) => {
                    warn!("TG network repair: network still down for chat {}: {}", chat_id, e);
                    let mut s = state.lock().await;
                    let count = s.network_error_count;
                    s.set_network_cooldown(Self::compute_network_cooldown(count));
                }
            }
        });
    }

    /// Send a message. If 429: buffer during cooldown, flush as one message after.
    pub async fn send_message(&self, chat_id: i64, text: &str, pin: bool) -> anyhow::Result<()> {
        let state = self.chat_state
            .entry(chat_id)
            .or_insert_with(|| Arc::new(Mutex::new(ChatState::new())))
            .value()
            .clone();

        loop {
            let mut s = state.lock().await;

            if s.is_cooling_down() || s.is_network_cooling_down() {
                // Still cooling down — just buffer
                s.buffer_alert(text, pin);
                return Ok(());
            }

            // Try to flush buffer first if anything accumulated
            if s.has_buffer() {
                let (combined, combined_pin) = s.take_buffer();
                drop(s);

                match self.send_http(chat_id, &combined).await {
                    Ok(msg_id) => {
                        s = state.lock().await;
                        s.reset_network_cooldown();
                        drop(s);
                        if combined_pin {
                            let _ = self.pin_message(chat_id, msg_id).await;
                        }
                        continue; // Buffer sent, now send the current alert below
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        // Check for network errors BEFORE checking 429
                        if Self::is_network_error(&err_str) {
                            let mut s = state.lock().await;
                            let cooldown = Self::compute_network_cooldown(s.network_error_count);
                            s.set_network_cooldown(cooldown);
                            // Put combined back into buffer + the current alert
                            for line in combined.splitn(20, "\n\n") {
                                s.buffer.push((line.to_string(), combined_pin));
                            }
                            s.buffer_alert(text, pin);
                            warn!("TG network error flush for chat {}: buffering for {:?}", chat_id, cooldown);
                            self.spawn_network_repair_timer(chat_id, state.clone(), s.network_cooldown_until);
                            return Ok(());
                        }
                        if let Some(secs) = extract_retry_after(&err_str) {
                            let mut s = state.lock().await;
                            let cd = Instant::now() + Duration::from_secs(secs) + Duration::from_millis(500);
                            s.cooldown_until = cd;
                            // Put combined back into buffer + the current alert
                            for line in combined.splitn(20, "\n\n") {
                                s.buffer.push((line.to_string(), combined_pin));
                            }
                            s.buffer_alert(text, pin);
                            warn!("TG 429 flush for chat {}: buffering for {}s", chat_id, secs);
                            self.spawn_flush_timer(chat_id, state.clone(), cd);
                            return Ok(());
                        }
                        return Err(e);
                    }
                }
            }

            // No buffer — send the current alert immediately
            drop(s);
            match self.send_http(chat_id, text).await {
                Ok(msg_id) => {
                    let mut s = state.lock().await;
                    s.reset_network_cooldown();
                    drop(s);
                    if pin {
                        let _ = self.pin_message(chat_id, msg_id).await;
                    }
                    return Ok(());
                }
                Err(e) => {
                    let err_str = e.to_string();
                    // Check for network errors BEFORE checking 429
                    if Self::is_network_error(&err_str) {
                        let mut s = state.lock().await;
                        let cooldown = Self::compute_network_cooldown(s.network_error_count);
                        s.set_network_cooldown(cooldown);
                        s.buffer_alert(text, pin);
                        warn!("TG network error for chat {}: buffering for {:?}", chat_id, cooldown);
                        self.spawn_network_repair_timer(chat_id, state.clone(), s.network_cooldown_until);
                        return Ok(());
                    }
                    if let Some(secs) = extract_retry_after(&err_str) {
                        let mut s = state.lock().await;
                        let cd = Instant::now() + Duration::from_secs(secs) + Duration::from_millis(500);
                        s.cooldown_until = cd;
                        s.buffer_alert(text, pin);
                        warn!("TG 429 for chat {}: buffering for {}s", chat_id, secs);
                        self.spawn_flush_timer(chat_id, state.clone(), cd);
                        return Ok(());
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Spawn a background task that will flush the buffer when cooldown expires.
    /// Only one timer per cooldown period — subsequent 429s just update cooldown_until.
    fn spawn_flush_timer(&self, chat_id: i64, state: Arc<Mutex<ChatState>>, cooldown_until: Instant) {
        let token = self.token.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            let delay = cooldown_until
                .saturating_duration_since(Instant::now())
                + Duration::from_millis(100);

            tokio::time::sleep(delay).await;

            // Flush buffer if anything accumulated (regardless of cooldown state)
            let (combined, combined_pin) = {
                let mut s = state.lock().await;
                if s.has_buffer() {
                    let n = s.buffer.len();
                    info!("TG flush timer: flushing {} buffered alerts for chat {}", n, chat_id);
                    s.take_buffer()
                } else {
                    return;
                }
            };

            // Send combined message
            let url = format!("{}/bot{}/sendMessage", TG_API_BASE, token);
            let resp = client
                .post(&url)
                .json(&serde_json::json!({
                    "chat_id": chat_id,
                    "text": combined,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": true,
                }))
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    info!("TG flush timer: sent batch to chat {}", chat_id);
                    let mut s = state.lock().await;
                    s.cooldown_until = Instant::now();

                    if combined_pin {
                        if let Ok(body) = r.text().await {
                            if let Ok(data) = serde_json::from_str::<TgResponse>(&body) {
                                if let Some(msg) = data.result {
                                    let pin_url = format!("{}/bot{}/pinChatMessage", TG_API_BASE, token);
                                    let _ = client.post(&pin_url)
                                        .json(&serde_json::json!({
                                            "chat_id": chat_id,
                                            "message_id": msg.message_id,
                                            "disable_notification": false,
                                        }))
                                        .send()
                                        .await;
                                }
                            }
                        }
                    }
                }
                Ok(r) => {
                    let body = r.text().await.unwrap_or_default();
                    if let Some(secs) = extract_retry_after(&body) {
                        warn!("TG flush timer: 429 again for chat {}, retry in {}s", chat_id, secs);
                        let mut s = state.lock().await;
                        s.cooldown_until = Instant::now() + Duration::from_secs(secs) + Duration::from_millis(500);
                        // We do not re-arm here: the next send_message will hit `has_buffer()` on next incoming alert.
                    }
                    error!("TG flush timer: send failed for chat {}: {}", chat_id, body);
                    let mut s = state.lock().await;
                    s.cooldown_until = Instant::now();
                }
                Err(e) => {
                    error!("TG flush timer: network error for chat {}: {}", chat_id, e);
                    let mut s = state.lock().await;
                    s.cooldown_until = Instant::now();
                }
            }
        });
    }

    /// Raw HTTP POST to Telegram API. Returns the message_id if successful.
    async fn send_http(&self, chat_id: i64, text: &str) -> anyhow::Result<i64> {
        let url = format!("{}/bot{}/sendMessage", TG_API_BASE, self.token);
        let resp = self.client
            .post(&url)
            .json(&serde_json::json!({
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": true,
            }))
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        
        if !status.is_success() {
            error!("TG API error {}: {}", status, body);
            anyhow::bail!("TG API error {}: {}", status, body);
        }

        let data: TgResponse = serde_json::from_str(&body)?;
        if let Some(msg) = data.result {
            Ok(msg.message_id)
        } else {
            Ok(0)
        }
    }

    /// Pin an already sent message.
    async fn pin_message(&self, chat_id: i64, message_id: i64) -> anyhow::Result<()> {
        if message_id == 0 {
            return Ok(());
        }
        let url = format!("{}/bot{}/pinChatMessage", TG_API_BASE, self.token);
        let resp = self.client
            .post(&url)
            .json(&serde_json::json!({
                "chat_id": chat_id,
                "message_id": message_id,
                "disable_notification": false,
            }))
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            error!("TG API pin error {}: {}", status, body);
        }
        Ok(())
    }
}

/// Extract `retry_after` seconds from TG 429 error string.
fn extract_retry_after(s: &str) -> Option<u64> {
    let marker = "\"retry_after\":";
    let pos = s.find(marker)?;
    let after = &s[pos + marker.len()..];
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}
