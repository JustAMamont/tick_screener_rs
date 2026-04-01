use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

const TG_API_BASE: &str = "https://api.telegram.org";

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
    /// Buffered alert messages accumulated during cooldown.
    buffer: Vec<String>,
    /// When we're allowed to send again (set from 429 retry_after).
    cooldown_until: Instant,
}

impl ChatState {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            cooldown_until: Instant::now(),
        }
    }

    fn is_cooling_down(&self) -> bool {
        Instant::now() < self.cooldown_until
    }

    fn buffer_alert(&mut self, text: &str) {
        self.buffer.push(text.to_string());
    }

    fn has_buffer(&self) -> bool {
        !self.buffer.is_empty()
    }

    fn take_buffer(&mut self) -> String {
        let combined = self.buffer.join("\n\n");
        self.buffer.clear();
        self.buffer.shrink_to_fit();
        combined
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

    /// Send a message. If 429: buffer during cooldown, flush as one message after.
    pub async fn send_message(&self, chat_id: i64, text: &str) -> anyhow::Result<()> {
        let state = self.chat_state
            .entry(chat_id)
            .or_insert_with(|| Arc::new(Mutex::new(ChatState::new())))
            .value()
            .clone();

        loop {
            let mut s = state.lock().await;

            if s.is_cooling_down() {
                // Still cooling down — just buffer
                s.buffer_alert(text);
                return Ok(());
            }

            // Try to flush buffer first if anything accumulated
            if s.has_buffer() {
                let combined = s.take_buffer();
                drop(s);

                match self.send_http(chat_id, &combined).await {
                    Ok(()) => continue, // Buffer sent, now send the current alert below
                    Err(e) => {
                        if let Some(secs) = extract_retry_after(&e.to_string()) {
                            let mut s = state.lock().await;
                            let cd = Instant::now() + Duration::from_secs(secs) + Duration::from_millis(500);
                            s.cooldown_until = cd;
                            // Put combined back into buffer + the current alert
                            for line in combined.splitn(20, "\n\n") {
                                s.buffer.push(line.to_string());
                            }
                            s.buffer_alert(text);
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
                Ok(()) => return Ok(()),
                Err(e) => {
                    if let Some(secs) = extract_retry_after(&e.to_string()) {
                        let mut s = state.lock().await;
                        let cd = Instant::now() + Duration::from_secs(secs) + Duration::from_millis(500);
                        s.cooldown_until = cd;
                        s.buffer_alert(text);
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
            let combined = {
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
                }
                Ok(r) => {
                    let body = r.text().await.unwrap_or_default();
                    if let Some(secs) = extract_retry_after(&body) {
                        warn!("TG flush timer: 429 again for chat {}, retry in {}s", chat_id, secs);
                        let mut s = state.lock().await;
                        s.cooldown_until = Instant::now() + Duration::from_secs(secs) + Duration::from_millis(500);
                        // Re-arm timer for new cooldown
                        drop(s);
                        // We need to re-spawn but we don't have TgBot here
                        // Instead just reset cooldown — next send_message call will handle it
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

    /// Raw HTTP POST to Telegram API.
    async fn send_http(&self, chat_id: i64, text: &str) -> anyhow::Result<()> {
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

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            error!("TG API error {}: {}", status, body);
            anyhow::bail!("TG API error {}: {}", status, body);
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
