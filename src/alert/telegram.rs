//! Отправка алертов в Telegram: пул ботов, rate limit, буферизация.
//!
//! # Особенности
//!
//! * **`BotPool`**: дедупликация `TgBot` по `bot_token` - несколько
//!   сканеров с одним токеном шарят одного бота.
//! * **Rate limit (429)**: при получении 429 от Telegram читаем
//!   `retry_after`, переходим в cooldown на это время. Все алерты
//!   за это время буферизуются и отправляются после истечения cooldown.
//! * **Network errors**: экспоненциальный backoff при сетевых сбоях
//!   (timeout, DNS, TLS). Автопроверка восстановления через `getMe`.
//! * **Разделение буферов**: алерты о листингах (pin=true) и обычные
//!   volatility-алерты (pin=false) накапливаются в разных буферах и
//!   отправляются отдельными сообщениями. В закрепе пинится только
//!   сообщение с листингами.

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

/// Пул Telegram-ботов, дедуплицированный по `bot_token`.
///
/// Несколько сканеров могут использовать один и тот же бот-токен -
/// в этом случае они шарят одного `TgBot`, что позволяет использовать
/// общий HTTP-клиент и кэш rate-limit-состояния по чатам.
#[derive(Clone)]
pub struct BotPool {
    bots: DashMap<String, Arc<TgBot>>,
}

impl BotPool {
    /// Создаёт пустой пул.
    pub fn new() -> Self {
        Self {
            bots: DashMap::new(),
        }
    }

    /// Возвращает существующего бота или создаёт нового для токена.
    pub fn get_or_create(&self, token: &str) -> Arc<TgBot> {
        if let Some(bot) = self.bots.get(token) {
            return bot.clone();
        }
        let bot = Arc::new(TgBot::new(token));
        self.bots.insert(token.to_string(), bot.clone());
        info!(
            "Created Telegram bot instance for token: {}...",
            &token[..token.len().min(10)]
        );
        bot
    }

    /// Удаляет ботов, чьи токены не входят в `active_tokens`.
    ///
    /// Вызывается при hot-reload конфигурации: если сканер с токеном X
    /// удалён, бот больше не нужен.
    pub fn cleanup(&self, active_tokens: &HashSet<String>) {
        self.bots.retain(|token, _| active_tokens.contains(token));
    }
}

impl Default for BotPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Состояние одного чата: раздельные буферы алертов и кулдауны при 429.
///
/// Буфер разделён на два независимых списка:
/// * `listing_buffer` - алерты о новых листингах (отправляются одним
///   сообщением и закрепляются в чате);
/// * `volatility_buffer` - обычные алерты по волатильности (отправляются
///   отдельным сообщением, без закрепления).
///
/// При сбросе буфера (`take_buffers`) отправляются два сообщения:
/// сначала листинги (с pin), затем волатильность (без pin). Это
/// гарантирует, что в закрепе чата оказывается только сообщение о
/// листингах, а не смесь листингов и волатильности.
struct ChatState {
    /// Буфер листингов: каждый элемент - готовый текст одного алерта.
    listing_buffer: Vec<String>,
    /// Буфер volatility-алертов: каждый элемент - готовый текст одного алерта.
    volatility_buffer: Vec<String>,
    /// До какого момента действует cooldown от Telegram 429.
    cooldown_until: Instant,
    /// До какого момента действует cooldown из-за сетевых ошибок.
    network_cooldown_until: Instant,
    /// Количество подряд сетевых ошибок (для экспоненциального backoff).
    network_error_count: u32,
}

impl ChatState {
    fn new() -> Self {
        Self {
            listing_buffer: Vec::new(),
            volatility_buffer: Vec::new(),
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

    /// Кладёт алерт в соответствующий буфер.
    ///
    /// `is_listing = true` означает алерт о листинге (будет закреплён),
    /// `false` - обычный volatility-алерт.
    fn buffer_alert(&mut self, text: &str, is_listing: bool) {
        if is_listing {
            self.listing_buffer.push(text.to_string());
        } else {
            self.volatility_buffer.push(text.to_string());
        }
    }

    fn has_buffer(&self) -> bool {
        !self.listing_buffer.is_empty() || !self.volatility_buffer.is_empty()
    }

    /// Забирает оба буфера, склеивая каждый в одно сообщение.
    ///
    /// Возвращает `(listings, volatility)`, где каждый `Option<String>`
    /// - это объединённое сообщение для соответствующего типа алертов
    ///   (`None`, если буфер был пуст). После вызова оба буфера очищаются.
    fn take_buffers(&mut self) -> (Option<String>, Option<String>) {
        let listings = if self.listing_buffer.is_empty() {
            None
        } else {
            let combined = self.listing_buffer.join("\n\n");
            Some(combined)
        };
        let volatility = if self.volatility_buffer.is_empty() {
            None
        } else {
            let combined = self.volatility_buffer.join("\n\n");
            Some(combined)
        };
        self.listing_buffer.clear();
        self.volatility_buffer.clear();
        self.listing_buffer.shrink_to_fit();
        self.volatility_buffer.shrink_to_fit();
        (listings, volatility)
    }
}

/// Один Telegram-бот с per-chat rate limiting и раздельной буферизацией
/// листингов и volatility-алертов.
///
/// См. документацию модуля для описания поведения при 429 и сетевых
/// ошибках.
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

    fn spawn_network_repair_timer(
        &self,
        chat_id: i64,
        state: Arc<Mutex<ChatState>>,
        cooldown_until: Instant,
    ) {
        let token = self.token.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            let delay = cooldown_until.saturating_duration_since(Instant::now())
                + Duration::from_millis(100);

            tokio::time::sleep(delay).await;

            // Проверяем, содержит ли буфер элементы
            let has_items = {
                let s = state.lock().await;
                s.has_buffer()
            };

            if !has_items {
                return;
            }

            // Пингуем телегу, чтобы проверить соединение.
            let url = format!("{}/bot{}/getMe", TG_API_BASE, token);
            let resp = client.get(&url).send().await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    info!(
                        "TG network repair: connectivity restored for chat {}",
                        chat_id
                    );
                    let mut s = state.lock().await;
                    s.reset_network_cooldown();
                }
                Ok(r) => {
                    let body = r.text().await.unwrap_or_default();
                    warn!(
                        "TG network repair: getMe failed for chat {}: {}",
                        chat_id, body
                    );
                }
                Err(e) => {
                    warn!(
                        "TG network repair: network still down for chat {}: {}",
                        chat_id, e
                    );
                    let mut s = state.lock().await;
                    let count = s.network_error_count;
                    s.set_network_cooldown(Self::compute_network_cooldown(count));
                }
            }
        });
    }

    /// Отправляет сообщение. При получении 429 накапливает алерты в раздельных
    /// буферах (листинги и volatility), а после истечения cooldown отправляет
    /// их двумя отдельными сообщениями: листинги закрепляются, volatility - нет.
    pub async fn send_message(&self, chat_id: i64, text: &str, pin: bool) -> anyhow::Result<()> {
        let state = self
            .chat_state
            .entry(chat_id)
            .or_insert_with(|| Arc::new(Mutex::new(ChatState::new())))
            .value()
            .clone();

        loop {
            let mut s = state.lock().await;

            if s.is_cooling_down() || s.is_network_cooling_down() {
                // Время кулдауна не прошло - кладём алерт в соответствующий буфер.
                s.buffer_alert(text, pin);
                return Ok(());
            }

            // Сначала сбрасываем буфер листингов (отдельное сообщение с pin).
            if let Some(listing_text) = take_listing(&mut s) {
                drop(s);
                match self.send_http(chat_id, &listing_text).await {
                    Ok(msg_id) => {
                        let mut s = state.lock().await;
                        s.reset_network_cooldown();
                        drop(s);
                        // Листинги всегда закрепляются.
                        let _ = self.pin_message(chat_id, msg_id).await;
                        // Повторяем цикл: сбросим volatility-буфер, затем отправим текущий.
                        continue;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        // Сначала проверяем сетевые ошибки перед 429.
                        if Self::is_network_error(&err_str) {
                            let mut s = state.lock().await;
                            let cooldown = Self::compute_network_cooldown(s.network_error_count);
                            s.set_network_cooldown(cooldown);
                            // Возвращаем листинги в буфер и кладём текущий алерт.
                            push_back_listing(&mut s, &listing_text);
                            s.buffer_alert(text, pin);
                            warn!(
                                "TG network error flush for chat {}: buffering for {:?}",
                                chat_id, cooldown
                            );
                            self.spawn_network_repair_timer(
                                chat_id,
                                state.clone(),
                                s.network_cooldown_until,
                            );
                            return Ok(());
                        }
                        if let Some(secs) = extract_retry_after(&err_str) {
                            let mut s = state.lock().await;
                            let cd = Instant::now()
                                + Duration::from_secs(secs)
                                + Duration::from_millis(500);
                            s.cooldown_until = cd;
                            push_back_listing(&mut s, &listing_text);
                            s.buffer_alert(text, pin);
                            warn!("TG 429 flush for chat {}: buffering for {}s", chat_id, secs);
                            self.spawn_flush_timer(chat_id, state.clone(), cd);
                            return Ok(());
                        }
                        return Err(e);
                    }
                }
            }

            // Затем сбрасываем буфер volatility (отдельное сообщение без pin).
            if let Some(vol_text) = take_volatility(&mut s) {
                drop(s);
                match self.send_http(chat_id, &vol_text).await {
                    Ok(_) => {
                        let mut s = state.lock().await;
                        s.reset_network_cooldown();
                        drop(s);
                        // Повторяем цикл: теперь отправим текущий алерт напрямую.
                        continue;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if Self::is_network_error(&err_str) {
                            let mut s = state.lock().await;
                            let cooldown = Self::compute_network_cooldown(s.network_error_count);
                            s.set_network_cooldown(cooldown);
                            push_back_volatility(&mut s, &vol_text);
                            s.buffer_alert(text, pin);
                            warn!(
                                "TG network error flush for chat {}: buffering for {:?}",
                                chat_id, cooldown
                            );
                            self.spawn_network_repair_timer(
                                chat_id,
                                state.clone(),
                                s.network_cooldown_until,
                            );
                            return Ok(());
                        }
                        if let Some(secs) = extract_retry_after(&err_str) {
                            let mut s = state.lock().await;
                            let cd = Instant::now()
                                + Duration::from_secs(secs)
                                + Duration::from_millis(500);
                            s.cooldown_until = cd;
                            push_back_volatility(&mut s, &vol_text);
                            s.buffer_alert(text, pin);
                            warn!("TG 429 flush for chat {}: buffering for {}s", chat_id, secs);
                            self.spawn_flush_timer(chat_id, state.clone(), cd);
                            return Ok(());
                        }
                        return Err(e);
                    }
                }
            }

            // Оба буфера пусты - отправляем текущий алерт напрямую.
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
                    // Сначала проверяем сетевые ошибки перед 429.
                    if Self::is_network_error(&err_str) {
                        let mut s = state.lock().await;
                        let cooldown = Self::compute_network_cooldown(s.network_error_count);
                        s.set_network_cooldown(cooldown);
                        s.buffer_alert(text, pin);
                        warn!(
                            "TG network error for chat {}: buffering for {:?}",
                            chat_id, cooldown
                        );
                        self.spawn_network_repair_timer(
                            chat_id,
                            state.clone(),
                            s.network_cooldown_until,
                        );
                        return Ok(());
                    }
                    if let Some(secs) = extract_retry_after(&err_str) {
                        let mut s = state.lock().await;
                        let cd =
                            Instant::now() + Duration::from_secs(secs) + Duration::from_millis(500);
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

    /// Фоновая таска: после истечения cooldown сбрасывает оба буфера
    /// двумя отдельными сообщениями. Только один таймер на период
    /// cooldown - последующие 429 просто обновляют `cooldown_until`.
    fn spawn_flush_timer(
        &self,
        chat_id: i64,
        state: Arc<Mutex<ChatState>>,
        cooldown_until: Instant,
    ) {
        let token = self.token.clone();
        let client = self.client.clone();

        tokio::spawn(async move {
            let delay = cooldown_until.saturating_duration_since(Instant::now())
                + Duration::from_millis(100);

            tokio::time::sleep(delay).await;

            // Забираем оба буфера - листинги и volatility отдельно.
            let (listings, volatility) = {
                let mut s = state.lock().await;
                if s.has_buffer() {
                    let n = s.listing_buffer.len() + s.volatility_buffer.len();
                    info!(
                        "TG flush timer: flushing {} buffered alerts for chat {}",
                        n, chat_id
                    );
                    s.take_buffers()
                } else {
                    return;
                }
            };

            // 1. Отправляем сообщение с листингами (и закрепляем).
            let mut any_error = false;
            if let Some(text) = listings {
                let url = format!("{}/bot{}/sendMessage", TG_API_BASE, token);
                let resp = client
                    .post(&url)
                    .json(&serde_json::json!({
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": true,
                    }))
                    .send()
                    .await;

                match resp {
                    Ok(r) if r.status().is_success() => {
                        info!("TG flush timer: sent listing batch to chat {}", chat_id);
                        // Закрепляем сообщение с листингами.
                        if let Ok(body) = r.text().await
                            && let Ok(data) = serde_json::from_str::<TgResponse>(&body)
                            && let Some(msg) = data.result
                        {
                            let pin_url = format!("{}/bot{}/pinChatMessage", TG_API_BASE, token);
                            let _ = client
                                .post(&pin_url)
                                .json(&serde_json::json!({
                                    "chat_id": chat_id,
                                    "message_id": msg.message_id,
                                    "disable_notification": false,
                                }))
                                .send()
                                .await;
                        }
                    }
                    Ok(r) => {
                        let body = r.text().await.unwrap_or_default();
                        if let Some(secs) = extract_retry_after(&body) {
                            warn!(
                                "TG flush timer: 429 again for chat {}, retry in {}s",
                                chat_id, secs
                            );
                            let mut s = state.lock().await;
                            s.cooldown_until = Instant::now()
                                + Duration::from_secs(secs)
                                + Duration::from_millis(500);
                            // Возвращаем листинги в буфер.
                            for line in text.split("\n\n") {
                                if !line.is_empty() {
                                    s.listing_buffer.push(line.to_string());
                                }
                            }
                            any_error = true;
                        } else {
                            error!(
                                "TG flush timer: listing send failed for chat {}: {}",
                                chat_id, body
                            );
                        }
                    }
                    Err(e) => {
                        error!(
                            "TG flush timer: listing network error for chat {}: {}",
                            chat_id, e
                        );
                        let mut s = state.lock().await;
                        // Возвращаем листинги в буфер.
                        for line in text.split("\n\n") {
                            if !line.is_empty() {
                                s.listing_buffer.push(line.to_string());
                            }
                        }
                        any_error = true;
                    }
                }
            }

            // 2. Отправляем сообщение с volatility-алертами (без pin).
            if let Some(text) = volatility {
                if any_error {
                    // Если не получилось отправить листинги, нет смысла
                    // пытаться отправить volatility - тот же rate limit.
                    let mut s = state.lock().await;
                    for line in text.split("\n\n") {
                        if !line.is_empty() {
                            s.volatility_buffer.push(line.to_string());
                        }
                    }
                } else {
                    let url = format!("{}/bot{}/sendMessage", TG_API_BASE, token);
                    let resp = client
                        .post(&url)
                        .json(&serde_json::json!({
                            "chat_id": chat_id,
                            "text": text,
                            "parse_mode": "Markdown",
                            "disable_web_page_preview": true,
                        }))
                        .send()
                        .await;

                    match resp {
                        Ok(r) if r.status().is_success() => {
                            info!("TG flush timer: sent volatility batch to chat {}", chat_id);
                        }
                        Ok(r) => {
                            let body = r.text().await.unwrap_or_default();
                            if let Some(secs) = extract_retry_after(&body) {
                                warn!(
                                    "TG flush timer: 429 on volatility for chat {}, retry in {}s",
                                    chat_id, secs
                                );
                                let mut s = state.lock().await;
                                s.cooldown_until = Instant::now()
                                    + Duration::from_secs(secs)
                                    + Duration::from_millis(500);
                                for line in text.split("\n\n") {
                                    if !line.is_empty() {
                                        s.volatility_buffer.push(line.to_string());
                                    }
                                }
                            } else {
                                error!(
                                    "TG flush timer: volatility send failed for chat {}: {}",
                                    chat_id, body
                                );
                            }
                        }
                        Err(e) => {
                            error!(
                                "TG flush timer: volatility network error for chat {}: {}",
                                chat_id, e
                            );
                            let mut s = state.lock().await;
                            for line in text.split("\n\n") {
                                if !line.is_empty() {
                                    s.volatility_buffer.push(line.to_string());
                                }
                            }
                        }
                    }
                }
            }

            // Сбрасываем cooldown только если всё прошло успешно.
            if !any_error {
                let mut s = state.lock().await;
                s.cooldown_until = Instant::now();
            }
        });
    }

    /// Отправляет HTTP-запрос к Telegram API для отправки сообщения.
    async fn send_http(&self, chat_id: i64, text: &str) -> anyhow::Result<i64> {
        let url = format!("{}/bot{}/sendMessage", TG_API_BASE, self.token);
        let resp = self
            .client
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

    /// Закрепляет уже отправленное сообщение.
    async fn pin_message(&self, chat_id: i64, message_id: i64) -> anyhow::Result<()> {
        if message_id == 0 {
            return Ok(());
        }
        let url = format!("{}/bot{}/pinChatMessage", TG_API_BASE, self.token);
        let resp = self
            .client
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

/// Вспомогательная функция: забирает склеенное сообщение из буфера листингов.
#[inline]
fn take_listing(state: &mut ChatState) -> Option<String> {
    if state.listing_buffer.is_empty() {
        return None;
    }
    let combined = state.listing_buffer.join("\n\n");
    state.listing_buffer.clear();
    Some(combined)
}

/// Вспомогательная функция: забирает склеенное сообщение из volatility-буфера.
#[inline]
fn take_volatility(state: &mut ChatState) -> Option<String> {
    if state.volatility_buffer.is_empty() {
        return None;
    }
    let combined = state.volatility_buffer.join("\n\n");
    state.volatility_buffer.clear();
    Some(combined)
}

/// Вспомогательная функция: возвращает текст обратно в буфер листингов
/// (после неудачной отправки склеенного сообщения).
#[inline]
fn push_back_listing(state: &mut ChatState, combined: &str) {
    // Разбиваем склеенное сообщение обратно на отдельные алерты.
    for line in combined.split("\n\n") {
        if !line.is_empty() {
            state.listing_buffer.push(line.to_string());
        }
    }
}

/// Вспомогательная функция: возвращает текст обратно в volatility-буфер.
#[inline]
fn push_back_volatility(state: &mut ChatState, combined: &str) {
    for line in combined.split("\n\n") {
        if !line.is_empty() {
            state.volatility_buffer.push(line.to_string());
        }
    }
}

/// Парсит `retry_after` из ответа Telegram 429, возвращает секунды.
fn extract_retry_after(s: &str) -> Option<u64> {
    let marker = "\"retry_after\":";
    let pos = s.find(marker)?;
    let after = &s[pos + marker.len()..];
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bot_pool_returns_same_instance_for_same_token() {
        let pool = BotPool::new();
        let bot1 = pool.get_or_create("token_abc");
        let bot2 = pool.get_or_create("token_abc");
        // Arc::ptr_eq проверяет что это один и тот же Arc
        assert!(Arc::ptr_eq(&bot1, &bot2));
    }

    #[test]
    fn bot_pool_returns_different_instances_for_different_tokens() {
        let pool = BotPool::new();
        let bot1 = pool.get_or_create("token_abc");
        let bot2 = pool.get_or_create("token_xyz");
        assert!(!Arc::ptr_eq(&bot1, &bot2));
    }

    #[test]
    fn bot_pool_cleanup_removes_inactive_tokens() {
        let pool = BotPool::new();
        let _ = pool.get_or_create("token_abc");
        let _ = pool.get_or_create("token_xyz");
        assert_eq!(pool.bots.len(), 2);

        let mut active = HashSet::new();
        active.insert("token_abc".to_string());
        pool.cleanup(&active);
        assert_eq!(pool.bots.len(), 1);
        assert!(pool.bots.contains_key("token_abc"));
        assert!(!pool.bots.contains_key("token_xyz"));
    }

    #[test]
    fn bot_pool_cleanup_with_empty_set_removes_all() {
        let pool = BotPool::new();
        let _ = pool.get_or_create("token_abc");
        let _ = pool.get_or_create("token_xyz");
        let active = HashSet::new();
        pool.cleanup(&active);
        assert_eq!(pool.bots.len(), 0);
    }

    #[test]
    fn extract_retry_after_parses_seconds() {
        assert_eq!(
            extract_retry_after(r#"{"ok":false,"retry_after":30}"#),
            Some(30)
        );
        assert_eq!(
            extract_retry_after(r#"{"ok":false,"retry_after":5}"#),
            Some(5)
        );
    }

    #[test]
    fn extract_retry_after_returns_none_for_missing() {
        assert_eq!(
            extract_retry_after(r#"{"ok":false,"error":"bad request"}"#),
            None
        );
        assert_eq!(extract_retry_after("not json"), None);
    }

    #[test]
    fn extract_retry_after_returns_none_for_zero_or_invalid() {
        // 0 retry_after - Telegram так не шлёт, но мы парсим как 0
        assert_eq!(extract_retry_after(r#"{"retry_after":0}"#), Some(0));
        // Нецифры
        assert_eq!(extract_retry_after(r#"{"retry_after":"abc"}"#), None);
    }

    #[test]
    fn is_network_error_detects_common_network_failures() {
        assert!(TgBot::is_network_error("operation timed out"));
        assert!(TgBot::is_network_error("connection refused"));
        assert!(TgBot::is_network_error("connection reset by peer"));
        assert!(TgBot::is_network_error("dns lookup failed"));
        assert!(TgBot::is_network_error("tls handshake error"));
        assert!(TgBot::is_network_error("hyper io error"));
    }

    #[test]
    fn is_network_error_returns_false_for_non_network() {
        assert!(!TgBot::is_network_error("400 Bad Request"));
        assert!(!TgBot::is_network_error("401 Unauthorized"));
        assert!(!TgBot::is_network_error("chat not found"));
    }

    #[test]
    fn compute_network_cooldown_grows_exponentially() {
        let c0 = TgBot::compute_network_cooldown(0);
        let c1 = TgBot::compute_network_cooldown(1);
        let c2 = TgBot::compute_network_cooldown(2);
        assert!(c0 < c1);
        assert!(c1 < c2);
    }

    #[test]
    fn compute_network_cooldown_capped_at_max() {
        let huge = TgBot::compute_network_cooldown(100);
        // 300s cap - проверяем что не превысило
        assert!(huge <= Duration::from_secs(300));
    }

    #[test]
    fn chat_state_buffers_listings_and_volatility_separately() {
        let mut s = ChatState::new();
        s.buffer_alert("listing 1", true);
        s.buffer_alert("listing 2", true);
        s.buffer_alert("volatility 1", false);
        s.buffer_alert("volatility 2", false);
        s.buffer_alert("volatility 3", false);

        assert_eq!(s.listing_buffer.len(), 2);
        assert_eq!(s.volatility_buffer.len(), 3);
        assert!(s.has_buffer());

        let (listings, volatility) = s.take_buffers();
        assert_eq!(listings.as_deref(), Some("listing 1\n\nlisting 2"));
        assert_eq!(
            volatility.as_deref(),
            Some("volatility 1\n\nvolatility 2\n\nvolatility 3")
        );
        assert!(!s.has_buffer());
    }

    #[test]
    fn chat_state_take_buffers_returns_none_when_empty() {
        let mut s = ChatState::new();
        let (l, v) = s.take_buffers();
        assert!(l.is_none());
        assert!(v.is_none());
    }

    #[test]
    fn chat_state_take_listing_only_returns_listings() {
        let mut s = ChatState::new();
        s.buffer_alert("listing 1", true);
        s.buffer_alert("volatility 1", false);

        let listing = take_listing(&mut s);
        assert_eq!(listing.as_deref(), Some("listing 1"));
        // Volatility остаётся в буфере.
        assert!(s.volatility_buffer.len() == 1);
        assert!(s.listing_buffer.is_empty());

        let vol = take_volatility(&mut s);
        assert_eq!(vol.as_deref(), Some("volatility 1"));
    }

    #[test]
    fn chat_state_push_back_listing_restores_buffer() {
        let mut s = ChatState::new();
        s.buffer_alert("listing 1", true);
        s.buffer_alert("listing 2", true);

        let combined = take_listing(&mut s).unwrap();
        assert!(s.listing_buffer.is_empty());

        push_back_listing(&mut s, &combined);
        assert_eq!(s.listing_buffer.len(), 2);
        assert_eq!(s.listing_buffer[0], "listing 1");
        assert_eq!(s.listing_buffer[1], "listing 2");
    }

    #[test]
    fn chat_state_push_back_volatility_restores_buffer() {
        let mut s = ChatState::new();
        s.buffer_alert("vol 1", false);
        s.buffer_alert("vol 2", false);

        let combined = take_volatility(&mut s).unwrap();
        assert!(s.volatility_buffer.is_empty());

        push_back_volatility(&mut s, &combined);
        assert_eq!(s.volatility_buffer.len(), 2);
    }
}
