//! Роутер алертов: доставляет алерты из mpsc-канала в Telegram.
//!
//! Работает как одиночный tokio-таск. Читает `(scanner_id, Alert)` из
//! канала, находит конфиг сканера, берёт `TgBot` из пула и отправляет
//! сообщение. При 429/network-ошибках `TgBot` сам буферизирует
//! сообщение и повторяет позже.

use crate::alert::telegram::BotPool;
use crate::scanner::core::Alert;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::{error, info, warn};

/// Роутер алертов: mpsc → Telegram.
///
/// Один экземпляр на всё приложение. Запускается через [`Self::run`].
pub struct AlertRouter {
    /// Пул Telegram-ботов (дедуплицированный по токену).
    bot_pool: BotPool,
    /// Приёмник алертов из mpsc-канала.
    alert_rx: mpsc::Receiver<(String, Alert)>,
    /// Конфиги сканеров (для поиска Telegram-настроек по scanner_id).
    scanner_configs: Arc<RwLock<Vec<crate::config::model::ScannerRuntimeConfig>>>,
}

impl AlertRouter {
    /// Создаёт новый роутер.
    pub fn new(
        bot_pool: BotPool,
        alert_rx: mpsc::Receiver<(String, Alert)>,
        scanner_configs: Arc<RwLock<Vec<crate::config::model::ScannerRuntimeConfig>>>,
    ) -> Self {
        Self {
            bot_pool,
            alert_rx,
            scanner_configs,
        }
    }

    /// Главный цикл роутинга. Запускается как одиночный tokio-таск.
    ///
    /// # Алгоритм
    ///
    /// 1. Читаем `(scanner_id, alert)` из mpsc-канала.
    /// 2. Ищем конфиг сканера по `scanner_id`.
    /// 3. Если у сканера есть `bot_token` - берём `TgBot` из пула
    ///    и отправляем сообщение (с учётом rate limit-ов).
    /// 4. Канал закрывается когда все `alert_tx` дропнуты →
    ///    `recv()` вернёт `None` → цикл завершается.
    pub async fn run(mut self) {
        info!("AlertRouter started");

        while let Some((scanner_id, alert)) = self.alert_rx.recv().await {
            let config = {
                let configs = self.scanner_configs.read().await;
                configs.iter().find(|c| c.scanner_id == scanner_id).cloned()
            };

            let Some(config) = config else {
                warn!("No config found for scanner: {}", scanner_id);
                continue;
            };

            let tg = &config.alert_settings.telegram;
            if tg.bot_token.is_empty() {
                continue;
            }

            let bot = self.bot_pool.get_or_create(&tg.bot_token);

            if let Err(e) = bot
                .send_message(tg.chat_id, &alert.message, alert.pin)
                .await
            {
                error!(
                    "Failed to send alert for scanner {} to chat {}: {}",
                    scanner_id, tg.chat_id, e
                );
            }
        }

        info!("AlertRouter stopped");
    }

    /// Обновляет конфиги (вызывается при hot-reload).
    ///
    /// Очищает пул ботов от тех, чьи токены больше не используются.
    pub async fn update_configs(
        &self,
        configs: Arc<RwLock<Vec<crate::config::model::ScannerRuntimeConfig>>>,
    ) {
        let tokens: HashSet<String> = {
            let guard = configs.read().await;
            guard
                .iter()
                .map(|c| c.alert_settings.telegram.bot_token.clone())
                .filter(|t| !t.is_empty())
                .collect()
        };
        self.bot_pool.cleanup(&tokens);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MarketType;
    use crate::config::model::{
        AlertSettings, ProcessSettings, ScannerRuntimeConfig, TelegramSettings,
    };
    use crate::exchanges::Exchange;
    use std::collections::HashSet;

    fn make_scanner_config(id: &str, token: &str) -> ScannerRuntimeConfig {
        ScannerRuntimeConfig {
            scanner_id: id.to_string(),
            exchange: Exchange::Bybit,
            market_type: MarketType::Spot,
            quote: "USDT".to_string(),
            quote_aliases: vec!["USDT".to_string()],
            blacklist: HashSet::new(),
            alert_settings: AlertSettings {
                return_limit: 1.0,
                volume_limit: 1000.0,
                trange: 60,
                telegram: TelegramSettings {
                    bot_token: token.to_string(),
                    chat_id: -100,
                },
                delimiter: "".to_string(),
            },
            process_settings: ProcessSettings {
                pairs_batch_size: 100,
                launch_delay: 1.0,
            },
        }
    }

    #[tokio::test]
    async fn router_processes_alerts_until_channel_closed() {
        let bot_pool = BotPool::new();
        let (tx, rx) = mpsc::channel(8);
        let configs = Arc::new(RwLock::new(vec![make_scanner_config(
            "bybit_spot",
            "test_token",
        )]));
        let router = AlertRouter::new(bot_pool, rx, configs);

        // Отправляем алерт и закрываем канал
        let alert = Alert {
            symbol: "BTCUSDT".to_string(),
            ts: 12345,
            message: "test alert".to_string(),
            alert_type: "volatility".to_string(),
            pin: false,
        };
        tx.send(("bybit_spot".to_string(), alert)).await.unwrap();
        drop(tx);

        // Роутер должен завершиться (без паники) после закрытия канала
        router.run().await;
        // Тест считается пройденным, если не было паники
    }

    #[tokio::test]
    async fn router_skips_scanner_with_empty_token() {
        let bot_pool = BotPool::new();
        let (tx, rx) = mpsc::channel(8);
        let configs = Arc::new(RwLock::new(vec![make_scanner_config("bybit_spot", "")]));
        let router = AlertRouter::new(bot_pool, rx, configs);

        let alert = Alert {
            symbol: "BTCUSDT".to_string(),
            ts: 12345,
            message: "test alert".to_string(),
            alert_type: "volatility".to_string(),
            pin: false,
        };
        tx.send(("bybit_spot".to_string(), alert)).await.unwrap();
        drop(tx);

        // Роутер не должен крашиться при пустом токене
        router.run().await;
    }

    #[tokio::test]
    async fn router_handles_unknown_scanner_id() {
        let bot_pool = BotPool::new();
        let (tx, rx) = mpsc::channel(8);
        let configs = Arc::new(RwLock::new(vec![]));
        let router = AlertRouter::new(bot_pool, rx, configs);

        let alert = Alert {
            symbol: "BTCUSDT".to_string(),
            ts: 12345,
            message: "test alert".to_string(),
            alert_type: "volatility".to_string(),
            pin: false,
        };
        tx.send(("unknown_scanner".to_string(), alert))
            .await
            .unwrap();
        drop(tx);

        // Не должно быть паники для неизвестного сканера
        router.run().await;
    }
}
