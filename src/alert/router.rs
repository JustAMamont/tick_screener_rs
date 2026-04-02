use crate::alert::telegram::BotPool;
use crate::scanner::core::Alert;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

/// Routes alerts from scanners to their configured Telegram bots.
pub struct AlertRouter {
    bot_pool: BotPool,
    alert_rx: mpsc::Receiver<(String, Alert)>,
    scanner_configs: Arc<RwLock<Vec<crate::config::model::ScannerRuntimeConfig>>>,
}

impl AlertRouter {
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

    /// Run the alert routing loop. Should be spawned as a single tokio task.
    pub async fn run(mut self) {
        info!("AlertRouter started");

        while let Some((scanner_id, alert)) = self.alert_rx.recv().await {
            let config = {
                let configs = self.scanner_configs.read().await;
                configs
                    .iter()
                    .find(|c| c.scanner_id == scanner_id)
                    .cloned()
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

            if let Err(e) = bot.send_message(tg.chat_id, &alert.message, alert.pin).await {
                error!(
                    "Failed to send alert for scanner {} to chat {}: {}",
                    scanner_id, tg.chat_id, e
                );
            }
        }

        info!("AlertRouter stopped");
    }

    /// Update scanner configs (called on config reload)
    pub async fn update_configs(
        &self,
        configs: Arc<RwLock<Vec<crate::config::model::ScannerRuntimeConfig>>>,
    ) {
        // Collect active tokens
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