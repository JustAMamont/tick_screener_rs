//! Алерты: роутинг и доставка в Telegram.
//!
//! * [`router::AlertRouter`] - mpsc → Telegram, один экземпляр на приложение.
//! * [`telegram::TgBot`] - отдельный бот с per-chat rate limit и буферизацией.
//! * [`telegram::BotPool`] - дедупликация ботов по токену.

pub mod router;
pub mod telegram;

pub use router::AlertRouter;
pub use telegram::{BotPool, TgBot};
