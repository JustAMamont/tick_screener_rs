//! Конфигурация приложения: парсинг, hot-reload, diff.
//!
//! # Структура
//!
//! * [`model`] - типы данных: `RawConfig`, `ScannerRuntimeConfig`, `FeedKey`.
//! * [`watcher`] - файловый вотчер + парсинг JSON.
//! * [`registry`] - текущий снэпшот + вычисление diff для hot-reload.

pub mod model;
pub mod registry;
pub mod watcher;

pub use model::*;
pub use registry::ConfigRegistry;
pub use watcher::ConfigWatcher;
