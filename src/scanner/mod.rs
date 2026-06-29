//! Сканер: обработка трейдов, агрегация свечей, генерация алертов.
//!
//! # Подмодули
//!
//! * [`core`] - ядро сканера с параллельной обработкой через rayon.
//! * [`metrics`] - thread-local метрики с агрегацией.
//! * [`processor`] - связка broadcast-канала с ядром сканера.

pub mod core;
pub mod metrics;
pub mod processor;

pub use core::{Alert, Candle, ScannerConfig, ScannerCore, ScannerStats};
pub use metrics::Metrics;
pub use processor::TradeProcessor;
