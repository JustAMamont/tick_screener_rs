//! # tick-screener
//!
//! Высокопроизводительный сканер аномалий торговых пар для нескольких
//! криптобирж. Подписывается на WebSocket-стримы сделок (trades) с
//! Binance, Bybit, Kucoin, Bitget, Gate, MEXC, агрегирует их в свечи
//! заданного таймфрейма и шлёт Telegram-алерт при превышении порогов
//! по объёму и изменению цены.
//!
//! # Ключевые оптимизации
//!
//! * **Интернинг символов** ([`interner`]): имена пар (`BTC/USDT.P`)
//!   заменяются на 4-байтный [`SymbolId`](interner::SymbolId), что
//!   устраняет аллокации и ускоряет хэширование в горячем пути.
//! * **Thread-local метрики** ([`scanner::metrics`]): запись без
//!   атомарных конфликтов, агрегация раз в 60 секунд.
//! * **Параллельная обработка** ([`scanner`]): rayon-параллелизм по
//!   символам внутри батча трейдов.
//! * **Опциональный io_uring** ([`io_util`]): feature-флаг `uring`
//!   активирует io_uring-рантайм для файлового I/O (Linux 5.1+).
//! * **Hot-reload global_params** ([`logging`]): уровень логирования
//!   и срок хранения логов настраиваются в `config.json` и применяются
//!   в рантайме без перезапуска.
//!
//! # Архитектура
//!
//! ```text
//!  exchanges ─┐
//!             ↓
//!   feed::FeedManager (broadcast)
//!             ↓
//!   scanner::TradeProcessor (per-scanner)
//!             ↓
//!   scanner::ScannerCore (rayon,thread-local)
//!             ↓
//!   alert::AlertRouter → alert::telegram::TgBot → Telegram
//!
//!   config::ConfigWatcher ──→ ConfigRegistry ──→ App (hot-reload)
//!         │
//!         └──→ logging::LogRuntime (reload log level + retention)
//! ```

pub mod alert;
pub mod config;
pub mod exchanges;
pub mod feed;
pub mod interner;
pub mod io_util;
pub mod logging;
pub mod scanner;
