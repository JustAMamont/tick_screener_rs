//! Коннекторы к биржам.
//!
//! Каждый коннектор реализует [`ExchangeConnector`] и отвечает за:
//!
//! * Загрузку рынков через REST API (`load_markets`).
//! * Подключение к WebSocket и стриминг трейдов (`stream_trades`).
//! * Конвертацию между unified-форматом символов (`BTC/USDT`) и
//!   native-форматом биржи (`BTCUSDT`).
//!
//! # Поддерживаемые биржи
//!
//! | Биржа   | Spot | Perp |
//! |---------|------|------|
//! | Binance |  ✓   |  ✓   |
//! | Bybit   |  ✓   |  ✓   |
//! | Kucoin  |  ✓   |  ✓   |
//! | Bitget  |  ✓   |  ✓   |
//! | Gate    |  ✓   |  ✓   |
//! | MEXC    |  ✓   |  ✓   |

pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod connector;
pub mod gate;
pub mod kucoin;
pub mod mexc;
pub mod normalized;

pub use connector::ExchangeConnector;
pub use normalized::Exchange;
pub use normalized::NormalizedTrade;

/// Простой XORSHIFT генератор псевдослучайных чисел.
///
/// Используется для jitter-а в retry-логике: не нужен
/// криптографический RNG, важна только скорость.
pub fn rand_int() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let mut x = nanos as u64;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    x
}
