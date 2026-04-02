pub mod connector;
pub mod normalized;
pub mod binance;
pub mod bybit;
pub mod kucoin;
pub mod bitget;
pub mod gate;
pub mod mexc;

pub use connector::ExchangeConnector;
pub use normalized::Exchange;
pub use normalized::NormalizedTrade;

pub fn rand_int() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos();
    let mut x = nanos;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    x.into()
}
