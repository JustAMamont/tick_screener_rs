//! Трейт `ExchangeConnector` - общий интерфейс коннекторов к биржам.

use crate::config::MarketType;
use crate::exchanges::normalized::{Exchange, MarketInfo, NormalizedTrade};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Тип фабричной функции для создания коннекторов бирж.
pub type ConnectorFactory = fn(MarketType) -> Box<dyn ExchangeConnector>;

/// Трейт, который должен реализовать каждый коннектор к бирже.
///
/// # Реализация
///
/// Все методы асинхронные (`async_trait`). Коннектор должен быть
/// `Send + Sync` для использования из многопоточного tokio-рантайма.
///
/// # Жизненный цикл
///
/// 1. `new(market_type)` - создаёт коннектор.
/// 2. `load_markets()` - один раз при подписке для получения списка пар.
/// 3. `stream_trades()` - запускается для каждого батча символов,
///    работает до отмены или фатальной ошибки.
#[async_trait]
pub trait ExchangeConnector: Send + Sync {
    /// Биржа, обслуживаемая этим коннектором.
    fn exchange(&self) -> Exchange;

    /// Тип рынка (spot/perp).
    fn market_type(&self) -> MarketType;

    /// Загружает все доступные рынки через REST API.
    ///
    /// Возвращает список [`MarketInfo`] для дальнейшей фильтрации
    /// по котировке, типу и т.д.
    async fn load_markets(&self) -> anyhow::Result<Vec<MarketInfo>>;

    /// Подключается к WebSocket и стримит нормализованные трейды.
    ///
    /// Отправляет трейды через broadcast-канал. Работает до отмены
    /// через `cancel` или возникновения фатальной ошибки.
    async fn stream_trades(
        &self,
        symbols: Vec<String>,
        tx: tokio::sync::broadcast::Sender<NormalizedTrade>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>;

    /// Преобразует unified-символ (`BTC/USDT`) в native-формат биржи (`BTCUSDT`).
    fn to_native_symbol(&self, unified: &str) -> String;

    /// Преобразует native-символ биржи обратно в unified-формат.
    fn to_unified_symbol(&self, native: &str) -> Option<String>;

    /// Максимальное количество аргументов в одном WebSocket SUBSCRIBE.
    /// `0` = без лимита (например, Bybit perp).
    fn max_subscribe_args(&self) -> usize;
}

/// Возвращает фабрику коннекторов для заданной биржи.
pub fn get_connector_factory(exchange: Exchange) -> ConnectorFactory {
    match exchange {
        Exchange::Binance => |mt| Box::new(crate::exchanges::binance::BinanceConnector::new(mt)),
        Exchange::Bybit => |mt| Box::new(crate::exchanges::bybit::BybitConnector::new(mt)),
        Exchange::Kucoin => |mt| Box::new(crate::exchanges::kucoin::KucoinConnector::new(mt)),
        Exchange::Bitget => |mt| Box::new(crate::exchanges::bitget::BitgetConnector::new(mt)),
        Exchange::Gate => |mt| Box::new(crate::exchanges::gate::GateConnector::new(mt)),
        Exchange::Mexc => |mt| Box::new(crate::exchanges::mexc::MexcConnector::new(mt)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_connector_factory_returns_for_each_exchange() {
        // Просто проверяем что фабрика не паникует для каждой биржи.
        let _ = get_connector_factory(Exchange::Binance)(MarketType::Spot);
        let _ = get_connector_factory(Exchange::Bybit)(MarketType::Perp);
        let _ = get_connector_factory(Exchange::Kucoin)(MarketType::Spot);
        let _ = get_connector_factory(Exchange::Bitget)(MarketType::Perp);
        let _ = get_connector_factory(Exchange::Gate)(MarketType::Spot);
        let _ = get_connector_factory(Exchange::Mexc)(MarketType::Perp);
    }

    #[test]
    fn connector_factory_returns_correct_exchange() {
        let c = get_connector_factory(Exchange::Binance)(MarketType::Spot);
        assert_eq!(c.exchange(), Exchange::Binance);
        assert_eq!(c.market_type(), MarketType::Spot);

        let c = get_connector_factory(Exchange::Bybit)(MarketType::Perp);
        assert_eq!(c.exchange(), Exchange::Bybit);
        assert_eq!(c.market_type(), MarketType::Perp);
    }
}
