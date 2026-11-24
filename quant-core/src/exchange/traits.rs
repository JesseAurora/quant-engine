use crate::exchange::errors::ExchangeError;
use async_trait::async_trait;
use quant_common::data::types::TickData;

#[async_trait]
pub trait Exchange: Send + Sync {
    async fn subscribe_trades(
        &self,
        symbols: &[String],
        callback: Box<dyn Fn(TickData) + Send + Sync>,
        shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), ExchangeError>;
}
