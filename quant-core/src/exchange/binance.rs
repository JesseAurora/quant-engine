use crate::exchange::errors::ExchangeError;
use crate::exchange::traits::Exchange;
use crate::exchange::types::{BinanceStreamMessage, BinanceSubscribeMessage, BinanceTradeMessage};
use crate::exchange::utils::{build_binance_trade_streams, convert_binance_to_tick_data};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use quant_common::data::types::TickData;
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tracing::log::{debug, error, info};
use tracing::warn;

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/stream";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

pub struct BinanceExchange {
    ws_url: String,
}

impl BinanceExchange {
    pub fn new() -> Self {
        Self {
            ws_url: BINANCE_WS_URL.to_string(),
        }
    }

    fn parse_trade_message(&self, text: &str) -> Result<TickData, ExchangeError> {
        if let Ok(stream_msg) = serde_json::from_str::<BinanceStreamMessage>(&text) {
            return convert_binance_to_tick_data(stream_msg.data);
        }

        if let Ok(trade_msg) = serde_json::from_str::<BinanceTradeMessage>(text) {
            return convert_binance_to_tick_data(trade_msg);
        }
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
            if value.get("result").is_some() || value.get("id").is_some() {
                // This is a subscription confirmation, not an error
                debug!("Received subscription confirmation: {}", text);
                return Err(ExchangeError::ParseError(
                    "Control message, not trade data".to_string(),
                ));
            }
        }

        Err(ExchangeError::ParseError(format!(
            "Unable to parse message: {}",
            text
        )))
    }

    async fn handle_websocket_connection(
        &self,
        symbols: &[String],
        callback: Box<dyn Fn(TickData) + Send + Sync>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), ExchangeError> {
        let streams = build_binance_trade_streams(symbols)?;
        info!(
            "Connecting to Binance WebSocket with {} streams",
            streams.len()
        );

        let mut reconnect_attempts = 0;
        const MAX_RECONNECT_ATTEMPTS: u32 = 10;

        loop {
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutting down Binance WebSocket connection");
                return Ok(());
            }

            match self
                .connect_and_subscribe(&streams, &callback, shutdown_rx.resubscribe())
                .await
            {
                Ok(()) => {
                    info!(
                        "WebSocket connection ended normally - checking if shutdown was requested"
                    );

                    return Ok(());
                }
                Err(e) => {
                    reconnect_attempts += 1;
                    error!(
                        "WebSocket connection failed (attempt {}): {}",
                        reconnect_attempts, e
                    );

                    if reconnect_attempts >= MAX_RECONNECT_ATTEMPTS {
                        return Err(ExchangeError::NetworkError(format!(
                            "Max reconnection attempts ({}) exceeded",
                            MAX_RECONNECT_ATTEMPTS
                        )));
                    }

                    warn!("Attempting to reconnect in {:?}...", RECONNECT_DELAY);

                    // Wait for reconnect delay or shutdown signal
                    tokio::select! {
                        _ = sleep(RECONNECT_DELAY) => {
                            // Continue to retry
                            continue;
                        }
                        _ = shutdown_rx.recv() => {
                            info!("Shutdown signal received during reconnect delay");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn connect_and_subscribe(
        &self,
        streams: &[String],
        callback: &Box<dyn Fn(TickData) + Send + Sync>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), ExchangeError> {
        let (ws_stream, _) = connect_async(&self.ws_url)
            .await
            .map_err(|e| ExchangeError::WebSocketError(format!("{:?}", e)))?;

        debug!("WebSocket connected to {}", self.ws_url);

        let (mut write, mut read) = ws_stream.split();

        let subscribe_msg = BinanceSubscribeMessage::new(streams.to_vec());
        let subscribe_json = serde_json::to_string(&subscribe_msg).map_err(|e| {
            ExchangeError::ParseError(format!("Failed to serialize subscription: {}", e))
        })?;

        write
            .send(Message::Text(Utf8Bytes::from(subscribe_json)))
            .await
            .map_err(|e| ExchangeError::WebSocketError(format!("{:?}", e)))?;
        info!("Subscription sent for {} streams", streams.len());

        // Message processing loop
        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            match self.parse_trade_message(&text) {
                                Ok(tick_data) => callback(tick_data),
                                Err(e) => warn!("Parse error: {}", e),
                            }
                        }
                        Some(Ok(Message::Ping(ping))) => {
                            write.send(Message::Pong(ping)).await?;
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("WebSocket closed by server");
                            break;
                        }
                        Some(Err(e)) => {
                            return Err(ExchangeError::WebSocketError(e.to_string()));
                        }
                        None => {
                            info!("WebSocket stream ended");
                            break;
                        }
                        _ => continue,
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, closing WebSocket gracefully");
                    // Send Close frame to server
                    if let Err(e) = write.send(Message::Close(None)).await {
                        warn!("Failed to send close frame: {}", e);
                    }
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Exchange for BinanceExchange {
    async fn subscribe_trades(
        &self,
        symbols: &[String],
        callback: Box<dyn Fn(TickData) + Send + Sync>,
        shutdown_rx: Receiver<()>,
    ) -> Result<(), ExchangeError> {
        if symbols.is_empty() {
            return Err(ExchangeError::InvalidSymbol(
                "no symbols provided".to_string(),
            ));
        }

        info!(
            "Starting Binance trade subscription for symbols: {:?}",
            symbols
        );

        self.handle_websocket_connection(symbols, callback, shutdown_rx.resubscribe())
            .await
    }
}

impl Default for BinanceExchange {
    fn default() -> Self {
        Self::new()
    }
}
