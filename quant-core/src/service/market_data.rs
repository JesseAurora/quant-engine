use crate::exchange::traits::Exchange;
use crate::live_trading::paper_trading::PaperTradingProcessor;
use crate::service::errors::ServiceError;
use crate::service::types::{BatchConfig, BatchStats};
use log::{error, warn};
use quant_common::data::cache::TickDataCache;
use quant_common::data::repository::TickDataRepository;
use quant_common::data::types::TickData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tokio::{select, spawn};
use tracing::debug;
use tracing::log::info;

pub struct MarketDataService {
    exchange: Arc<dyn Exchange>,
    repository: Arc<TickDataRepository>,
    symbols: Vec<String>,
    batch_config: BatchConfig,
    shutdown_tx: broadcast::Sender<()>,
    // AtomicU64 refine
    stats: Arc<Mutex<BatchStats>>,
    paper_trading: Option<Arc<Mutex<PaperTradingProcessor>>>,
}

impl MarketDataService {
    pub fn new(
        exchange: Arc<dyn Exchange>,
        repository: Arc<TickDataRepository>,
        symbols: Vec<String>,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(16);
        Self {
            exchange,
            repository,
            symbols,
            batch_config: BatchConfig::default(),
            shutdown_tx,
            stats: Arc::new(Mutex::new(BatchStats::default())),
            paper_trading: None,
        }
    }

    pub fn with_paper_trading(mut self, paper_trading: Arc<Mutex<PaperTradingProcessor>>) -> Self {
        self.paper_trading = Some(paper_trading);
        self
    }

    pub fn get_shutdown_tx(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    pub async fn start(&self) -> Result<(), ServiceError> {
        if self.symbols.is_empty() {
            return Err(ServiceError::Config("Missing symbols".to_string()));
        }

        info!(
            "Starting market data service for symbols: {:?}",
            self.symbols
        );

        let (tick_tx, tick_rx) = mpsc::channel::<TickData>(1000);
        let collection_task = self.start_data_collection(tick_tx).await?;
        let processing_task = self.start_data_processing(tick_rx).await?;

        let result = tokio::try_join!(collection_task, processing_task);

        match result {
            Ok(_) => {
                info!("Market data service stopped successfully");
                Ok(())
            }
            Err(e) => Err(ServiceError::Task(format!("task failed: {}", e))),
        }
    }

    async fn start_data_collection(
        &self,
        tick_tx: mpsc::Sender<TickData>,
    ) -> Result<tokio::task::JoinHandle<()>, ServiceError> {
        let exchange = Arc::clone(&self.exchange);
        let symbols = self.symbols.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let handle = spawn(async move {
            loop {
                if shutdown_rx.try_recv().is_ok() {
                    info!("Data collection shutdown requested before connection attempt");
                    break;
                }
                let tick_tx_clone = tick_tx.clone();

                // cant lose data
                // let callback = Box::new(move |tick: TickData| {
                //     let tx = tick_tx_clone.clone();
                //     spawn(async move {
                //         if let Err(e) = tx.send(tick).await {
                //             if !e.to_string().contains("channel closed") {
                //                 error!("Failed to send tick data to processing pipeline: {}", e);
                //             }
                //         }
                //     });
                // });

                // faster
                let callback = Box::new(move |tick: TickData| match tick_tx_clone.try_send(tick) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!("Tick processing pipeline is full, dropping tick");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Expected during shutdown; the receiver has already been dropped.
                    }
                });

                match exchange
                    // resubribe kind of clone here
                    .subscribe_trades(&symbols, callback, shutdown_rx.resubscribe())
                    .await
                {
                    Ok(_) => {
                        info!("exchange subscription completed normally");
                        break;
                    }
                    Err(e) => {
                        error!("Failed to subscribe trades: {}", e);
                        if shutdown_rx.try_recv().is_ok() {
                            info!("data collection shutdown requested, cancelling retry");
                            break;
                        }
                        warn!("Retrying exchange connection in 5 seconds....");
                        select! {
                        _ = sleep(Duration::from_secs(5)) => {
                        continue;
                        }
                        _ = shutdown_rx.recv() => {
                        info ! ("exchange connection shutdown requested, shutting down");
                        break;
                        }
                        }
                    }
                }
            }
            info!("data collection shutdown complete");
        });

        Ok(handle)
    }

    async fn start_data_processing(
        &self,
        mut tick_rx: mpsc::Receiver<TickData>,
    ) -> Result<JoinHandle<()>, ServiceError> {
        let repository = Arc::clone(&self.repository);
        let batch_config = self.batch_config.clone();
        let stats = Arc::clone(&self.stats);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let paper_trading = self.paper_trading.clone();

        let handle = spawn(async move {
            let mut batch_buffer = Vec::with_capacity(batch_config.max_batch_size);
            // let mut last_flush = Instant::now();
            let mut flush_timer = interval(Duration::from_secs(batch_config.max_batch_time));

            // rewritten
            loop {
                select! {
                    tick_opt = tick_rx.recv() => {
                        match tick_opt {
                            Some(tick) => {
                                // Self::update_cache_async(&repository, &tick, &stats).await;
                                let repo_clone = repository.clone();
                                let stats_clone = stats.clone();
                                let tick_clone = tick.clone();

                                tokio::spawn(async move {
                                    Self::update_cache_async(&repo_clone, &tick_clone, &stats_clone).await;
                                });

                                if let Some(paper_trading_processor) = &paper_trading {
                                    let mut processor = paper_trading_processor.lock().await;
                                    if let Err(e) = processor.process_tick(&tick).await {
                                        warn!("Failed to process tick: {}", e);
                                    }
                                }

                                batch_buffer.push(tick);

                                {
                                    let mut s = stats.lock().await;
                                    s.total_ticks_processed += 1;
                                }

                                if batch_buffer.len() >= batch_config.max_batch_size {
                                    Self::flush_batch_with_retry(
                                        &repository,
                                        &mut batch_buffer,
                                        &batch_config,
                                        &stats,
                                    ).await;

                                    // last_flush = Instant::now();
                                    // when full reset alarm
                                    flush_timer.reset();
                                }
                            }
                            None => {
                                warn!("Data collection shutting down");
                                break;
                            }
                        }
                    }
                    _ = flush_timer.tick() => {
                        // if !batch_buffer.is_empty() && last_flush.elapsed() >= Duration::from_secs(batch_config.max_batch_time) {
                        // alarm on time wake dont have to check time again
                        if !batch_buffer.is_empty() {
                            debug!("Time-based batch flush triggered (batch size: {})", batch_buffer.len());

                            Self::flush_batch_with_retry(
                                &repository,
                                &mut batch_buffer,
                                &batch_config,
                                &stats,
                            ).await;
                            // last_flush = Instant::now();
                            // alarm auto reset after trigger
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Processing shutdown requested, flushing remaining data");
                        if !batch_buffer.is_empty() {
                            Self::flush_batch_with_retry(
                                &repository,
                                &mut batch_buffer,
                                &batch_config,
                                &stats,
                            ).await;
                        }
                        break;
                    }
                }
            }

            info!("Data processing stopped");
        });
        Ok(handle)
    }

    async fn update_cache_async(
        repository: &TickDataRepository,
        tick: &TickData,
        stats: &Arc<Mutex<BatchStats>>,
    ) {
        if let Err(e) = repository.get_cache().push_tick(tick).await {
            warn!("Failed to update ticker cache: {} {}", tick.trade_id, e);

            {
                let mut s = stats.lock().await;
                s.cache_update_failures += 1;
            }
        } else {
            debug!("cache updated for symbol:{}", tick.symbol);
        }
    }

    async fn flush_batch_with_retry(
        repository: &TickDataRepository,
        batch_buffer: &mut Vec<TickData>,
        config: &BatchConfig,
        stats: &Arc<Mutex<BatchStats>>,
    ) {
        if batch_buffer.is_empty() {
            return;
        }

        let batch_size = batch_buffer.len();
        let mut attempt = 0;

        loop {
            match repository.batch_insert(batch_buffer.clone()).await {
                Ok(inserted_count) => {
                    info!(
                        "Successfully flushed batch: {} ticks inserted",
                        inserted_count
                    );
                    {
                        let mut s = stats.lock().await;
                        s.total_batches_flushed += 1;
                        s.last_flush_time = Some(chrono::Utc::now())
                    }
                    batch_buffer.clear();
                    break;
                }
                Err(e) => {
                    attempt += 1;
                    error!(
                        "batch insert failed (attempt {}/{}):{}",
                        attempt, config.max_retry_attempts, e
                    );
                    {
                        let mut s = stats.lock().await;
                        s.total_retry_attempts += 1;
                    }
                    if attempt >= config.max_retry_attempts {
                        error!(
                            "batch insert failed after {} attempts,discarding {} ticks",
                            config.max_retry_attempts, batch_size
                        );

                        {
                            let mut s = stats.lock().await;
                            s.total_failed_batches += 1;
                        }
                        batch_buffer.clear();
                        break;
                    }
                    warn!("Retrying batch insert in {}ms...", config.retry_delay_ms);
                    sleep(Duration::from_millis(config.retry_delay_ms)).await;
                }
            }
        }
    }
}
