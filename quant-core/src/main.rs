use log::error;
use quant_common::backtest;
use quant_common::backtest::engine::{BackTestConfig, BackTestEngine};
use quant_common::backtest::strategy::{create_strategy, list_strategies};
use quant_common::data::cache::{TickDataCache, TieredCache};
use quant_common::data::repository::TickDataRepository;
use quant_core::config::Settings;
use quant_core::exchange::binance::BinanceExchange;
use quant_core::live_trading::paper_trading::PaperTradingProcessor;
use quant_core::service::market_data::MarketDataService;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::io;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install signal handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install signal handler");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("backtest") => run_backtest_mode().await,
        Some("live") => {
            if args.contains(&"--paper-trading".to_string()) {
                run_live_with_paper_trading().await
            } else {
                run_live_mode().await
            }
        }
        None => run_live_mode().await,
        Some("--help") | Some("-h") => {
            print_usage();
            Ok(())
        }
        _ => {
            eprintln!("X unknow command:{}", args[1]);
            print_usage();
            std::process::exit(1);
        }
    }
}

fn print_usage() {
    println!();
    println!("Usage:");
    println!("  cargo run                # Run live data collection");
    println!("  cargo run live           # Run live data collection");
    println!("  cargo run backtest       # Run backtesting mode");
    println!("  cargo run --help         # Show this help message");
    println!();
}

async fn run_live_with_paper_trading() -> Result<(), Box<dyn std::error::Error>> {
    init_application().await?;
    info!("Live with Paper-Trading");
    let settings = Settings::new()?;

    if !settings.paper_trading.enabled {
        warn!("⚠️ Paper trading is disabled in config. Set paper_trading.enabled = true");
        warn!("⚠️ Falling back to live data collection only...");
        return run_live_mode().await;
    }

    if backtest::strategy::get_strategy_info(&settings.paper_trading.strategy).is_none() {
        error!("unknow strategy {}", settings.paper_trading.strategy);
        error!("available strategies: rsi sma");
        std::process::exit(1);
    }

    let pool = create_database_pool(&settings).await?;
    test_database_connection(&pool).await?;
    info!("database connection established");

    let cache = create_cache(&settings).await?;

    let repository = Arc::new(TickDataRepository::new(pool, cache));

    let exchange = Arc::new(BinanceExchange::new());


    let strategy = backtest::strategy::create_strategy(&settings.paper_trading.strategy)?;

    let initial_capital = Decimal::try_from(settings.paper_trading.initial_capital)
        .map_err(|e| format!("initial capital overflow: {}", e))?;

    let paper_trading = Arc::new(tokio::sync::Mutex::new(PaperTradingProcessor::new(
        strategy,
        Arc::clone(&repository),
        initial_capital,
    )));

    let service = MarketDataService::new(exchange, repository, settings.symbols.clone())
        .with_paper_trading(paper_trading);

    run_live_application_with_service(service).await?;
    Ok(())
}

async fn run_live_application_with_service(
    service: MarketDataService,
) -> Result<(), Box<dyn std::error::Error>> {
    let service_shutdown_tx = service.get_shutdown_tx();

    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        println!("received shutdown signal");
        info!("shutting down ...");
        let _ = service_shutdown_tx.send(());
    });

    match service.start().await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("{}", e);
            Err(Box::new(e))
        }
    }
}

async fn run_live_mode() -> Result<(), Box<dyn std::error::Error>> {
    init_application().await?;
    info!("Live mode");
    let settings = Settings::new()?;

    run_live_application(settings).await?;

    info!("Live mode stopped");

    Ok(())
}

async fn init_application() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    // Initialize tracing/logging
    init_tracing()?;

    info!("🔧 Application environment initialized");
    Ok(())
}

fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("quant_core=info,sqlx=info,tokio=info,hyper=info"));

    // Setup tracing subscriber with structured logging
    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .compact(),
        )
        .init();

    Ok(())
}

async fn run_backtest_mode() -> Result<(), Box<dyn std::error::Error>> {
    init_application().await?;
    info!("Backtest mode");
    let settings = Settings::new()?;

    let pool = create_database_pool(&settings).await?;

    test_database_connection(&pool).await?;

    let cache = create_backtest_cache(&settings).await?;

    let repository = TickDataRepository::new(pool, cache);

    run_backtest_interactive(repository).await?;
    info!("Backtest mode stopped");
    Ok(())
}

async fn create_database_pool(settings: &Settings) -> Result<PgPool, Box<dyn std::error::Error>> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(settings.database.max_connections)
        .min_connections(settings.database.min_connections)
        .max_lifetime(Duration::from_secs(settings.database.max_lifetime))
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(600))
        .connect(&settings.database.url)
        .await?;
    Ok(pool)
}

async fn test_database_connection(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query("select 1").execute(pool).await?;

    let table_exist = sqlx::query_scalar::<_, bool>(
        "select exists
        (select from information_schema.tables where table_schema = 'public'
        and table_name = 'tick_data'
        )",
    )
    .fetch_one(pool)
    .await?;

    if !table_exist {
        error!("Database does not exist");
        std::process::exit(1);
    }

    info!("database schema validation passed");
    Ok(())
}

async fn create_cache(settings: &Settings) -> Result<TieredCache, Box<dyn std::error::Error>> {
    let memory_config = (
        settings.cache.memory.max_ticks_per_symbol,
        settings.cache.memory.ttl_seconds,
    );

    let redis_config = (
        settings.cache.redis.url.as_str(),
        settings.cache.redis.max_ticks_per_symbol,
        settings.cache.redis.ttl_seconds,
    );

    let cache = TieredCache::new(memory_config, redis_config).await?;

    test_cache_connection(&cache).await?;

    Ok(cache)
}

async fn create_backtest_cache(
    settings: &Settings,
) -> Result<TieredCache, Box<dyn std::error::Error>> {
    let memory_config = (10, 60);

    let redis_config = (settings.cache.redis.url.as_str(), 10, 60);
    let cache = TieredCache::new(memory_config, redis_config).await?;

    if let Err(e) = test_cache_connection(&cache).await {
        warn!("⚠️ Cache test failed (this is OK for backtest mode): {}", e);
    }

    Ok(cache)
}

async fn test_cache_connection(cache: &TieredCache) -> Result<(), Box<dyn std::error::Error>> {
    cache.get_symbols().await?;
    info!("✅ Cache connectivity test passed");
    Ok(())
}

async fn run_backtest_interactive(
    repository: TickDataRepository,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_info = repository.get_backtest_data_info().await?;
    if let Some(earliest) = data_info.earliest_timestamp {
        println!(
            "  Earliest Data: {}",
            earliest.format("%Y-%m-%d %H:%M:%S UTC")
        );
    }
    if let Some(latest) = data_info.latest_timestamp {
        println!("  Latest Data: {}", latest.format("%Y-%m-%d %H:%M:%S UTC"));
    }

    println!("\n Symbol Details:");
    for (i, symbol_info) in data_info.symbol_info.iter().take(10).enumerate() {
        println!(
            "  {}: {} ({} records)",
            i + 1,
            symbol_info.symbol,
            symbol_info.records_count
        );
    }

    if data_info.symbol_info.len() > 10 {
        println!(
            "  ... and {} more symbols",
            data_info.symbol_info.len() - 10
        );
    }

    println!("\n Available Strategies:");
    let strategies = list_strategies();

    for (i, strategy) in strategies.iter().enumerate() {
        println!("  {}) {} - {}", i + 1, strategy.name, strategy.description);
    }

    print!("\nSelect strategy (1-{}):", strategies.len());
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let choice = input.trim().parse().unwrap_or(0);

    if choice == 0 || choice > strategies.len() {
        println!("❌ Invalid selection");
        return Ok(());
    }

    let selected_strategy = &strategies[choice - 1];
    println!("Selected strategy: {}", selected_strategy.name);

    println!("\n Symbol Selection:");
    let available_symbols = data_info.get_available_symbols();

    for (i, symbol) in available_symbols.iter().take(10).enumerate() {
        let symbol_info = data_info.get_symbol_info(symbol).unwrap();
        println!(
            "  {}) {} ({} records)",
            i + 1,
            symbol,
            symbol_info.records_count
        );
    }

    print!(
        "\nSelect symbol (1-{}) or enter custom symbol:",
        available_symbols.len().min(10)
    );
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();

    let symbol = if let Ok(choice) = input.parse::<usize>() {
        if choice > 0 && choice <= available_symbols.len().min(10) {
            available_symbols[choice - 1].clone()
        } else {
            println!("Invalid selection");
            return Ok(());
        }
    } else if input.is_empty() {
        "BTCUSDT".to_string()
    } else {
        input.to_uppercase()
    };

    if !data_info.has_sufficient_data(&symbol, 100) {
        println!(
            "insufficient data from symbol: {} (minimum 100 records required)",
            symbol
        );
        return Ok(());
    }

    let symbol_info = data_info.get_symbol_info(&symbol).unwrap();
    println!(
        "Selected Symbol: {} ({} records available)",
        symbol, symbol_info.records_count
    );

    print!(
        "\nEnter number of records to backtest (default:10000, max:{})",
        symbol_info.records_count
    );
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let data_count: i64 = if input.trim().is_empty() {
        10000.min(symbol_info.records_count as i64)
    } else {
        input
            .trim()
            .parse()
            .unwrap_or(10000)
            .min(symbol_info.records_count as i64)
    };

    print!("\nEnter initial capital (default: $10000): $");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let initial_capital = if input.trim().is_empty() {
        Decimal::from(10000)
    } else {
        Decimal::from_str(input.trim()).unwrap_or(Decimal::from(10000))
    };

    print!("\nEnter commission rate % (default: 0.1%)");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let commission_rate = if input.trim().is_empty() {
        Decimal::from_str("0.001").unwrap()
    } else {
        let rate = input.trim().parse::<f64>().unwrap_or(0.1);
        Decimal::from_str(&format!("{}", rate / 100.0))
            .unwrap_or(Decimal::from_str("0.001").unwrap())
    };

    let temp_strategy = create_strategy(&selected_strategy.id)?;

    if temp_strategy.supports_ohlc() {
        if let Some(time_frame) = temp_strategy.preferred_timeframe() {
            println!(
                "strategy supports ohlc using {} timefram for better performance",
                time_frame.as_str()
            );

            let candle_count = (data_count / 50).max(100) as u32;
            match repository
                .generate_recent_ohlc_for_backtest(&symbol, time_frame, candle_count)
                .await
            {
                Ok(ohlc_data) if !ohlc_data.is_empty() => {
                    println!("loaded {} ohlc candles", ohlc_data.len());
                    println!(
                        "data range : {} to {}",
                        ohlc_data
                            .first()
                            .unwrap()
                            .timestamp
                            .format("%d-%m-%Y %H:%M:%S"),
                        ohlc_data
                            .last()
                            .unwrap()
                            .timestamp
                            .format("%d-%m-%Y %H:%M:%S")
                    );

                    let config =
                        BackTestConfig::new(initial_capital).with_commission_rate(commission_rate);
                    let strategy = create_strategy(&selected_strategy.id)?;
                    let mut engine = BackTestEngine::new(strategy, config)?;
                    let result = engine.run_with_ohlc(ohlc_data);

                    result.print_summary();

                    print!("\n show detailed trade analysis ? (y/N):");
                    io::stdout().flush()?;
                    let mut answer = String::new();
                    io::stdin().read_line(&mut answer)?;
                    if input.trim().to_lowercase() == "y" || input.trim().to_lowercase() == "yes" {
                        result.print_trade_analysis();
                    }

                    return Ok(());
                }
                Ok(_) => {
                    println!(
                        "no ohlc data available for timeframe {}",
                        time_frame.as_str()
                    );
                }
                Err(err) => {
                    println!("error generating recent ohlc: {}", err);
                }
            }
        }
    }

    println!(
        "\n loading historical tick data:{} latest {} records...",
        symbol, data_count
    );

    let data = repository
        .get_recent_ticks_for_backtest(&symbol, data_count)
        .await?;

    if data.is_empty() {
        println!("no historical tick data for symbol {}", symbol);
        return Ok(());
    }

    println!("loaded {} tick data points", data.len());

    println!(
        "data range : {} to {}",
        data.first().unwrap().timestamp.format("%d-%m-%Y %H:%M:%S"),
        data.last().unwrap().timestamp.format("%d-%m-%Y %H:%M:%S")
    );

    let config = BackTestConfig::new(initial_capital).with_commission_rate(commission_rate);
    let strategy = create_strategy(&selected_strategy.id)?;
    let mut engine = BackTestEngine::new(strategy, config)?;

    let result = engine.run(data);

    result.print_summary();

    // Ask whether to display detailed transaction analysis
    print!("\nShow detailed trade analysis? (y/N): ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    if input.trim().to_lowercase() == "y" || input.trim().to_lowercase() == "yes" {
        result.print_trade_analysis();
    }

    println!("\n🎉 Backtest completed successfully!");

    Ok(())
}

async fn run_live_application(settings: Settings) -> Result<(), Box<dyn std::error::Error>> {
    if settings.symbols.is_empty() {
        error!("❌ No symbols configured for monitoring");
        std::process::exit(1);
    }

    if settings.database.max_connections == 0 {
        error!("❌ Database max_connections must be greater than 0");
        std::process::exit(1);
    }

    let pool = create_database_pool(&settings).await?;

    // Test database connectivity
    test_database_connection(&pool).await?;

    let cache = create_cache(&settings).await?;

    let repository = Arc::new(TickDataRepository::new(pool, cache));
    let exchange = Arc::new(BinanceExchange::new());

    let service = MarketDataService::new(exchange, repository, settings.symbols.clone());

    let service_shutdown_tx = service.get_shutdown_tx();

    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        println!("Shutting down...");
        info!("received shutdown signal forwarding to service");
        let _ = service_shutdown_tx.send(());
    });

    match service.start().await {
        Ok(()) => {
            info!("service stopped");
            Ok(())
        }
        Err(err) => {
            error!("error starting service: {}", err);
            Err(Box::new(err))
        }
    }
}
