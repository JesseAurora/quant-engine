// use quant_common::backtest::strategy::base::{Signal, Strategy};
// use quant_common::data::cache::TickDataCache;
// use quant_common::data::repository::TickDataRepository;
// use quant_common::data::types::{LiveStrategyLog, TickData, TimeFrame};
// use rust_decimal::Decimal;
// use std::collections::HashMap;
// use std::sync::Arc;
// use std::time::Instant;
// use tracing::log::debug;
//
// #[derive(Debug, Clone, Copy)]
// struct SymbolPosition {
//     quantity: Decimal,
//     avg_cost: Decimal,
// }
//
// impl SymbolPosition {
//     fn flat() -> Self {
//         Self {
//             quantity: Decimal::ZERO,
//             avg_cost: Decimal::ZERO,
//         }
//     }
// }
//
// pub struct PaperTradingProcessor {
//     strategy: Box<dyn Strategy + Send>,
//     repository: Arc<TickDataRepository>,
//     initial_capital: Decimal,
//     cash: Decimal,
//     positions: HashMap<String, SymbolPosition>,
//     last_prices: HashMap<String, Decimal>,
//     total_trades: u64,
// }
//
// impl PaperTradingProcessor {
//     pub fn new(
//         strategy: Box<dyn Strategy + Send>,
//         repository: Arc<TickDataRepository>,
//         initial_capital: Decimal,
//     ) -> Self {
//         Self {
//             strategy,
//             repository,
//             initial_capital,
//             cash: initial_capital,
//             positions: HashMap::new(),
//             last_prices: HashMap::new(),
//             total_trades: 0,
//         }
//     }
//
//     pub async fn process_tick(&mut self, tick: &TickData) -> Result<(), String> {
//         let start_time = Instant::now();
//
//         let cache_start = Instant::now();
//
//         let recent_ticks = self
//             .repository
//             .get_cache()
//             .get_recent_ticks(&tick.symbol, 20)
//             .await
//             .map_err(|e| format!("Failed to get recent ticks: {}", e))?;
//
//         let cache_hit = !recent_ticks.is_empty();
//
//         let cache_time = cache_start.elapsed().as_micros() as u64;
//
//         self.last_prices.insert(tick.symbol.clone(), tick.price);
//
//         let signal = self.strategy.on_tick(tick);
//
//         let signal_type = self.execute_signal(&signal, tick)?;
//
//         let portfolio_value = self.calculate_portfolio_value();
//
//         let total_pnl = portfolio_value - self.initial_capital;
//
//         let processing_time = start_time.elapsed().as_micros() as u64;
//
//         let log = LiveStrategyLog {
//             timestamp: tick.timestamp,
//             strategy_id: self.strategy.name().to_string(),
//             symbol: tick.symbol.clone(),
//             current_price: tick.price,
//             signal_type: signal_type.clone(),
//             portfolio_value,
//             total_pnl,
//             cache_hit,
//             processing_time_us: processing_time,
//         };
//
//         self.repository
//             .insert_live_strategy_log(&log)
//             .await
//             .map_err(|e| format!("Failed to insert live strategy log: {}", e))?;
//
//         self.log_activity(
//             &signal_type,
//             tick,
//             portfolio_value,
//             total_pnl,
//             cache_hit,
//             cache_time,
//             processing_time,
//         );
//
//         Ok(())
//     }
//
//     fn execute_signal(&mut self, signal: &Signal, tick: &TickData) -> Result<String, String> {
//         match signal {
//             Signal::Buy { quantity, .. } => {
//                 let cost = quantity * tick.price;
//                 if cost <= self.cash {
//                     let position = self
//                         .positions
//                         .entry(tick.symbol.clone())
//                         .or_insert_with(SymbolPosition::flat);
//
//                     if position.quantity == Decimal::ZERO {
//                         position.quantity = *quantity;
//                         position.avg_cost = tick.price;
//                     } else {
//                         let total_cost = (position.quantity * position.avg_cost) + cost;
//                         position.quantity += quantity;
//                         position.avg_cost = total_cost / position.quantity;
//                     }
//
//                     self.cash -= cost;
//                     self.total_trades += 1;
//
//                     debug!(
//                         "BUY executed: {} {} @ {}, position: {}, cash: {}",
//                         tick.symbol, quantity, tick.price, position.quantity, self.cash
//                     );
//                     return Ok("BUY".to_string());
//                 } else {
//                     debug!(
//                         "BUY signal ignored: insufficient cash ({} needed, {} available)",
//                         cost, self.cash
//                     );
//                 }
//             }
//
//             Signal::Sell { quantity, .. } => {
//                 let position = self
//                     .positions
//                     .entry(tick.symbol.clone())
//                     .or_insert_with(SymbolPosition::flat);
//
//                 if *quantity <= position.quantity {
//                     let proceeds = quantity * tick.price;
//                     self.cash += proceeds;
//                     position.quantity -= quantity;
//                     self.total_trades += 1;
//
//                     if position.quantity == Decimal::ZERO {
//                         position.avg_cost = Decimal::ZERO;
//                     }
//
//                     debug!(
//                         "SELL executed: {} {} @ {}, position: {}, cash: {}",
//                         tick.symbol, quantity, tick.price, position.quantity, self.cash
//                     );
//                     return Ok("SELL".to_string());
//                 } else {
//                     debug!(
//                         "SELL signal ignored: insufficient position ({} needed, {} available)",
//                         quantity, position.quantity
//                     );
//                 }
//             }
//
//             Signal::Hold => return Ok("HOLD".to_string()),
//         }
//         Ok("HOLD".to_string())
//     }
//
//     fn calculate_portfolio_value(&self) -> Decimal {
//         let holding_value: Decimal = self
//             .positions
//             .iter()
//             .filter(|(_, position)| position.quantity != Decimal::ZERO)
//             .map(|(symbol, position)| {
//                 let price = self
//                     .last_prices
//                     .get(symbol)
//                     .copied()
//                     .unwrap_or(position.avg_cost);
//
//                 position.quantity * price
//             })
//             .sum();
//         self.cash + holding_value
//     }
//
//     fn log_activity(
//         &self,
//         signal_type: &str,
//         tick: &TickData,
//         portfolio_value: Decimal,
//         total_pnl: Decimal,
//         cache_hit: bool,
//         cache_time_us: u64,
//         total_time_us: u64,
//     ) {
//         let position_qty = self
//             .positions
//             .get(&tick.symbol)
//             .map(|p| p.quantity)
//             .unwrap_or(Decimal::ZERO);
//
//         if signal_type != "HOLD" {
//             let return_pct = if self.initial_capital > Decimal::ZERO {
//                 total_pnl / self.initial_capital * Decimal::from(100)
//             } else {
//                 Decimal::ZERO
//             };
//             println!("🎯 {} {} @ ${} | Portfolio: ${} | P&L: ${} ({:.2}%) | Position: {} | Cash: ${} | Trades: {} | Cache: {} ({}μs) | Total: {}μs",
//                      signal_type,
//                      tick.symbol,
//                      tick.price,
//                      portfolio_value,
//                      total_pnl,
//                      return_pct,
//                      position_qty,
//                      self.cash,
//                      self.total_trades,
//                      if cache_hit { "HIT" } else { "MISS" },
//                      cache_time_us,
//                      total_time_us);
//         } else if tick.timestamp.timestamp() % 10 == 0 {
//             println!(
//                 "📊 {} {} @ ${} | Portfolio: ${} | P&L: ${} | Cache: {} ({}μs)",
//                 tick.symbol,
//                 if cache_hit { "HIT" } else { "MISS" },
//                 tick.price,
//                 portfolio_value,
//                 total_pnl,
//                 if cache_hit { "✓" } else { "✗" },
//                 cache_time_us
//             );
//         }
//     }
// }


use quant_common::backtest::strategy::base::{Signal, Strategy};
use quant_common::data::cache::TickDataCache;
use quant_common::data::repository::TickDataRepository;
use quant_common::data::types::{LiveStrategyLog, TickData};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Portfolio accounting (pure logic, no I/O, unit-testable without a database)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct SymbolState {
    quantity: Decimal,
    avg_cost: Decimal,
    last_price: Decimal,
    last_hold_log_secs: Option<i64>,
}

impl SymbolState {
    fn new(price: Decimal) -> Self {
        Self {
            quantity: Decimal::ZERO,
            avg_cost: Decimal::ZERO,
            last_price: price,
            last_hold_log_secs: None,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Reject {
    NonPositiveQuantity,
    InsufficientCash { needed: Decimal, available: Decimal },
    InsufficientPosition { needed: Decimal, available: Decimal },
}

#[derive(Debug)]
struct Portfolio {
    initial_capital: Decimal,
    cash: Decimal,
    /// Sum of quantity * last_price over all positions. Kept up to date
    /// incrementally, so valuing the portfolio is O(1) instead of a loop over
    /// every symbol on every tick. Decimal arithmetic is exact, so it can't drift.
    holdings_value: Decimal,
    symbols: HashMap<String, SymbolState>,
    total_trades: u64,
}

impl Portfolio {
    fn new(initial_capital: Decimal) -> Self {
        Self {
            initial_capital,
            cash: initial_capital,
            holdings_value: Decimal::ZERO,
            symbols: HashMap::new(),
            total_trades: 0,
        }
    }

    fn value(&self) -> Decimal {
        self.cash + self.holdings_value
    }

    fn pnl(&self) -> Decimal {
        self.value() - self.initial_capital
    }

    fn position_qty(&self, symbol: &str) -> Decimal {
        self.symbols
            .get(symbol)
            .map(|s| s.quantity)
            .unwrap_or(Decimal::ZERO)
    }

    /// Records the newest price for a symbol and revalues its position.
    /// Allocates the key String only the first time a symbol is seen.
    fn mark_to_market(&mut self, symbol: &str, price: Decimal) {
        if !self.symbols.contains_key(symbol) {
            self.symbols
                .insert(symbol.to_owned(), SymbolState::new(price));
        }
        if let Some(state) = self.symbols.get_mut(symbol) {
            self.holdings_value += state.quantity * (price - state.last_price);
            state.last_price = price;
        }
    }

    /// Call `mark_to_market` for this symbol first (the price is `state.last_price`).
    fn buy(&mut self, symbol: &str, quantity: Decimal, price: Decimal) -> Result<(), Reject> {
        // A zero or negative quantity would otherwise turn a "buy" into a cash deposit.
        if quantity <= Decimal::ZERO {
            return Err(Reject::NonPositiveQuantity);
        }

        let cost = quantity * price;
        if cost > self.cash {
            return Err(Reject::InsufficientCash {
                needed: cost,
                available: self.cash,
            });
        }

        let state = self
            .symbols
            .entry(symbol.to_owned())
            .or_insert_with(|| SymbolState::new(price));

        if state.quantity.is_zero() {
            state.quantity = quantity;
            state.avg_cost = price;
        } else {
            let total_cost = state.quantity * state.avg_cost + cost;
            state.quantity += quantity;
            state.avg_cost = total_cost / state.quantity;
        }

        self.cash -= cost;
        self.holdings_value += cost; // quantity * current price
        self.total_trades += 1;
        Ok(())
    }

    fn sell(&mut self, symbol: &str, quantity: Decimal, price: Decimal) -> Result<(), Reject> {
        if quantity <= Decimal::ZERO {
            return Err(Reject::NonPositiveQuantity);
        }

        // get_mut: a sell for an unknown symbol must not create an empty entry.
        let available = self
            .symbols
            .get(symbol)
            .map(|s| s.quantity)
            .unwrap_or(Decimal::ZERO);
        if quantity > available {
            return Err(Reject::InsufficientPosition {
                needed: quantity,
                available,
            });
        }

        let state = self
            .symbols
            .get_mut(symbol)
            .expect("position exists: available > 0");

        let proceeds = quantity * price;
        state.quantity -= quantity;
        if state.quantity.is_zero() {
            state.avg_cost = Decimal::ZERO;
        }

        self.cash += proceeds;
        self.holdings_value -= proceeds;
        self.total_trades += 1;
        Ok(())
    }

    /// Rate limit for HOLD records: at most one per `interval_secs` per symbol,
    /// based on tick time. `interval_secs <= 0` records every tick.
    fn should_log_hold(&mut self, symbol: &str, now_secs: i64, interval_secs: i64) -> bool {
        if interval_secs <= 0 {
            return true;
        }
        match self.symbols.get_mut(symbol) {
            Some(state) => match state.last_hold_log_secs {
                Some(last) if now_secs - last < interval_secs => false,
                _ => {
                    state.last_hold_log_secs = Some(now_secs);
                    true
                }
            },
            None => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Processor
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Buy,
    Sell,
    Hold,
}

impl Action {
    fn as_str(self) -> &'static str {
        match self {
            Action::Buy => "BUY",
            Action::Sell => "SELL",
            Action::Hold => "HOLD",
        }
    }
}

pub struct PaperTradingProcessor {
    strategy: Box<dyn Strategy + Send>,
    repository: Arc<TickDataRepository>,
    portfolio: Portfolio,
    /// HOLD ticks are recorded at most once per this many seconds per symbol
    /// (trades are always recorded). 0 = record every tick.
    hold_log_interval_secs: i64,
}

impl PaperTradingProcessor {
    pub fn new(
        strategy: Box<dyn Strategy + Send>,
        repository: Arc<TickDataRepository>,
        initial_capital: Decimal,
    ) -> Self {
        Self {
            strategy,
            repository,
            portfolio: Portfolio::new(initial_capital),
            hold_log_interval_secs: 10,
        }
    }

    pub fn with_hold_log_interval_secs(mut self, secs: i64) -> Self {
        self.hold_log_interval_secs = secs;
        self
    }

    pub async fn process_tick(&mut self, tick: &TickData) -> Result<(), String> {
        let start_time = Instant::now();

        self.portfolio.mark_to_market(&tick.symbol, tick.price);

        let signal = self.strategy.on_tick(tick);
        let action = self.execute_signal(&signal, tick);

        let portfolio_value = self.portfolio.value();
        let total_pnl = self.portfolio.pnl();

        // Strategy + execution time only; recording below is excluded.
        let processing_time_us = start_time.elapsed().as_micros() as u64;

        let should_record = action != Action::Hold
            || self.portfolio.should_log_hold(
            &tick.symbol,
            tick.timestamp.timestamp(),
            self.hold_log_interval_secs,
        );

        if should_record {
            self.record(action, tick, portfolio_value, total_pnl, processing_time_us)
                .await;
        }

        Ok(())
    }

    fn execute_signal(&mut self, signal: &Signal, tick: &TickData) -> Action {
        match signal {
            Signal::Buy { quantity, .. } => {
                match self.portfolio.buy(&tick.symbol, *quantity, tick.price) {
                    Ok(()) => {
                        debug!(
                            "BUY executed: {} {} @ {}, position: {}, cash: {}",
                            tick.symbol,
                            quantity,
                            tick.price,
                            self.portfolio.position_qty(&tick.symbol),
                            self.portfolio.cash
                        );
                        Action::Buy
                    }
                    Err(reason) => {
                        debug!("BUY signal ignored: {:?}", reason);
                        Action::Hold
                    }
                }
            }
            Signal::Sell { quantity, .. } => {
                match self.portfolio.sell(&tick.symbol, *quantity, tick.price) {
                    Ok(()) => {
                        debug!(
                            "SELL executed: {} {} @ {}, position: {}, cash: {}",
                            tick.symbol,
                            quantity,
                            tick.price,
                            self.portfolio.position_qty(&tick.symbol),
                            self.portfolio.cash
                        );
                        Action::Sell
                    }
                    Err(reason) => {
                        debug!("SELL signal ignored: {:?}", reason);
                        Action::Hold
                    }
                }
            }
            Signal::Hold => Action::Hold,
        }
    }

    /// Runs only for ticks worth recording (trades and rate-limited HOLDs), so
    /// the cache probe, allocations, logging and the DB write are off the
    /// per-tick hot path.
    async fn record(
        &self,
        action: Action,
        tick: &TickData,
        portfolio_value: Decimal,
        total_pnl: Decimal,
        processing_time_us: u64,
    ) {
        // Cheap probe (1 tick, not 20). A cache error is a miss, never a failure.
        let cache_start = Instant::now();
        let cache_hit = match self
            .repository
            .get_cache()
            .get_recent_ticks(&tick.symbol, 1)
            .await
        {
            Ok(ticks) => !ticks.is_empty(),
            Err(e) => {
                warn!("cache probe failed for {}: {}", tick.symbol, e);
                false
            }
        };
        let cache_time_us = cache_start.elapsed().as_micros() as u64;
        let cache_label = if cache_hit { "HIT" } else { "MISS" };

        if action != Action::Hold {
            let return_pct = if self.portfolio.initial_capital > Decimal::ZERO {
                total_pnl / self.portfolio.initial_capital * Decimal::from(100)
            } else {
                Decimal::ZERO
            };
            info!(
                "🎯 {} {} @ ${} | Portfolio: ${} | P&L: ${} ({:.2}%) | Position: {} | Cash: ${} | Trades: {} | Cache: {} ({}μs) | Total: {}μs",
                action.as_str(),
                tick.symbol,
                tick.price,
                portfolio_value,
                total_pnl,
                return_pct,
                self.portfolio.position_qty(&tick.symbol),
                self.portfolio.cash,
                self.portfolio.total_trades,
                cache_label,
                cache_time_us,
                processing_time_us + cache_time_us
            );
        } else {
            info!(
                "📊 {} {} @ ${} | Portfolio: ${} | P&L: ${} | Cache: {} ({}μs)",
                tick.symbol, cache_label, tick.price, portfolio_value, total_pnl, cache_label,
                cache_time_us
            );
        }

        let log = LiveStrategyLog {
            timestamp: tick.timestamp,
            strategy_id: self.strategy.name().to_string(),
            symbol: tick.symbol.clone(),
            current_price: tick.price,
            signal_type: action.as_str().to_string(),
            portfolio_value,
            total_pnl,
            cache_hit,
            processing_time_us,
        };

        // Fire and forget: the DB round trip no longer delays the next tick, and
        // a failed log write is a warning, not a failed trade. Volume is bounded
        // because only trades and rate-limited HOLDs get here.
        let repository = Arc::clone(&self.repository);
        tokio::spawn(async move {
            if let Err(e) = repository.insert_live_strategy_log(&log).await {
                warn!("Failed to insert live strategy log: {}", e);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(n: i64) -> Decimal {
        Decimal::from(n)
    }

    #[test]
    fn buy_then_price_move_revalues_holdings() {
        let mut p = Portfolio::new(d(1000));
        p.mark_to_market("BTC", d(100));
        p.buy("BTC", d(2), d(100)).unwrap();

        assert_eq!(p.cash, d(800));
        assert_eq!(p.value(), d(1000));

        p.mark_to_market("BTC", d(110));
        assert_eq!(p.value(), d(1020));
        assert_eq!(p.pnl(), d(20));
    }

    #[test]
    fn sell_realizes_proceeds_and_keeps_value_consistent() {
        let mut p = Portfolio::new(d(1000));
        p.mark_to_market("BTC", d(100));
        p.buy("BTC", d(2), d(100)).unwrap();
        p.mark_to_market("BTC", d(110));
        p.sell("BTC", d(1), d(110)).unwrap();

        assert_eq!(p.cash, d(910));
        assert_eq!(p.position_qty("BTC"), d(1));
        assert_eq!(p.value(), d(1020)); // unchanged by the sale itself
        assert_eq!(p.total_trades, 2);
    }

    #[test]
    fn average_cost_is_weighted() {
        let mut p = Portfolio::new(d(1000));
        p.mark_to_market("BTC", d(100));
        p.buy("BTC", d(1), d(100)).unwrap();
        p.mark_to_market("BTC", d(200));
        p.buy("BTC", d(1), d(200)).unwrap();

        assert_eq!(p.symbols["BTC"].avg_cost, d(150));
    }

    #[test]
    fn closing_a_position_resets_avg_cost() {
        let mut p = Portfolio::new(d(1000));
        p.mark_to_market("BTC", d(100));
        p.buy("BTC", d(1), d(100)).unwrap();
        p.sell("BTC", d(1), d(100)).unwrap();

        assert_eq!(p.symbols["BTC"].avg_cost, Decimal::ZERO);
        assert_eq!(p.holdings_value, Decimal::ZERO);
    }

    #[test]
    fn rejects_do_not_change_state() {
        let mut p = Portfolio::new(d(100));
        p.mark_to_market("BTC", d(100));

        assert_eq!(p.buy("BTC", d(0), d(100)), Err(Reject::NonPositiveQuantity));
        assert_eq!(p.buy("BTC", d(-5), d(100)), Err(Reject::NonPositiveQuantity));
        assert!(matches!(
            p.buy("BTC", d(2), d(100)),
            Err(Reject::InsufficientCash { .. })
        ));
        assert!(matches!(
            p.sell("BTC", d(1), d(100)),
            Err(Reject::InsufficientPosition { .. })
        ));

        assert_eq!(p.cash, d(100));
        assert_eq!(p.total_trades, 0);
        assert_eq!(p.value(), d(100));
    }

    #[test]
    fn sell_of_unknown_symbol_creates_no_entry() {
        let mut p = Portfolio::new(d(100));
        assert!(p.sell("ETH", d(1), d(10)).is_err());
        assert!(!p.symbols.contains_key("ETH"));
    }

    #[test]
    fn multiple_symbols_are_valued_independently() {
        let mut p = Portfolio::new(d(1000));
        p.mark_to_market("BTC", d(100));
        p.mark_to_market("ETH", d(10));
        p.buy("BTC", d(1), d(100)).unwrap();
        p.buy("ETH", d(10), d(10)).unwrap();

        p.mark_to_market("BTC", d(120));
        p.mark_to_market("ETH", d(9));

        // cash 800 + BTC 120 + ETH 90
        assert_eq!(p.value(), d(1010));
    }

    #[test]
    fn hold_log_is_rate_limited_per_symbol() {
        let mut p = Portfolio::new(d(100));
        p.mark_to_market("BTC", d(100));

        assert!(p.should_log_hold("BTC", 1000, 10)); // first one logs
        assert!(!p.should_log_hold("BTC", 1005, 10)); // within the interval
        assert!(p.should_log_hold("BTC", 1010, 10)); // interval elapsed
        assert!(p.should_log_hold("BTC", 1011, 0)); // 0 = every tick
    }
}