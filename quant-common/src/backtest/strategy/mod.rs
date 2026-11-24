use crate::backtest::strategy::base::Strategy;
use crate::backtest::strategy::rsi::RsiStrategy;
use crate::backtest::strategy::sma::SmaStrategy;

pub mod base;
pub mod rsi;
pub mod sma;

#[derive(Debug, Clone)]
pub struct StrategyInfo {
    pub id: String,
    pub name: String,
    pub description: String,
}

pub fn create_strategy(strategy_id: &str) -> Result<Box<dyn Strategy>, String> {
    match strategy_id {
        "sma" => Ok(Box::new(SmaStrategy::new())),
        "rsi" => Ok(Box::new(RsiStrategy::new())),
        _ => Err(format!("unknown strategy id {}", strategy_id)),
    }
}

pub fn list_strategies() -> Vec<StrategyInfo> {
    vec![
        StrategyInfo {
            id: "sma".to_string(),
            name: "Simple Moving Average".to_string(),
            description: "Trading strategy based on short and long-term moving average crossover"
                .to_string(),
        },
        StrategyInfo {
            id: "rsi".to_string(),
            name: "RSI Strategy".to_string(),
            description: "Trading strategy based on Relative Strength Index (RSI)".to_string(),
        },
    ]
}

pub fn get_strategy_info(strategy_id: &str) -> Option<StrategyInfo> {
    list_strategies()
        .into_iter()
        .find(|info| info.id == strategy_id)
}
