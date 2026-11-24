use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TradeSide {
    Sell,
    Buy,
}

impl TradeSide {
    pub fn as_str(self) -> &'static str {
        match self {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TickData {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: TradeSide,
    pub trade_id: String,
    pub is_buyer_maker: bool,
}
impl TickData {
    pub fn new(
        timestamp: DateTime<Utc>,
        symbol: String,
        price: Decimal,
        quantity: Decimal,
        side: TradeSide,
        trade_id: String,
        is_buyer_maker: bool,
    ) -> Self {
        Self {
            timestamp,
            symbol,
            price,
            quantity,
            side,
            trade_id,
            is_buyer_maker,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbStats {
    pub symbol: Option<String>,
    pub total_records: u64,
    pub earliest_timestamp: Option<DateTime<Utc>>,
    pub latest_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct TickQuery {
    pub symbol: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
    pub trade_side: Option<TradeSide>,
}

impl TickQuery {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            start_time: None,
            end_time: None,
            limit: None,
            trade_side: None,
        }
    }
}

#[derive(Error, Debug)]
pub enum DataError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Invalid data format: {0}")]
    InvalidFormat(String),

    #[error("Data not found:{0}")]
    NotFound(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Decimal conversion error: {0}")]
    DecimalConversionError(#[from] rust_decimal::Error),

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

pub type DataResult<T> = Result<T, DataError>;

#[derive(Debug, Clone)]
pub struct SymbolDataInfo {
    pub symbol: String,
    pub records_count: u64,
    pub earliest_timestamp: Option<DateTime<Utc>>,
    pub latest_timestamp: Option<DateTime<Utc>>,
    pub min_price: Option<Decimal>,
    pub max_price: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct BackTestDataInfo {
    pub total_records: u64,
    pub symbols_count: u64,
    pub earliest_timestamp: Option<DateTime<Utc>>,
    pub latest_timestamp: Option<DateTime<Utc>>,
    pub symbol_info: Vec<SymbolDataInfo>,
}

impl BackTestDataInfo {
    pub fn get_symbol_info(&self, symbol: &str) -> Option<&SymbolDataInfo> {
        self.symbol_info.iter().find(|s| s.symbol == symbol)
    }

    pub fn get_available_symbols(&self) -> Vec<String> {
        self.symbol_info
            .iter()
            .map(|symbol| symbol.symbol.clone())
            .collect()
    }

    pub fn has_sufficient_data(&self, symbol: &str, min_records: u64) -> bool {
        self.get_symbol_info(symbol)
            .map(|info| info.records_count >= min_records)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveStrategyLog {
    pub timestamp: DateTime<Utc>,
    pub strategy_id: String,
    pub symbol: String,
    pub current_price: Decimal,
    pub signal_type: String,
    pub portfolio_value: Decimal,
    pub total_pnl: Decimal,
    pub cache_hit: bool,
    pub processing_time_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy, PartialEq, Eq)]
pub enum TimeFrame {
    OneMinute,
    FiveMinute,
    FifteenMinute,
    ThirtyMinute,
    OneHour,
    FourHour,
    OneDay,
    OneWeek,
}

impl TimeFrame {
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeFrame::OneMinute => "1m",
            TimeFrame::FiveMinute => "5m",
            TimeFrame::FifteenMinute => "15m",
            TimeFrame::ThirtyMinute => "30m",
            TimeFrame::OneHour => "1h",
            TimeFrame::FourHour => "4h",
            TimeFrame::OneDay => "1d",
            TimeFrame::OneWeek => "1w",
        }
    }

    pub fn as_duration(&self) -> Duration {
        match self {
            TimeFrame::OneMinute => Duration::minutes(1),
            TimeFrame::FiveMinute => Duration::minutes(5),
            TimeFrame::FifteenMinute => Duration::minutes(15),
            TimeFrame::ThirtyMinute => Duration::minutes(30),
            TimeFrame::OneHour => Duration::hours(1),
            TimeFrame::FourHour => Duration::hours(4),
            TimeFrame::OneDay => Duration::days(1),
            TimeFrame::OneWeek => Duration::weeks(1),
        }
    }

    pub fn align_timestamp(&self, timestamp: DateTime<Utc>) -> DateTime<Utc> {
        let base = timestamp
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();

        match self {
            TimeFrame::OneMinute => base,

            TimeFrame::FiveMinute => base.with_minute((base.minute() / 5) * 5).unwrap(),
            TimeFrame::FifteenMinute => base.with_minute((base.minute() / 15) * 15).unwrap(),
            TimeFrame::ThirtyMinute => base.with_minute((base.minute() / 30) * 30).unwrap(),

            TimeFrame::OneHour => base.with_minute(0).unwrap(),

            TimeFrame::FourHour => base
                .with_minute(0)
                .unwrap()
                .with_hour((base.hour() / 4) * 4)
                .unwrap(),

            TimeFrame::OneDay => base.with_minute(0).unwrap().with_hour(0).unwrap(),

            TimeFrame::OneWeek => {
                let days_from_monday = base.weekday().num_days_from_monday();
                let week_start = base - Duration::days(days_from_monday as i64);
                week_start.with_hour(0).unwrap().with_minute(0).unwrap()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OHLCData {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub time_frame: TimeFrame,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    // total_count_volumn how the quantity
    pub volume: Decimal,
    // trade times how many times
    pub trade_count: u64,
}

impl OHLCData {
    pub fn new(
        timestamp: DateTime<Utc>,
        symbol: String,
        time_frame: TimeFrame,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
        trade_count: u64,
    ) -> Self {
        Self {
            timestamp,
            symbol,
            time_frame,
            open,
            high,
            low,
            close,
            volume,
            trade_count,
        }
    }

    // create OHLC from a collection of tick data
    pub fn from_ticks(
        ticks: &[TickData],
        time_frame: TimeFrame,
        window_start: DateTime<Utc>,
    ) -> Option<Self> {
        if ticks.is_empty() {
            return None;
        }

        let symbol = ticks[0].symbol.clone();
        let open = ticks[0].price;
        let mut high = ticks[0].price;
        let mut low = ticks[0].price;
        let close = ticks[ticks.len() - 1].price;
        let mut volume = Decimal::zero();

        for tick in ticks {
            if tick.price > high {
                high = tick.price;
            }
            if tick.price < low {
                low = tick.price;
            }
            volume += tick.quantity;
        }

        Some(OHLCData::new(
            window_start,
            symbol,
            time_frame,
            open,
            high,
            low,
            close,
            volume,
            ticks.len() as u64,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_align_timestamp() {
        // Base messy input: Wednesday, Oct 14, 2026, at 10:14:32 AM and 500 nanoseconds
        let input_time = Utc
            .with_ymd_and_hms(2026, 10, 14, 10, 14, 32)
            .unwrap()
            .with_nanosecond(500)
            .unwrap();

        // 1. OneMinute Case
        let expected_1m = Utc.with_ymd_and_hms(2026, 10, 14, 10, 14, 0).unwrap();
        assert_eq!(
            TimeFrame::OneMinute.align_timestamp(input_time),
            expected_1m
        );

        // 2. FiveMinutes Case (14 min rounds down to 10 min)
        let expected_5m = Utc.with_ymd_and_hms(2026, 10, 14, 10, 10, 0).unwrap();
        assert_eq!(
            TimeFrame::FiveMinute.align_timestamp(input_time),
            expected_5m
        );

        // 3. FifteenMinutes Case (14 min rounds down to 00 min)
        let expected_15m = Utc.with_ymd_and_hms(2026, 10, 14, 10, 0, 0).unwrap();
        assert_eq!(
            TimeFrame::FifteenMinute.align_timestamp(input_time),
            expected_15m
        );

        // 4. ThirtyMinutes Case (14 min rounds down to 00 min)
        let expected_30m = Utc.with_ymd_and_hms(2026, 10, 14, 10, 0, 0).unwrap();
        assert_eq!(
            TimeFrame::ThirtyMinute.align_timestamp(input_time),
            expected_30m
        );

        // 5. OneHour Case
        let expected_1h = Utc.with_ymd_and_hms(2026, 10, 14, 10, 0, 0).unwrap();
        assert_eq!(TimeFrame::OneHour.align_timestamp(input_time), expected_1h);

        // 6. FourHours Case (10 AM rounds down to 8 AM)
        let expected_4h = Utc.with_ymd_and_hms(2026, 10, 14, 8, 0, 0).unwrap();
        assert_eq!(TimeFrame::FourHour.align_timestamp(input_time), expected_4h);

        // 7. OneDay Case (Resets to Midnight)
        let expected_1d = Utc.with_ymd_and_hms(2026, 10, 14, 0, 0, 0).unwrap();
        assert_eq!(TimeFrame::OneDay.align_timestamp(input_time), expected_1d);

        // 8. OneWeek Case (Rewinds from Wednesday Oct 14 to Monday Oct 12)
        let expected_1w = Utc.with_ymd_and_hms(2026, 10, 12, 0, 0, 0).unwrap();
        assert_eq!(TimeFrame::OneWeek.align_timestamp(input_time), expected_1w);
    }
}
