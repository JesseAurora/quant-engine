use crate::data::cache::{TickDataCache, TieredCache};
use crate::data::types::{
    BackTestDataInfo, DataError, DataResult, DbStats, LiveStrategyLog, OHLCData, SymbolDataInfo,
    TickData, TickQuery, TimeFrame, TradeSide,
};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, QueryBuilder, Row};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

const DEFAULT_QUERY_LIMIT: u32 = 1000;
const MAX_QUERY_LIMIT: u32 = 10000;
const MAX_BATCH_SIZE: usize = 1000;

pub struct TickDataRepository {
    pool: PgPool,
    cache: TieredCache,
}

impl TickDataRepository {
    pub fn new(pool: PgPool, cache: TieredCache) -> Self {
        Self { pool, cache }
    }

    pub fn get_pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn get_cache(&self) -> &TieredCache {
        &self.cache
    }

    fn validate_tick_data(&self, tick: &TickData) -> DataResult<()> {
        if tick.symbol.is_empty() {
            return Err(DataError::ValidationError("symbol cannot be empty".into()));
        }

        if tick.price <= Decimal::ZERO {
            return Err(DataError::ValidationError("price cannot be zero".into()));
        }

        if tick.quantity <= Decimal::ZERO {
            return Err(DataError::ValidationError("quantity cannot be zero".into()));
        }

        if tick.trade_id.is_empty() {
            return Err(DataError::ValidationError(
                "trade_id cannot be empty".into(),
            ));
        }

        Ok(())
    }

    pub async fn insert_tick(&self, tick: &TickData) -> DataResult<()> {
        self.validate_tick_data(tick)?;

        debug!(
            "insert tick: symbol={}, price={}, trade_id={}",
            tick.symbol, tick.price, tick.trade_id
        );

        sqlx::query(
            r#"
    INSERT INTO tick_data
    (timestamp, symbol, price, quantity, side, trade_id, is_buyer_maker)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (symbol, trade_id, timestamp) DO NOTHING
    "#,
        )
        .bind(tick.timestamp)
        .bind(&tick.symbol)
        .bind(tick.price)
        .bind(tick.quantity)
        .bind(tick.side.as_str())
        .bind(&tick.trade_id)
        .bind(tick.is_buyer_maker)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            error!("Failed to insert tick data: {}", e);
            DataError::DatabaseError(e)
        })?;

        if let Err(e) = self.cache.push_tick(tick).await {
            warn!("failed to update cache after insert tick: {}", e);
        }

        debug!("insert tick complete");
        Ok(())
    }

    async fn batch_insert_chunk(&self, ticks: &[TickData]) -> DataResult<usize> {
        if ticks.is_empty() {
            return Ok(0);
        }

        let mut query_builder = QueryBuilder::new(
            "INSERT INTO tick_data (timestamp, symbol, price, quantity, side, trade_id, is_buyer_maker)"
        );

        // b builder row handler
        query_builder.push_values(ticks, |mut b, tick| {
            b.push_bind(tick.timestamp)
                .push_bind(&tick.symbol)
                .push_bind(tick.price)
                .push_bind(tick.quantity)
                .push_bind(tick.side.as_str())
                .push_bind(&tick.trade_id)
                .push_bind(tick.is_buyer_maker);
        });

        query_builder.push(" on conflict (symbol,trade_id,timestamp) do nothing ");
        let query = query_builder.build();
        let result = query.execute(&self.pool).await?;

        Ok(result.rows_affected() as usize)
    }

    pub async fn batch_insert(&self, ticks: Vec<TickData>) -> DataResult<usize> {
        if ticks.is_empty() {
            return Ok(0);
        }

        let filter_ticks = ticks
            .into_iter()
            .filter(|tick| self.validate_tick_data(&tick).is_ok())
            .collect::<Vec<_>>();

        let total_count = filter_ticks.len();
        if total_count == 0 {
            return Ok(0);
        }

        debug!("batch insert total_count={}", total_count);

        let mut total_inserted = 0;

        for chunk in filter_ticks.chunks(MAX_BATCH_SIZE) {
            let insert_chunk = self.batch_insert_chunk(chunk).await?;
            total_inserted += insert_chunk;

            for tick in chunk {
                if let Err(e) = self.cache.push_tick(tick).await {
                    warn!("failed to insert tick data:{} {}", tick.trade_id, e);
                }
            }
        }

        info!(
            "Successfully batch inserted {} out of {} tick records",
            total_inserted, total_count
        );
        Ok(total_inserted)
    }

    fn is_recent_query(&self, query: &TickQuery) -> bool {
        if let Some(start_time) = query.start_time {
            let now = Utc::now();
            let duration = now - start_time;
            duration <= Duration::hours(1)
        } else {
            true
        }
    }

    fn parse_trade_side(&self, side_str: &str) -> DataResult<TradeSide> {
        match side_str.to_uppercase().as_str() {
            "BUY" => Ok(TradeSide::Buy),
            "SELL" => Ok(TradeSide::Sell),
            _ => Err(DataError::InvalidFormat(format!(
                "Unknown trade side {}",
                side_str
            ))),
        }
    }

    fn parse_rows(&self, rows: Vec<PgRow>) -> DataResult<Vec<TickData>> {
        rows.into_iter()
            .map(|row| -> Result<TickData, _> {
                Ok(TickData {
                    timestamp: row.get::<DateTime<Utc>, _>("timestamp"),
                    symbol: row.get::<String, _>("symbol"),
                    price: row.get::<Decimal, _>("price"),
                    quantity: row.get::<Decimal, _>("quantity"),
                    side: self.parse_trade_side(&row.get::<String, _>("side"))?,
                    trade_id: row.get::<String, _>("trade_id"),
                    is_buyer_maker: row.get::<bool, _>("is_buyer_maker"),
                })
            })
            .collect()
    }

    async fn query_ticks_from_db(&self, query: &TickQuery) -> DataResult<Vec<TickData>> {
        let limit = query
            .limit
            .unwrap_or(DEFAULT_QUERY_LIMIT)
            .min(MAX_QUERY_LIMIT);

        let mut sql_query = QueryBuilder::new(
            "SELECT timestamp, symbol, price, quantity, side, trade_id, is_buyer_maker FROM tick_data WHERE symbol =   "
        );
        sql_query.push_bind(&query.symbol);

        if let Some(start_time) = query.start_time {
            sql_query.push(" and timestamp >= ").push_bind(start_time);
        }

        if let Some(end_time) = query.end_time {
            sql_query.push(" and timestamp <= ").push_bind(end_time);
        }

        if let Some(side) = query.trade_side {
            sql_query
                .push(" and trade_side = ")
                .push_bind(side.as_str());
        }

        sql_query
            .push(" order by timestamp desc limit ")
            .push_bind(limit as i64);

        let rows = sql_query.build().fetch_all(&self.pool).await?;

        self.parse_rows(rows)
    }
    pub async fn get_ticks(&self, query: &TickQuery) -> DataResult<Vec<TickData>> {
        let limit = query
            .limit
            .unwrap_or(DEFAULT_QUERY_LIMIT)
            .min(MAX_QUERY_LIMIT);

        debug!("query limit={}, symbol={} ", limit, query.symbol);

        // get recent ticks from cache
        if self.is_recent_query(query) {
            let cached_ticks = self
                .cache
                .get_recent_ticks(&query.symbol, limit as usize)
                .await?;

            if cached_ticks.len() == limit as usize {
                debug!(
                    "Cache hit: retrieved {} ticks from cache",
                    cached_ticks.len()
                );
                return Ok(cached_ticks);
            }
        }

        // cache miss or not recent search from db
        let ticks = self.query_ticks_from_db(query).await?;

        for tick in &ticks {
            if let Err(e) = self.cache.push_tick(tick).await {
                warn!(
                    "Failed to update tick data: {},trade_id {}",
                    e, tick.trade_id
                );
            }
        }

        Ok(ticks)
    }

    pub async fn get_latest_price(&self, symbol: &str) -> DataResult<Option<Decimal>> {
        debug!("query latest price symbol={}", symbol);

        let cached_ticks = self.cache.get_recent_ticks(symbol, 1).await?;

        if let Some(latest_tick) = cached_ticks.first() {
            debug!("query latest price tick: {}", latest_tick.price);
            return Ok(Some(latest_tick.price));
        }

        let price = sqlx::query_scalar::<_, Decimal>(
            r#"
            select price
            from tick_data
            where symbol = $1
            order by timestamp desc
            limit 1
            "#,
        )
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await?;

        // rewritten the row doesn't have .price
        // let price = row.map(|r| r.price);

        debug!("query latest price: {:?}", price);
        Ok(price)
    }

    pub async fn get_latest_prices(
        &self,
        symbols: &[String],
    ) -> DataResult<HashMap<String, Decimal>> {
        if symbols.is_empty() {
            return Ok(HashMap::new());
        }

        debug!("query latest prices symbols={:?}", symbols);

        let mut prices = HashMap::new();

        // try to get cache from cache first
        for symbol in symbols {
            if let Ok(cached_ticks) = self.cache.get_recent_ticks(symbol, 1).await {
                if let Some(latest_tick) = cached_ticks.first() {
                    prices.insert(symbol.to_string(), latest_tick.price);
                }
            }
        }

        // get symbol from db
        let missing_symbols: Vec<_> = symbols
            .iter()
            .filter(|symbol| !prices.contains_key(*symbol))
            .map(|symbol| symbol.clone())
            .collect();

        if !missing_symbols.is_empty() {
            let rows = sqlx::query(
                r#"
                select distinct on (symbol) symbol , price
                from tick_data
                where symbol = any($1)
                order by symbol, timestamp desc
                "#,
            )
            .bind(&missing_symbols[..])
            .fetch_all(&self.pool)
            .await?;

            for row in rows {
                prices.insert(row.get("symbol"), row.get("price"));
            }
        }

        debug!("query latest prices: {} for symbols", prices.len());

        Ok(prices)
    }

    pub async fn get_recent_ticks_for_backtest(
        &self,
        symbol: &str,
        count: i64,
    ) -> DataResult<Vec<TickData>> {
        debug!("Fetching {} recent ticks for backtest: {}", count, symbol);

        let limit = count.min(MAX_QUERY_LIMIT as i64);

        let rows = sqlx::query(
            r#"
            SELECT timestamp, symbol, price, quantity, side, trade_id, is_buyer_maker
            FROM tick_data
            WHERE symbol = $1
            ORDER BY timestamp DESC
            LIMIT $2
            "#,
        )
        .bind(symbol)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        // let ticks: DataResult<Vec<TickData>> = rows
        //     .iter()
        //     .map(|row| {
        //         Ok(TickData {
        //             timestamp: row.get("timestamp"),
        //             symbol: row.get("symbol"),
        //             price: row.get("price"),
        //             quantity: row.get("quantity"),
        //             side: self.parse_trade_size(row.get("side"))?,
        //             trade_id: row.get("trade_id"),
        //             is_buyer_maker: row.get("is_buyer_maker"),
        //         })
        //     })
        //     .collect();

        let mut ticks = self.parse_rows(rows)?;

        ticks.reverse();

        Ok(ticks)
    }

    pub async fn get_historical_data_for_backtest(
        &self,
        symbol: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: Option<i64>,
    ) -> DataResult<Vec<TickData>> {
        debug!(
            "Fetching historical data for backtest: {} from {} to {}",
            symbol, start_time, end_time
        );

        let query_limit = limit
            .unwrap_or(MAX_QUERY_LIMIT as i64)
            .min(MAX_QUERY_LIMIT as i64);

        let rows = sqlx::query(
            r#"
            SELECT timestamp, symbol, price, quantity, side, trade_id, is_buyer_maker
            FROM tick_data
            WHERE symbol = $1
            AND timestamp >= $2
            AND timestamp <= $3
            ORDER BY timestamp ASC
            LIMIT $4
            "#,
        )
        .bind(symbol)
        .bind(start_time)
        .bind(end_time)
        .bind(query_limit)
        .fetch_all(&self.pool)
        .await?;

        let ticks = self.parse_rows(rows)?;

        debug!("Retrieved {} historical ticks for backtest", ticks.len());
        Ok(ticks)
    }

    pub async fn get_backtest_data_info(&self) -> DataResult<BackTestDataInfo> {
        debug!("Fetching backtest data information");

        let overall_stats = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as symbols_count,
                MIN(timestamp) as earliest_time,
                MAX(timestamp) as latest_time
            FROM tick_data
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get per-symbol statistics
        let symbol_stats = sqlx::query(
            r#"
            SELECT
                symbol,
                COUNT(*) as records_count,
                MIN(timestamp) as earliest_time,
                MAX(timestamp) as latest_time,
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM tick_data
            GROUP BY symbol
            ORDER BY records_count DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let symbol_info: Vec<SymbolDataInfo> = symbol_stats
            .into_iter()
            .map(|row| SymbolDataInfo {
                // symbol: row.symbol,
                // records_count: row.records_count.unwrap_or(0) as u64,
                // earliest_timestamp: row.earliest_time,
                // latest_timestamp: row.latest_time,
                // min_price: row.min_price,
                // max_price: row.max_price,
                //
                symbol: row.get("symbol"),
                records_count: row.get::<Option<i64>, _>("records_count").unwrap_or(0) as u64,
                earliest_timestamp: row.get("earliest_time"),
                latest_timestamp: row.get("latest_time"),
                min_price: row.get("min_price"),
                max_price: row.get("max_price"),
            })
            .collect();

        let info = BackTestDataInfo {
            // total_records: overall_stats.total_records.unwrap_or(0) as u64,
            // symbols_count: overall_stats.symbols_count.unwrap_or(0) as u64,
            // earliest_timestamp: overall_stats.earliest_time,
            // latest_timestamp: overall_stats.latest_time,
            total_records: overall_stats
                .get::<Option<i64>, _>("total_records")
                .unwrap_or(0) as u64,
            symbols_count: overall_stats
                .get::<Option<i64>, _>("symbols_count")
                .unwrap_or(0) as u64,
            earliest_timestamp: overall_stats.get("earliest_time"),
            latest_timestamp: overall_stats.get("latest_time"),

            symbol_info,
        };

        debug!(
            "Backtest data info: {} total records, {} symbols",
            info.total_records, info.symbols_count
        );
        Ok(info)
    }

    pub async fn cleanup_old_data(&self, days_to_kep: f64) -> DataResult<u64> {
        info!("Cleaning up old data older than {} days", days_to_kep);

        let result = sqlx::query(
            r#"
            with deleted as (
            delete from tick_data
            where timestamp < NOW() - INTERVAL '1 day' * $1
            RETURNING *
            )
            select count(*) as count
            from deleted
            "#,
        )
        .bind(days_to_kep)
        .fetch_one(&self.pool)
        .await?;

        let deleted_count = result.get::<i64, _>("count") as u64;
        info!("cleaned up old data older than {} days", deleted_count);
        Ok(deleted_count)
    }

    pub async fn get_db_stats(&self, symbol: Option<&str>) -> DataResult<DbStats> {
        // let (total_records, earliest_timestamp, latest_timestamp) = if let Some(sym) = symbol {
        //     let row = sqlx::query(
        //         r#"
        //         SELECT
        //             COUNT(*) as total_records,
        //             MIN(timestamp) as earliest_timestamp,
        //             MAX(timestamp) as latest_timestamp
        //         FROM tick_data
        //         WHERE symbol = $1
        //         "#,
        //     )
        //     .bind(sym)
        //     .fetch_one(&self.pool)
        //     .await?;
        //
        //     (
        //         row.get::<Option<i64>, _>("total_records").unwrap_or(0) as u64,
        //         row.get::<Option<DateTime<Utc>>, _>("earliest_time"),
        //         row.get::<Option<DateTime<Utc>>, _>("latest_time"),
        //     )
        // } else {
        //     let row = sqlx::query(
        //         r#"
        //         SELECT
        //             COUNT(*) as total_records,
        //             MIN(timestamp) as earliest_timestamp,
        //             MAX(timestamp) as latest_timestamp
        //         FROM tick_data
        //         "#,
        //     )
        //     .fetch_one(&self.pool)
        //     .await?;
        //
        //     (
        //         row.get::<Option<i64>, _>("total_records").unwrap_or(0) as u64,
        //         row.get::<Option<DateTime<Utc>>, _>("earliest_time"),
        //         row.get::<Option<DateTime<Utc>>, _>("latest_time"),
        //     )
        // };
        //
        // Ok(DbStats {
        //     symbol: symbol.map(|s| s.into()),
        //     total_records,
        //     earliest_timestamp,
        //     latest_timestamp,
        // })

        // rewritten
        let row = sqlx::query(
            r#"
    SELECT
        COUNT(*) as total_records,
        MIN(timestamp) as earliest_timestamp,
        MAX(timestamp) as latest_timestamp
    FROM tick_data
    WHERE $1::TEXT IS NULL OR symbol = $1
    "#,
        )
        .bind(symbol.as_ref()) // Safely binds Option<&str> or Option<&String>
        .fetch_one(&self.pool)
        .await?;

        // 2. Safely extract values using the exact aliases defined in the SQL above
        let total_records = row.get::<Option<i64>, _>("total_records").unwrap_or(0) as u64;
        let earliest_timestamp = row.get::<Option<DateTime<Utc>>, _>("earliest_timestamp");
        let latest_timestamp = row.get::<Option<DateTime<Utc>>, _>("latest_timestamp");

        Ok(DbStats {
            symbol: symbol.map(|s| s.into()),
            total_records,
            earliest_timestamp,
            latest_timestamp,
        })
    }

    pub async fn insert_live_strategy_log(&self, log: &LiveStrategyLog) -> DataResult<()> {
        sqlx::query(
            r#"
        INSERT INTO live_strategy_log
        (timestamp, strategy_id, symbol, current_price, signal_type,
         portfolio_value, total_pnl, cache_hit, processing_time_us)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
        )
        .bind(log.timestamp)
        .bind(&log.strategy_id)
        .bind(&log.symbol)
        .bind(log.current_price)
        .bind(&log.signal_type)
        .bind(log.portfolio_value)
        .bind(log.total_pnl)
        .bind(log.cache_hit)
        .bind(log.processing_time_us as i32)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn generate_ohlc_from_ticks(
        &self,
        symbol: &str,
        time_frame: TimeFrame,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        limit: Option<i64>,
    ) -> DataResult<Vec<OHLCData>> {
        debug!(
            "Generating OHLC data: {} {} from {} to {}",
            symbol,
            time_frame.as_str(),
            start_time,
            end_time
        );

        let aligned_start = time_frame.align_timestamp(start_time);
        let aligned_end = time_frame.align_timestamp(end_time);

        let ticks = self
            .get_historical_data_for_backtest(
                symbol,
                aligned_start,
                aligned_end + time_frame.as_duration(),
                limit,
            )
            .await?;

        if ticks.is_empty() {
            debug!("No ticks found for {}", symbol);
            return Ok(vec![]);
        }

        let mut windows: HashMap<DateTime<Utc>, Vec<TickData>> = HashMap::new();

        for tick in ticks {
            let window_start = time_frame.align_timestamp(tick.timestamp);
            windows
                .entry(window_start)
                .or_insert_with(Vec::new)
                .push(tick);
        }

        let mut ohlc_data: Vec<OHLCData> = windows
            .into_iter()
            .filter_map(|(window_start, mut window_ticks)| {
                if window_start >= aligned_start && window_start <= aligned_end {
                    window_ticks.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
                    OHLCData::from_ticks(&window_ticks, time_frame, window_start)
                } else {
                    None
                }
            })
            .collect();

        ohlc_data.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        debug!(
            "Generated {} OHLC candles for {} {}",
            ohlc_data.len(),
            symbol,
            time_frame.as_str()
        );

        Ok(ohlc_data)
    }
    pub async fn get_ticks_for_timespan(
        &self,
        symbol: &str,
        duration_hour: i64,
    ) -> DataResult<Vec<TickData>> {
        let end_time = Utc::now();
        let start_time = end_time - Duration::hours(duration_hour);

        // 1. Convert to plain dynamic sqlx::query with explicit bindings
        let rows = sqlx::query(
            r#"
    SELECT timestamp, symbol, price, quantity, side, trade_id, is_buyer_maker
    FROM tick_data
    WHERE symbol = $1
      AND timestamp >= $2
      AND timestamp <= $3
    ORDER BY timestamp ASC
    "#,
        )
        .bind(symbol)
        .bind(start_time)
        .bind(end_time)
        .fetch_all(&self.pool)
        .await?;

        self.parse_rows(rows)
    }

    pub async fn get_ticks_for_timespan_limited(
        &self,
        symbol: &str,
        duration_hours: i64,
        max_records: i64,
    ) -> DataResult<Vec<TickData>> {
        let end_time = Utc::now();
        let start_time = end_time - Duration::hours(duration_hours);
        let limit = max_records.min(MAX_QUERY_LIMIT as i64);

        let rows = sqlx::query(
            r#"
        SELECT timestamp, symbol, price, quantity, side, trade_id, is_buyer_maker
        FROM tick_data
        WHERE symbol = $1
        AND timestamp >= $2
        AND timestamp <= $3
        ORDER BY timestamp ASC
        LIMIT $4
        "#,
        )
        .bind(symbol)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        self.parse_rows(rows)
    }

    fn calculate_required_duration_hours(timeframe: TimeFrame, candle_count: u32) -> i64 {
        let base_hours = match timeframe {
            TimeFrame::OneMinute => 1,
            TimeFrame::FiveMinute => 1,
            TimeFrame::FifteenMinute => 1,
            TimeFrame::ThirtyMinute => 1,
            TimeFrame::OneHour => 1,
            TimeFrame::FourHour => 4,
            TimeFrame::OneDay => 24,
            TimeFrame::OneWeek => 24 * 7,
        };

        // Add 20% buffer for data gaps
        let total_hours = (base_hours * candle_count as i64) as f64 * 1.2;
        total_hours.ceil() as i64
    }

    pub async fn generate_recent_ohlc_for_backtest(
        &self,
        symbol: &str,
        timeframe: TimeFrame,
        candle_count: u32,
    ) -> DataResult<Vec<OHLCData>> {
        // Calculate required time duration
        let duration_hours = Self::calculate_required_duration_hours(timeframe, candle_count);

        // Set reasonable limits for different timeframes
        let max_ticks = match timeframe {
            TimeFrame::OneMinute | TimeFrame::FiveMinute => 50000,
            TimeFrame::FifteenMinute | TimeFrame::ThirtyMinute => 100000,
            TimeFrame::OneHour => 200000,
            TimeFrame::FourHour => 500000,
            TimeFrame::OneDay => 1000000,
            TimeFrame::OneWeek => 2000000,
        };

        // Get ticks for the calculated time duration
        let recent_ticks = self
            .get_ticks_for_timespan_limited(symbol, duration_hours, max_ticks)
            .await?;

        if recent_ticks.is_empty() {
            return Ok(Vec::new());
        }

        // Use actual data time range for OHLC generation
        let start_time = recent_ticks[0].timestamp;
        let end_time = recent_ticks[recent_ticks.len() - 1].timestamp;

        // Generate OHLC data from tick data
        let mut ohlc_data = self
            .generate_ohlc_from_ticks(symbol, timeframe, start_time, end_time, None)
            .await?;

        // Sort by timestamp descending and take requested count
        ohlc_data.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        ohlc_data.truncate(candle_count as usize);
        ohlc_data.reverse(); // Return in chronological order

        Ok(ohlc_data)
    }

    pub async fn get_ohlc_data_info(
        &self,
        symbol: &str,
        timeframe: TimeFrame,
    ) -> DataResult<(u64, Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
        // Get basic tick data info first
        let stats = self.get_db_stats(Some(symbol)).await?;

        if let (Some(earliest), Some(latest)) = (stats.earliest_timestamp, stats.latest_timestamp) {
            // Align to timeframe boundaries
            let aligned_earliest = timeframe.align_timestamp(earliest);
            let aligned_latest = timeframe.align_timestamp(latest);

            // Calculate approximate number of candles
            let duration_diff = aligned_latest - aligned_earliest;
            let timeframe_duration = timeframe.as_duration();

            let estimated_candles = if timeframe_duration.num_seconds() > 0 {
                (duration_diff.num_seconds() / timeframe_duration.num_seconds()) as u64
            } else {
                0
            };

            Ok((
                estimated_candles,
                Some(aligned_earliest),
                Some(aligned_latest),
            ))
        } else {
            Ok((0, None, None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use std::env;
    use std::str::FromStr;

    fn create_test_tick(
        symbol: &str,
        price: &str,
        trade_id: &str,
        timestamp: Option<DateTime<Utc>>,
    ) -> TickData {
        TickData::new(
            timestamp.unwrap_or_else(Utc::now),
            symbol.to_string(),
            Decimal::from_str(price).unwrap(),
            Decimal::from_str("1.0").unwrap(),
            TradeSide::Buy,
            trade_id.to_string(),
            false,
        )
    }

    async fn create_repository() -> TickDataRepository {
        dotenv().ok();
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");

        let pool = PgPool::connect(&database_url)
            .await
            .expect("Cannot connect to database");

        let cache = TieredCache::new((100, 300), (&redis_url, 1000, 3600))
            .await
            .expect("Cannot initialize cache");

        TickDataRepository::new(pool, cache)
    }

    async fn cleanup_database(pool: &PgPool, symbol: &str) {
        sqlx::query("DELETE FROM tick_data WHERE symbol = $1")
            .bind(symbol)
            .execute(pool)
            .await
            .expect("Failed to clean up database");
    }

    #[tokio::test]
    async fn test_insert_and_read_single_tick() {
        let repo = create_repository().await;
        let pool = repo.get_pool();
        let symbol = "BTCUSDT_TEST_SINGLE";

        // Clean up before test
        cleanup_database(pool, symbol).await;

        // Insert a single tick
        let tick = create_test_tick(symbol, "50000.0", "test1", None);
        repo.insert_tick(&tick)
            .await
            .expect("Failed to insert tick");

        // Query the tick
        let query = TickQuery {
            symbol: symbol.to_string(),
            limit: Some(1),
            start_time: None,
            end_time: None,
            trade_side: None,
        };
        let ticks = repo.get_ticks(&query).await.expect("Failed to query ticks");

        // Verify
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].symbol, symbol);
        assert_eq!(ticks[0].price, Decimal::from_str("50000.0").unwrap());
        assert_eq!(ticks[0].trade_id, "test1");

        // Clean up
        cleanup_database(pool, symbol).await;
    }

    #[tokio::test]
    async fn test_batch_insert_and_read() {
        let repo = create_repository().await;
        let pool = repo.get_pool();
        let symbol = "BTCUSDT_TEST_BATCH";

        // Clean up before test
        cleanup_database(pool, symbol).await;

        // Prepare batch ticks
        let ticks = vec![
            create_test_tick(symbol, "50000.0", "batch1", None),
            create_test_tick(symbol, "51000.0", "batch2", None),
            create_test_tick(symbol, "52000.0", "batch3", None),
        ];

        // Batch insert
        let inserted_count = repo
            .batch_insert(ticks.clone())
            .await
            .expect("Failed to batch insert");
        assert_eq!(inserted_count, 3);

        // Query ticks
        let query = TickQuery {
            symbol: symbol.to_string(),
            limit: Some(3),
            start_time: None,
            end_time: None,
            trade_side: None,
        };
        let queried_ticks = repo.get_ticks(&query).await.expect("Failed to query ticks");

        // Verify
        assert_eq!(queried_ticks.len(), 3);
        assert!(queried_ticks
            .iter()
            .any(|t| t.trade_id == "batch1" && t.price == Decimal::from_str("50000.0").unwrap()));
        assert!(queried_ticks
            .iter()
            .any(|t| t.trade_id == "batch2" && t.price == Decimal::from_str("51000.0").unwrap()));
        assert!(queried_ticks
            .iter()
            .any(|t| t.trade_id == "batch3" && t.price == Decimal::from_str("52000.0").unwrap()));

        // Clean up
        cleanup_database(pool, symbol).await;
    }

    #[tokio::test]
    async fn test_cache_read_write() {
        let repo = create_repository().await;
        let pool = repo.get_pool();
        let cache = repo.get_cache();
        let symbol = "BTCUSDT_TEST_CACHE";

        // Clean up before test
        cleanup_database(pool, symbol).await;
        cache
            .clear_symbol(symbol)
            .await
            .expect("Failed to clear cache");

        // Insert a tick
        let tick = create_test_tick(symbol, "50000.0", "cache1", None);
        repo.insert_tick(&tick)
            .await
            .expect("Failed to insert tick");

        // Query from cache
        let cached_ticks = cache
            .get_recent_ticks(symbol, 1)
            .await
            .expect("Failed to read from cache");
        assert_eq!(cached_ticks.len(), 1);
        assert_eq!(cached_ticks[0].symbol, symbol);
        assert_eq!(cached_ticks[0].price, Decimal::from_str("50000.0").unwrap());
        assert_eq!(cached_ticks[0].trade_id, "cache1");

        // Query via get_ticks (should hit cache for recent data)
        let query = TickQuery {
            symbol: symbol.to_string(),
            limit: Some(1),
            start_time: Some(Utc::now() - Duration::hours(1)),
            end_time: None,
            trade_side: None,
        };
        let ticks = repo.get_ticks(&query).await.expect("Failed to query ticks");
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].symbol, symbol);
        assert_eq!(ticks[0].price, Decimal::from_str("50000.0").unwrap());

        // Clean up
        cleanup_database(pool, symbol).await;
        cache
            .clear_symbol(symbol)
            .await
            .expect("Failed to clear cache");
    }

    #[tokio::test]
    async fn test_latest_price() {
        let repo = create_repository().await;
        let pool = repo.get_pool();
        let symbol = "BTCUSDT_TEST_PRICE";

        // Clean up before test
        cleanup_database(pool, symbol).await;

        // Insert ticks with different timestamps
        let base_time = Utc::now();
        let tick1 = create_test_tick(symbol, "50000.0", "price1", Some(base_time));
        let tick2 = create_test_tick(
            symbol,
            "51000.0",
            "price2",
            Some(base_time + Duration::seconds(1)),
        );
        repo.insert_tick(&tick1)
            .await
            .expect("Failed to insert tick1");
        repo.insert_tick(&tick2)
            .await
            .expect("Failed to insert tick2");

        // Query latest price
        let price = repo
            .get_latest_price(symbol)
            .await
            .expect("Failed to get latest price");
        assert_eq!(price, Some(Decimal::from_str("51000.0").unwrap()));

        // Clean up
        cleanup_database(pool, symbol).await;
    }
    #[tokio::test]
    async fn test_tick_validation() {
        let repo = create_repository().await;

        let valid_tick = create_test_tick("BTCUSDT", "50000.0", "test1", None);
        assert!(repo.validate_tick_data(&valid_tick).is_ok());

        let invalid_tick = TickData::new(
            Utc::now(),
            "".to_string(),
            Decimal::from_str("50000.0").unwrap(),
            Decimal::from_str("1.0").unwrap(),
            TradeSide::Buy,
            "test".to_string(),
            false,
        );
        assert!(repo.validate_tick_data(&invalid_tick).is_err());
    }

    #[tokio::test]
    async fn test_get_recent_ticks_for_backtest() {
        let repo = create_repository().await;
        let pool = repo.get_pool();
        let symbol = "BTCUSDT_BACKTEST";

        // Clean up before test
        cleanup_database(pool, symbol).await;

        // Insert ticks with different timestamps
        let base_time = Utc::now();
        let ticks = vec![
            create_test_tick(symbol, "50000.0", "bt1", Some(base_time)),
            create_test_tick(
                symbol,
                "51000.0",
                "bt2",
                Some(base_time + Duration::seconds(1)),
            ),
            create_test_tick(
                symbol,
                "52000.0",
                "bt3",
                Some(base_time + Duration::seconds(2)),
            ),
        ];

        for tick in ticks {
            repo.insert_tick(&tick)
                .await
                .expect("Failed to insert tick");
        }

        // Get recent ticks for backtest
        let backtest_ticks = repo
            .get_recent_ticks_for_backtest(symbol, 3)
            .await
            .expect("Failed to get recent ticks for backtest");

        // Verify order is ASC (oldest first)
        assert_eq!(backtest_ticks.len(), 3);
        assert_eq!(backtest_ticks[0].trade_id, "bt1");
        assert_eq!(backtest_ticks[1].trade_id, "bt2");
        assert_eq!(backtest_ticks[2].trade_id, "bt3");
        assert!(backtest_ticks[0].timestamp <= backtest_ticks[1].timestamp);
        assert!(backtest_ticks[1].timestamp <= backtest_ticks[2].timestamp);

        // Clean up
        cleanup_database(pool, symbol).await;
    }

    #[tokio::test]
    async fn test_get_historical_data_for_backtest() {
        let repo = create_repository().await;
        let pool = repo.get_pool();
        let symbol = "BTCUSDT_BACKTEST";

        // Clean up before test
        cleanup_database(pool, symbol).await;

        // Insert ticks with different timestamps
        let base_time = Utc::now();
        let ticks = vec![
            create_test_tick(
                symbol,
                "50000.0",
                "hist1",
                Some(base_time - Duration::hours(2)),
            ),
            create_test_tick(
                symbol,
                "51000.0",
                "hist2",
                Some(base_time - Duration::hours(1)),
            ),
            create_test_tick(symbol, "52000.0", "hist3", Some(base_time)),
        ];

        for tick in ticks {
            repo.insert_tick(&tick)
                .await
                .expect("Failed to insert tick");
        }

        // Get historical data for backtest
        let start_time = base_time - Duration::hours(3);
        let end_time = base_time + Duration::hours(1);
        let historical_ticks = repo
            .get_historical_data_for_backtest(symbol, start_time, end_time, None)
            .await
            .expect("Failed to get historical data for backtest");

        // Verify order is ASC and within time range
        assert_eq!(historical_ticks.len(), 3);
        assert_eq!(historical_ticks[0].trade_id, "hist1");
        assert_eq!(historical_ticks[1].trade_id, "hist2");
        assert_eq!(historical_ticks[2].trade_id, "hist3");

        for tick in &historical_ticks {
            assert!(tick.timestamp >= start_time);
            assert!(tick.timestamp <= end_time);
        }

        // Clean up
        cleanup_database(pool, symbol).await;
    }

    #[tokio::test]
    async fn test_get_backtest_data_info() {
        let repo = create_repository().await;
        let pool = repo.get_pool();
        let symbol = "BTCUSDT_BACKTEST";

        // Clean up before test
        cleanup_database(pool, symbol).await;

        // Insert test data
        let tick = create_test_tick(symbol, "50000.0", "info1", None);
        repo.insert_tick(&tick)
            .await
            .expect("Failed to insert tick");

        // Get backtest data info
        let info = repo
            .get_backtest_data_info()
            .await
            .expect("Failed to get backtest data info");

        // Verify structure
        assert!(info.total_records > 0);
        assert!(info.symbols_count > 0);
        assert!(info.earliest_timestamp.is_some());
        assert!(info.latest_timestamp.is_some());
        assert!(!info.symbol_info.is_empty());

        // Test helper methods
        let symbols = info.get_available_symbols();
        assert!(symbols.contains(&symbol.to_string()));

        let symbol_info = info.get_symbol_info(symbol);
        assert!(symbol_info.is_some());
        assert!(symbol_info.unwrap().records_count > 0);

        // Clean up
        cleanup_database(pool, symbol).await;
    }
}
