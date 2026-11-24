use crate::data::types::{DataError, DataResult, TickData};
use async_trait::async_trait;
use redis::{aio::ConnectionManager, AsyncCommands, Client};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// Redis set that tracks which symbols have a tick list (replaces `KEYS tick:*`).
/// Deliberately not prefixed `tick:` so it can never collide with a symbol key.
const SYMBOLS_KEY: &str = "ticks:symbols";

#[async_trait]
pub trait TickDataCache: Send + Sync {
    async fn push_tick(&self, tick_data: &TickData) -> DataResult<()>;
    async fn get_recent_ticks(&self, symbol: &str, limit: usize) -> DataResult<Vec<TickData>>;
    async fn get_symbols(&self) -> DataResult<Vec<String>>;
    async fn clear_symbol(&self, symbol: &str) -> DataResult<()>;
    async fn clear_all(&self) -> DataResult<()>;
}

// ---------------------------------------------------------------------------
// L1: in-memory cache
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct MemoryCacheEntry {
    /// Oldest at the front, newest at the back.
    ticks: VecDeque<TickData>,
    /// TTL is measured from the last write, which matches Redis `EXPIRE` semantics.
    last_updated: Instant,
}

impl MemoryCacheEntry {
    fn new(max_size: usize) -> Self {
        Self {
            ticks: VecDeque::with_capacity(max_size.min(4096)),
            last_updated: Instant::now(),
        }
    }

    fn push_tick(&mut self, tick_data: TickData, max_size: usize) {
        self.ticks.push_back(tick_data);
        self.last_updated = Instant::now();

        while self.ticks.len() > max_size {
            self.ticks.pop_front();
        }
    }

    /// Newest first. Takes `&self`, so reads only need a read lock.
    fn get_recent_ticks(&self, limit: usize) -> Vec<TickData> {
        self.ticks.iter().rev().take(limit).cloned().collect()
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.last_updated.elapsed() > ttl
    }
}

pub struct InMemoryTickCache {
    // Critical sections are tiny and never held across an `.await`,
    // so a std lock is the right tool (not tokio's).
    // RwLock: many concurrent readers, writers only block on push/clear.
    data: Arc<RwLock<HashMap<String, MemoryCacheEntry>>>,
    max_ticks_per_symbol: usize,
    ttl: Duration,
}

impl InMemoryTickCache {
    pub fn new(max_ticks_per_symbol: usize, ttl_seconds: u64) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_ticks_per_symbol: max_ticks_per_symbol.max(1),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    // A poisoned lock only means another thread panicked mid-write; the map is
    // still structurally valid, so recover instead of failing every call.
    fn read(&self) -> RwLockReadGuard<'_, HashMap<String, MemoryCacheEntry>> {
        self.data.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<String, MemoryCacheEntry>> {
        self.data.write().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn cleanup_expired(&self) {
        let mut guard = self.write();
        let ttl = self.ttl;
        guard.retain(|symbol, entry| {
            let keep = !entry.is_expired(ttl);
            if !keep {
                debug!("cleaned up expired cache for symbol:{}", symbol);
            }
            keep
        });
    }

    /// Replace a symbol's ticks with `newest_first` (the order Redis returns).
    /// Used for L2 -> L1 backfill so ordering is preserved and nothing is duplicated.
    fn replace_ticks(&self, symbol: &str, newest_first: &[TickData]) {
        let max = self.max_ticks_per_symbol;
        let mut guard = self.write();
        let entry = guard
            .entry(symbol.to_owned())
            .or_insert_with(|| MemoryCacheEntry::new(max));

        entry.ticks = newest_first
            .iter()
            .take(max)
            .rev() // oldest -> newest, matching push_back order
            .cloned()
            .collect();
        entry.last_updated = Instant::now();
    }
}

#[async_trait]
impl TickDataCache for InMemoryTickCache {
    async fn push_tick(&self, tick_data: &TickData) -> DataResult<()> {
        let max = self.max_ticks_per_symbol;
        let mut data = self.write();

        // get_mut first: only allocate the key String when the symbol is new.
        if let Some(entry) = data.get_mut(&tick_data.symbol) {
            entry.push_tick(tick_data.clone(), max);
        } else {
            let mut entry = MemoryCacheEntry::new(max);
            entry.push_tick(tick_data.clone(), max);
            data.insert(tick_data.symbol.clone(), entry);
        }

        debug!(
            "Added tick to memory cache: symbol={},price={}",
            tick_data.symbol, tick_data.price
        );
        Ok(())
    }

    async fn get_recent_ticks(&self, symbol: &str, limit: usize) -> DataResult<Vec<TickData>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let data = self.read();
        match data.get(symbol) {
            Some(entry) if !entry.is_expired(self.ttl) => {
                let ticks = entry.get_recent_ticks(limit);
                debug!("Retrieved {} ticks for symbol {}", ticks.len(), symbol);
                Ok(ticks)
            }
            Some(_) => {
                debug!("memory cache entry expired for symbol {}", symbol);
                Ok(Vec::new())
            }
            None => {
                debug!("no memory cache entry for symbol {}", symbol);
                Ok(Vec::new())
            }
        }
    }

    async fn get_symbols(&self) -> DataResult<Vec<String>> {
        let data = self.read();
        Ok(data
            .iter()
            .filter(|(_, entry)| !entry.is_expired(self.ttl))
            .map(|(symbol, _)| symbol.clone())
            .collect())
    }

    async fn clear_symbol(&self, symbol: &str) -> DataResult<()> {
        self.write().remove(symbol);
        debug!("Removed symbol {}", symbol);
        Ok(())
    }

    async fn clear_all(&self) -> DataResult<()> {
        self.write().clear();
        debug!("cleared all symbols");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// L2: Redis cache
// ---------------------------------------------------------------------------

pub struct RedisTickCache {
    /// Multiplexed + auto-reconnecting. Cheap to clone, so no mutex is needed
    /// and concurrent callers don't serialize on a lock.
    conn: ConnectionManager,
    max_ticks_per_symbol: usize,
    ttl_seconds: u64,
}

impl RedisTickCache {
    pub async fn new(
        redis_url: &str,
        max_ticks_per_symbol: usize,
        ttl_seconds: u64,
    ) -> DataResult<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| DataError::CacheError(format!("failed to create redis client: {}", e)))?;

        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| DataError::CacheError(format!("failed to get redis connection: {}", e)))?;

        Ok(Self {
            conn,
            max_ticks_per_symbol: max_ticks_per_symbol.max(1),
            ttl_seconds,
        })
    }

    fn get_cache_key(&self, symbol: &str) -> String {
        format!("tick:{}", symbol)
    }
}

#[async_trait]
impl TickDataCache for RedisTickCache {
    async fn push_tick(&self, tick_data: &TickData) -> DataResult<()> {
        let key = self.get_cache_key(&tick_data.symbol);
        let tick_json = serde_json::to_string(tick_data)
            .map_err(|e| DataError::CacheError(format!("failed to serialize tick data: {}", e)))?;

        let mut conn = self.conn.clone();

        // One round trip, executed atomically (MULTI/EXEC) instead of three.
        let mut pipe = redis::pipe();
        pipe.atomic()
            .lpush(&key, &tick_json)
            .ignore()
            .ltrim(&key, 0, self.max_ticks_per_symbol as isize - 1)
            .ignore()
            .expire(&key, self.ttl_seconds as i64)
            .ignore()
            .sadd(SYMBOLS_KEY, &tick_data.symbol)
            .ignore();

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| DataError::CacheError(format!("failed to push tick to redis: {}", e)))?;

        debug!(
            "added tick to redis cache: symbol={},price={}",
            tick_data.symbol, tick_data.price
        );

        Ok(())
    }

    async fn get_recent_ticks(&self, symbol: &str, limit: usize) -> DataResult<Vec<TickData>> {
        // LRANGE key 0 -1 would return the WHOLE list, so guard zero explicitly.
        if limit == 0 {
            return Ok(Vec::new());
        }

        let key = self.get_cache_key(symbol);
        let mut conn = self.conn.clone();

        let tick_jsons: Vec<String> = conn
            .lrange(&key, 0, limit as isize - 1)
            .await
            .map_err(|e| DataError::CacheError(format!("failed to redis range: {}", e)))?;

        let mut ticks = Vec::with_capacity(tick_jsons.len());
        for tick_json in &tick_jsons {
            match serde_json::from_str::<TickData>(tick_json) {
                Ok(tick) => ticks.push(tick),
                Err(e) => warn!("failed to deserialize tick: {}", e),
            }
        }

        debug!("Retrieved {} ticks for symbol {}", ticks.len(), symbol);
        Ok(ticks)
    }

    async fn get_symbols(&self) -> DataResult<Vec<String>> {
        let mut conn = self.conn.clone();

        // O(N) in tracked symbols but non-blocking for Redis, unlike `KEYS tick:*`.
        let symbols: Vec<String> = conn
            .smembers(SYMBOLS_KEY)
            .await
            .map_err(|e| DataError::CacheError(format!("failed to read symbol set: {}", e)))?;

        if symbols.is_empty() {
            return Ok(symbols);
        }

        // A symbol's list can expire via TTL while its set member stays behind.
        // Check liveness in one pipelined round trip and lazily prune the stale ones.
        let mut pipe = redis::pipe();
        for symbol in &symbols {
            pipe.exists(self.get_cache_key(symbol));
        }
        let alive: Vec<bool> = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| DataError::CacheError(format!("failed to check symbol keys: {}", e)))?;

        let mut live = Vec::with_capacity(symbols.len());
        let mut stale = Vec::new();
        for (symbol, is_alive) in symbols.into_iter().zip(alive) {
            if is_alive {
                live.push(symbol);
            } else {
                stale.push(symbol);
            }
        }

        if !stale.is_empty() {
            let pruned: redis::RedisResult<()> = conn.srem(SYMBOLS_KEY, &stale).await;
            if let Err(e) = pruned {
                warn!("failed to prune stale symbols: {}", e);
            }
        }

        Ok(live)
    }

    async fn clear_symbol(&self, symbol: &str) -> DataResult<()> {
        let key = self.get_cache_key(symbol);
        let mut conn = self.conn.clone();

        let mut pipe = redis::pipe();
        pipe.atomic()
            .del(&key)
            .ignore()
            .srem(SYMBOLS_KEY, symbol)
            .ignore();

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| DataError::CacheError(format!("failed to delete tick: {}", e)))?;

        debug!("cleared symbol {}", symbol);
        Ok(())
    }

    async fn clear_all(&self) -> DataResult<()> {
        let mut conn = self.conn.clone();

        let symbols: Vec<String> = conn
            .smembers(SYMBOLS_KEY)
            .await
            .map_err(|e| DataError::CacheError(format!("failed to read symbol set: {}", e)))?;

        // Everything goes in a single round trip instead of one per symbol.
        let mut pipe = redis::pipe();
        for symbol in &symbols {
            pipe.unlink(self.get_cache_key(symbol)).ignore();
        }
        pipe.unlink(SYMBOLS_KEY).ignore();

        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| DataError::CacheError(format!("failed to clear redis cache: {}", e)))?;

        debug!("cleared all symbols");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tiered: L1 memory + L2 redis
// ---------------------------------------------------------------------------

pub struct TieredCache {
    memory_cache: InMemoryTickCache,
    redis_cache: RedisTickCache,
}

impl TieredCache {
    pub async fn new(
        memory_config: (usize, u64),
        redis_config: (&str, usize, u64),
    ) -> DataResult<Self> {
        let memory_cache = InMemoryTickCache::new(memory_config.0, memory_config.1);
        let redis_cache =
            RedisTickCache::new(redis_config.0, redis_config.1, redis_config.2).await?;

        debug!("initialized tiered cache with memory and redis cache");

        Ok(Self {
            memory_cache,
            redis_cache,
        })
    }

    pub fn cleanup_memory_cache(&self) {
        self.memory_cache.cleanup_expired()
    }

    /// Periodically evicts expired L1 entries. The task holds only a `Weak`
    /// reference, so it stops by itself once the cache is dropped.
    pub fn spawn_cleanup_task(self: &Arc<Self>, every: Duration) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(every);
            loop {
                ticker.tick().await;
                match weak.upgrade() {
                    Some(cache) => cache.cleanup_memory_cache(),
                    None => break,
                }
            }
        })
    }
}

#[async_trait]
impl TickDataCache for TieredCache {
    async fn push_tick(&self, tick_data: &TickData) -> DataResult<()> {
        // The L1 push is synchronous work, so there is nothing to overlap with join!.
        if let Err(e) = self.memory_cache.push_tick(tick_data).await {
            error!("memory cache push failed: {}", e);
        }

        self.redis_cache.push_tick(tick_data).await
    }

    async fn get_recent_ticks(&self, symbol: &str, limit: usize) -> DataResult<Vec<TickData>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let memory_ticks = self.memory_cache.get_recent_ticks(symbol, limit).await?;
        if memory_ticks.len() >= limit {
            debug!("memory cache hit: {}", symbol);
            return Ok(memory_ticks);
        }

        // L1 doesn't have enough; ask L2. If Redis is down, degrade to whatever L1 has.
        let redis_ticks = match self.redis_cache.get_recent_ticks(symbol, limit).await {
            Ok(ticks) => ticks,
            Err(e) => {
                warn!(
                    "redis read failed for {}, serving memory cache: {}",
                    symbol, e
                );
                return Ok(memory_ticks);
            }
        };

        if redis_ticks.is_empty() {
            debug!("cache miss for symbol: {}", symbol);
            return Ok(memory_ticks);
        }

        debug!("redis cache hit: {}", symbol);

        // Backfill L1 only if L2 has at least as much; replace (don't append) so
        // ordering stays newest-at-back and nothing gets duplicated.
        if redis_ticks.len() >= memory_ticks.len() {
            self.memory_cache.replace_ticks(symbol, &redis_ticks);
        }

        Ok(redis_ticks)
    }

    async fn get_symbols(&self) -> DataResult<Vec<String>> {
        let memory_symbols = self.memory_cache.get_symbols().await?;
        let redis_symbols = self.redis_cache.get_symbols().await?;

        let mut all_symbols = HashSet::with_capacity(memory_symbols.len() + redis_symbols.len());
        all_symbols.extend(memory_symbols);
        all_symbols.extend(redis_symbols);

        Ok(all_symbols.into_iter().collect())
    }

    async fn clear_symbol(&self, symbol: &str) -> DataResult<()> {
        // Clear L1 first (sync, infallible), then L2.
        self.memory_cache.clear_symbol(symbol).await?;
        self.redis_cache.clear_symbol(symbol).await
    }

    async fn clear_all(&self) -> DataResult<()> {
        self.memory_cache.clear_all().await?;
        self.redis_cache.clear_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::types::TradeSide;
    use chrono::Utc;
    use rust_decimal::Decimal;

    fn create_test_tick(symbol: &str, price: &str, trade_id: &str) -> TickData {
        TickData::new(
            Utc::now(),
            symbol.to_string(),
            price.parse::<Decimal>().unwrap(),
            "1.0".parse::<Decimal>().unwrap(),
            TradeSide::Buy,
            trade_id.to_string(),
            false,
        )
    }

    #[tokio::test]
    async fn test_memory_cache() {
        let cache = InMemoryTickCache::new(100, 300);
        let tick = create_test_tick("BTCUSDT", "5000.0", "test1");
        cache.push_tick(&tick).await.unwrap();
        let ticks = cache.get_recent_ticks("BTCUSDT", 1).await.unwrap();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].price, "5000.0".parse::<Decimal>().unwrap());
        assert_eq!(ticks[0].symbol, "BTCUSDT");
    }

    #[tokio::test]
    async fn test_memory_cache_size() {
        let cache = InMemoryTickCache::new(2, 300);

        for i in 1..=3 {
            let price = format!("{}.0", 5000 + i);
            let tick = create_test_tick("BTCUSDT", &price, &format!("test{}", i));
            cache.push_tick(&tick).await.unwrap();
        }

        let ticks = cache.get_recent_ticks("BTCUSDT", 10).await.unwrap();
        assert_eq!(ticks.len(), 2);

        assert_eq!(ticks[0].price, "5003.0".parse::<Decimal>().unwrap());
        assert_eq!(ticks[1].price, "5002.0".parse::<Decimal>().unwrap());
    }

    #[tokio::test]
    async fn test_limit_zero_returns_empty() {
        let cache = InMemoryTickCache::new(10, 300);
        cache
            .push_tick(&create_test_tick("BTCUSDT", "5000.0", "t1"))
            .await
            .unwrap();
        let ticks = cache.get_recent_ticks("BTCUSDT", 0).await.unwrap();
        assert!(ticks.is_empty());
    }

    #[tokio::test]
    async fn test_replace_ticks_preserves_newest_first_order() {
        let cache = InMemoryTickCache::new(10, 300);

        // Pre-existing data must be replaced, not duplicated.
        cache
            .push_tick(&create_test_tick("BTCUSDT", "1.0", "old"))
            .await
            .unwrap();

        // Redis order: newest first.
        let newest_first = vec![
            create_test_tick("BTCUSDT", "3.0", "t3"),
            create_test_tick("BTCUSDT", "2.0", "t2"),
        ];
        cache.replace_ticks("BTCUSDT", &newest_first);

        let ticks = cache.get_recent_ticks("BTCUSDT", 10).await.unwrap();
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].price, "3.0".parse::<Decimal>().unwrap());
        assert_eq!(ticks[1].price, "2.0".parse::<Decimal>().unwrap());
    }

    #[tokio::test]
    async fn test_expired_entries_are_not_served_and_get_cleaned() {
        let cache = InMemoryTickCache::new(10, 0); // ttl = 0s
        cache
            .push_tick(&create_test_tick("BTCUSDT", "1.0", "t1"))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(5)).await;

        assert!(cache
            .get_recent_ticks("BTCUSDT", 1)
            .await
            .unwrap()
            .is_empty());
        assert!(cache.get_symbols().await.unwrap().is_empty());

        cache.cleanup_expired();
        assert!(cache.read().is_empty());
    }

    #[tokio::test]
    async fn test_clear_symbol_and_clear_all() {
        let cache = InMemoryTickCache::new(10, 300);
        cache
            .push_tick(&create_test_tick("BTCUSDT", "1.0", "t1"))
            .await
            .unwrap();
        cache
            .push_tick(&create_test_tick("ETHUSDT", "2.0", "t2"))
            .await
            .unwrap();

        cache.clear_symbol("BTCUSDT").await.unwrap();
        assert!(cache
            .get_recent_ticks("BTCUSDT", 1)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            cache.get_symbols().await.unwrap(),
            vec!["ETHUSDT".to_string()]
        );

        cache.clear_all().await.unwrap();
        assert!(cache.get_symbols().await.unwrap().is_empty());
    }
}
