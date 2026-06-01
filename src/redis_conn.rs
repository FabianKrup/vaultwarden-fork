use std::sync::OnceLock;

use tokio::sync::Mutex;

use crate::{CONFIG, Error};

// Shared Redis used by both the WebSocket backplane (src/api/notifications.rs) and the rate
// limiter (src/ratelimit.rs). Present only when REDIS_URL is configured; absent means
// single-instance / local-only.

// Parsed at boot when REDIS_URL is set. `redis::Client::open` does not connect, so this only
// fails on a malformed URL (a real config error) — never on Redis being down.
static CLIENT: OnceLock<redis::Client> = OnceLock::new();

// Multiplexed connection manager, built on first use and cached. Rebuilt after a failed attempt
// so the fleet recovers when Redis returns without a restart.
static MANAGER: OnceLock<Mutex<Option<redis::aio::ConnectionManager>>> = OnceLock::new();

/// Open the shared Redis client at boot. Returns `Err` only on a malformed `REDIS_URL`; a missing
/// `REDIS_URL` is fine (single-instance / local-only) and opening does not connect to Redis.
/// A `rediss://` URL enables TLS (native-tls / OS CA store) for encrypted transport to Redis.
pub fn init() -> Result<(), Error> {
    use std::io::Error as IoError;

    let Some(url) = CONFIG.redis_url() else {
        return Ok(());
    };
    let client = redis::Client::open(url).map_err(IoError::other)?;
    // `set` errors only if already initialized (init runs once at boot).
    if CLIENT.set(client).is_err() {
        err!("Redis client must only be initialized once");
    }
    Ok(())
}

/// Whether Redis is configured (multi-replica mode).
pub fn is_enabled() -> bool {
    CLIENT.get().is_some()
}

/// The shared client, for pub/sub which needs its own dedicated connection.
pub fn client() -> Option<redis::Client> {
    CLIENT.get().cloned()
}

/// A multiplexed connection manager for ordinary commands. Built on first use and cached; returns
/// `None` when Redis is unset or currently unreachable (a later call retries the connection).
pub async fn manager() -> Option<redis::aio::ConnectionManager> {
    let client = CLIENT.get()?;
    let cell = MANAGER.get_or_init(|| Mutex::new(None));
    let mut guard = cell.lock().await;
    if let Some(mgr) = guard.as_ref() {
        return Some(mgr.clone());
    }
    match redis::aio::ConnectionManager::new(client.clone()).await {
        Ok(mgr) => {
            *guard = Some(mgr.clone());
            Some(mgr)
        }
        Err(e) => {
            error!("Redis connection unavailable: {e}");
            None
        }
    }
}
