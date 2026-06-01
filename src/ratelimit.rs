use std::{net::IpAddr, num::NonZeroU32, sync::LazyLock, time::Duration};

use governor::{Quota, RateLimiter, clock::DefaultClock, state::keyed::DashMapStateStore};

use crate::{CONFIG, Error, redis_conn};

type Limiter<T = IpAddr> = RateLimiter<T, DashMapStateStore<T>, DefaultClock>;

static LIMITER_LOGIN: LazyLock<Limiter> = LazyLock::new(|| {
    let seconds = Duration::from_secs(CONFIG.login_ratelimit_seconds());
    let burst = NonZeroU32::new(CONFIG.login_ratelimit_max_burst()).expect("Non-zero login ratelimit burst");
    RateLimiter::keyed(Quota::with_period(seconds).expect("Non-zero login ratelimit seconds").allow_burst(burst))
});

static LIMITER_ADMIN: LazyLock<Limiter> = LazyLock::new(|| {
    let seconds = Duration::from_secs(CONFIG.admin_ratelimit_seconds());
    let burst = NonZeroU32::new(CONFIG.admin_ratelimit_max_burst()).expect("Non-zero admin ratelimit burst");
    RateLimiter::keyed(Quota::with_period(seconds).expect("Non-zero admin ratelimit seconds").allow_burst(burst))
});

// Per-account login limiter (in-memory fallback), keyed by the hashed username instead of IP.
static LIMITER_LOGIN_ACCOUNT: LazyLock<Limiter<String>> = LazyLock::new(|| {
    let seconds = Duration::from_secs(CONFIG.login_account_ratelimit_seconds());
    let burst =
        NonZeroU32::new(CONFIG.login_account_ratelimit_max_burst()).expect("Non-zero login account ratelimit burst");
    RateLimiter::keyed(Quota::with_period(seconds).expect("Non-zero login account ratelimit seconds").allow_burst(burst))
});

// Atomic token bucket matching the governor quota: capacity = burst, refill one token every
// `seconds`. Uses Redis server TIME as the clock so replica clock skew is irrelevant.
// KEYS[1] = bucket key, ARGV[1] = capacity, ARGV[2] = refill seconds/token, ARGV[3] = consume flag
// (1 = take a token, 0 = peek). Peek reports availability without mutating the bucket. Returns 1/0.
static TOKEN_BUCKET: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r"
        local capacity = tonumber(ARGV[1])
        local refill_ms = tonumber(ARGV[2]) * 1000
        local consume = tonumber(ARGV[3])
        local t = redis.call('TIME')
        local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
        local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
        local tokens = tonumber(data[1])
        local ts = tonumber(data[2])
        if tokens == nil then tokens = capacity; ts = now end
        local elapsed = now - ts
        if elapsed > 0 then tokens = math.min(capacity, tokens + elapsed / refill_ms); ts = now end
        local allowed = 0
        if tokens >= 1 then allowed = 1 end
        if consume == 0 then return allowed end
        if allowed == 1 then tokens = tokens - 1 end
        redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', ts)
        redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * refill_ms))
        return allowed
    ",
    )
});

// Returns `Some(allowed)` from the shared Redis limiter, or `None` when Redis is unset or
// unreachable so the caller can fall back to the per-replica in-memory governor limiter.
// `id` is the bucket discriminator (an IP, or a hashed username for the per-account limiter).
async fn check_redis(scope: &str, seconds: u64, burst: u32, id: &str, consume: bool) -> Option<bool> {
    let mut conn = redis_conn::manager().await?;
    let key = format!("{}:rl:{scope}:{id}", CONFIG.redis_channel_prefix());
    let result: Result<i64, _> =
        TOKEN_BUCKET.key(key).arg(burst).arg(seconds).arg(i64::from(consume)).invoke_async(&mut conn).await;
    match result {
        Ok(allowed) => Some(allowed == 1),
        Err(e) => {
            error!("Redis rate-limit check failed: {e}");
            None
        }
    }
}

pub async fn check_limit_login(ip: &IpAddr) -> Result<(), Error> {
    let allowed = match check_redis(
        "login",
        CONFIG.login_ratelimit_seconds(),
        CONFIG.login_ratelimit_max_burst(),
        &ip.to_string(),
        true,
    )
    .await
    {
        Some(allowed) => allowed,
        None => LIMITER_LOGIN.check_key(ip).is_ok(),
    };
    if allowed {
        Ok(())
    } else {
        err_code!("Too many login requests", 429);
    }
}

// Bucket discriminator for the per-account limiter: the username hashed so the raw email never
// lands in a Redis key. Both `peek_` and `penalize_` key on this so they share one bucket.
fn account_bucket_id(username: &str) -> String {
    crate::crypto::sha256_hex(username.trim().to_lowercase().as_bytes())
}

/// Per-account login gate keyed by the hashed username. Defends distributed brute force (many IPs,
/// one account) that the per-IP limiter (`check_limit_login`) cannot see. This is a *non-consuming*
/// peek: it rejects an account whose bucket is already drained before the (expensive) password
/// verification runs, but a correct password never costs a token. Tokens are drained only on
/// failure by `penalize_login_account`.
///
/// Redis-backed when configured. The in-memory `governor` fallback cannot peek without consuming, so
/// when Redis is unset/unreachable this is a no-op `Ok(())` and the limit is enforced lazily by
/// `penalize_login_account` draining (and returning 429) on each failed attempt.
pub async fn peek_limit_login_account(username: &str) -> Result<(), Error> {
    let id = account_bucket_id(username);
    // Redis peek; fallback can't peek without consuming, so default-allow and let
    // `penalize_login_account` enforce the limit on failed attempts instead.
    let allowed = check_redis(
        "login_acct",
        CONFIG.login_account_ratelimit_seconds(),
        CONFIG.login_account_ratelimit_max_burst(),
        &id,
        false,
    )
    .await
    .unwrap_or(true);
    if allowed {
        Ok(())
    } else {
        err_code!("Too many login requests", 429);
    }
}

/// Drains one token from the per-account bucket. Called *only on failed* login attempts so a valid
/// password never penalizes the account. Returns `Err(429)` when the bucket was already empty: on
/// the in-memory fallback (which has no upfront peek) this is what enforces the limit; on Redis it
/// backstops `peek_limit_login_account`. Callers invoke this immediately before returning the
/// underlying auth error, so a 429 supersedes that error once the account is locked.
pub async fn penalize_login_account(username: &str) -> Result<(), Error> {
    let id = account_bucket_id(username);
    let allowed = match check_redis(
        "login_acct",
        CONFIG.login_account_ratelimit_seconds(),
        CONFIG.login_account_ratelimit_max_burst(),
        &id,
        true,
    )
    .await
    {
        Some(allowed) => allowed,
        None => LIMITER_LOGIN_ACCOUNT.check_key(&id).is_ok(),
    };
    if allowed {
        Ok(())
    } else {
        err_code!("Too many login requests", 429);
    }
}

pub async fn check_limit_admin(ip: &IpAddr) -> Result<(), Error> {
    let allowed = match check_redis(
        "admin",
        CONFIG.admin_ratelimit_seconds(),
        CONFIG.admin_ratelimit_max_burst(),
        &ip.to_string(),
        true,
    )
    .await
    {
        Some(allowed) => allowed,
        None => LIMITER_ADMIN.check_key(ip).is_ok(),
    };
    if allowed {
        Ok(())
    } else {
        err_code!("Too many admin requests", 429);
    }
}
