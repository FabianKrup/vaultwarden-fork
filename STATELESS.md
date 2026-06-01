# Vaultwarden — Stateless Fork

> Fork branch: `fork/stateless`. Tracks upstream [`dani-garcia/vaultwarden`](https://github.com/dani-garcia/vaultwarden) by merging `upstream/main` **into** this branch. Never merge this branch back into `main`.

## 1. Purpose

Make Vaultwarden **100% stateless** so it can run as a **horizontally-scalable, multi-replica** deployment:

- No reliance on a local persistent volume.
- Any replica can serve any request (load-balanced, no sticky requirement except where called out).
- Pods are disposable: kill/reschedule/scale at will with zero data loss and no functional degradation.
- All durable state lives in **external, shared backends** (SQL database, object storage, Redis).

### What "stateless" means here

A replica holds **no authoritative state** on its local filesystem or in process-local memory. Everything that must survive a restart, or be visible to another replica, is externalized. Process-local data is allowed **only** as a rebuildable performance cache.

## 2. Architecture Decisions

| Area | Decision | Rationale |
|------|----------|-----------|
| Deployment target | **Multi-replica HA** | Full horizontal scaling behind a load balancer. |
| Durable record store | **External SQL** (PostgreSQL / MySQL) | Already supported via `DATABASE_URL`. SQLite file is disallowed in stateless mode. |
| Blob storage (attachments, sends, icons, key, config) | **S3 / S3-compatible object storage** | Routed through the opendal `PathType` abstraction. This fork **compiles the S3 backend by default** (no `s3` feature flag); opendal is still pre-1.0, so see the §3.1 maturity caveat and roadmap step 0. |
| Live-sync (WebSocket fan-out) | **Redis pub/sub backplane** | Any replica can broadcast to clients connected to any other replica. |
| Scheduled background jobs | **DB distributed lock (leader election)** | Only one replica runs the scheduler at a time. No extra infra beyond the DB. |
| JWT / RSA signing key | **Injected via env / secret manager** | Deterministic, identical across all replicas, no per-instance generation. |
| Rate limiting | **Redis-backed** (reuse the pub/sub Redis) | In-memory limiters are per-replica and bypassable across the fleet. |
| Runtime configuration | **Env-driven and immutable** | Admin-panel writes to `config.json` cause cross-replica divergence (config is read once at boot, no hot-reload). |

## 3. Current State (as-is)

Legend: ✅ stateless-ready · ⚠️ partial / needs config or work · ❌ blocks statelessness

### 3.1 Already stateless-capable (provided by upstream)

Maturity legend: **Stable** = long-standing / default-built · **New** = recently merged, opt-in, dependency pre-1.0 — validate before relying on it.

| Component | Backing | State | Maturity | Reference |
|-----------|---------|-------|----------|-----------|
| Records (users, ciphers, orgs, devices, refresh tokens, 2FA, invites, auth-requests, emergency access, SSO flows) | External SQL via `DATABASE_URL` | ✅ | **Stable** — years old; PostgreSQL marked "recommended" | `src/db/mod.rs:256` (`DbConnType::from_url`) |
| File-storage abstraction (FS path) | opendal `PathType` → `services-fs` | ✅ | **Stable** — #5626 (2025-05-29), default code path for all file access | `src/storage.rs:53` |
| Attachments | opendal `PathType::Attachments` (FS or `s3://`) | ✅ via S3 | **New (S3)** — see note | `src/config.rs:1604`, `src/db/models/attachment.rs:53` |
| Sends (file blobs) | opendal `PathType::Sends` | ✅ via S3 | **New (S3)** — see note | `src/api/core/sends.rs:430` |
| Icon cache | opendal `PathType::IconCache` | ✅ via S3 (rebuildable) | **New (S3)** — see note | `src/api/icons.rs:161` |
| Templates / web-vault static assets | Disk or compile-time embedded, read-only | ✅ baked into image | **Stable** | `src/api/web.rs:194`, `src/config.rs:517` |
| Session / refresh / 2FA-remember tokens | DB rows | ✅ | **Stable** | `src/db/models/device.rs:34` |
| JWT validation | Stateless signature check | ✅ | **Stable** | `src/auth.rs:106` |

> **S3 backend maturity caveat.** This fork **always compiles S3 in** (the upstream `s3` feature flag and its `cfg(s3)` gates were removed; opendal builds with `services-s3` and the AWS credential crates are mandatory deps). The opendal dependency is `0.56.0` — **pre-1.0, so its API is not frozen**. Full S3 parameter support (`OpenDAL S3 parameter support`, #6127) landed only **2026-05-15**, ~2 weeks before this assessment. It carries **no "experimental" label** in the code or `.env.template`, but it is new and comparatively unproven. The fork's reliance on S3 for attachments/sends/icons should be **validated end-to-end** (upload, download, delete, recursive Send purge) against the target object store before production. Treat opendal version bumps as potentially breaking.

### 3.2 Stateless blockers (work required)

| # | Component | Current behavior | Problem | Reference |
|---|-----------|------------------|---------|-----------|
| 1 | ✅ **WebSocket live-sync** | `WS_USERS` `DashMap` is now a **local registry only**; updates publish to a Redis pub/sub channel and every replica forwards to its own clients. | ~~Process-local~~ **Resolved** via `REDIS_URL` backplane — see roadmap step 2. Unset = local-only (single instance). | `src/api/notifications.rs:27`, backplane at `start_backplane`/`run_subscriber` |
| 2 | ✅ **Anonymous WebSocket subscriptions** | `WS_ANONYMOUS_SUBSCRIPTIONS` — same backplane, separate channel. | **Resolved** with #1. | `src/api/notifications.rs:33` |
| 3 | ✅ **Background job scheduler** | ~~Dedicated in-process thread on every replica~~ **Resolved:** every replica still ticks, but a DB lease (`job_lock` table) elects one leader; only the lease holder runs jobs. Lease auto-expires (TTL = 3× poll interval) so a dead leader is replaced. | ~~Every replica fires every cron job~~ Done — see roadmap step 3. Always-on (no flag). | `src/main.rs` (`schedule_jobs` lease loop), `src/db/models/job_lock.rs` |
| 4 | ✅ **Login rate limiter** | ~~In-memory `governor` keyed by IP~~ **Resolved:** with `REDIS_URL` set, an atomic token-bucket Lua script (`TOKEN_BUCKET`) shares the limit across the fleet keyed by IP; falls back to the per-replica `governor` on any Redis error. | ~~Per-replica; effective limit = limit × replicas~~ Done — see roadmap step 4. | `src/ratelimit.rs`, `src/redis_conn.rs` |
| 5 | ✅ **Admin rate limiter** | ~~In-memory `governor`~~ **Resolved** with #4 (`:rl:admin:` keyspace, admin knobs). | ~~Same as #4~~ Done — see roadmap step 4. | `src/ratelimit.rs` |
| 6 | ✅ **JWT / RSA signing key** | ~~Generated on first boot, written to disk/S3~~ **Resolved:** `PRIVATE_RSA_KEY_PEM` env var injects the key; disk/S3 read + on-boot generation remain the fallback when unset. | ~~No env-injection path~~ Done — see roadmap step 1. | `src/auth.rs:63` |
| 7 | ✅ **`config.json` runtime writes** | ~~Admin panel writes config to disk via opendal~~ **Resolved:** `IMMUTABLE_CONFIG` env flag refuses admin config writes (`post_config`/`delete_config`) and makes `Config::load` skip reading `config.json` — config becomes env-only. | ~~Write on one replica invisible to others until restart~~ Done — see roadmap step 5. Unset = behavior unchanged. | `src/config.rs` (`load` gate + `immutable_config`), `src/api/admin.rs:798,807` |
| 8 | ✅ **tmp folder for uploads** | `save_temp_file` streams the multipart upload straight to opendal (S3/FS) at the final path; `tmp_folder` is only Rocket's request-scoped multipart spool. | **No action needed:** uploads are single-request Direct uploads (`fileUploadType:0`) — v2 is metadata-to-DB then full-file-to-S3, both shared backends. No chunk spans replicas; local temp is never authoritative. | `src/util.rs:877`, `src/api/core/sends.rs:303,375`, `src/config.rs:515` |
| 9 | ✅ **SQLite backup endpoint** | `/admin/config/backup_db` writes a file | **No action needed:** already gated by `CAN_BACKUP`, which is `false` whenever the DB is not SQLite, so the endpoint returns an error under external Postgres/MySQL. | `src/api/admin.rs:96-98` (gate), `src/api/admin.rs:816` (guard) |
| 10 | ⚠️ **Native Redis Cluster** | `redis_conn` opens a standalone `redis::Client` + `ConnectionManager`; the WS backplane uses standard pub/sub. | Works with a **single logical endpoint** only (managed Redis / Sentinel-behind-proxy). A native multi-shard **Redis Cluster** needs client-side MOVED/ASK routing (`redis::cluster::ClusterClient`) + sharded pub/sub — neither supported today. **Optional:** only required if the deployment targets native Cluster. | `src/redis_conn.rs`, `src/api/notifications.rs` |

### 3.3 Acceptable process-local caches (no change needed)

| Component | Why it's fine | Reference |
|-----------|---------------|-----------|
| Push relay OAuth token (`API_TOKEN`) | Per-replica refresh from upstream; independently rebuildable. | `src/api/push.rs:37` |
| SSO client + refresh caches | Rebuildable from config; short TTL. | `src/sso_client.rs:28` |
| Storage operator cache | Stateless operators, reconstructable. | `src/storage.rs:55` |
| `CONFIG`, JWT issuers, WebAuthn, HTTP client | Read-only, derived from env at boot. | `src/config.rs:37`, `src/auth.rs:44` |
| Multipart upload temp (`tmp_folder`) | Rocket request-scoped spool; deleted after the response, never read by another replica/request. Final blob is written to shared opendal/S3. | `src/config.rs:515`, `src/util.rs:877` |

## 4. Target State (to-be)

| # | Component | Target |
|---|-----------|--------|
| 1–2 | WebSocket fan-out | ✅ **Done.** Publishes every notification to a **Redis pub/sub** channel (`REDIS_URL`); every replica subscribes and forwards to its locally-connected clients. In-memory `DashMap` stays as the local connection registry only. Anonymous subscriptions use the same backplane (separate channel). |
| 3 | Background jobs | ✅ **Done.** Each replica keeps ticking, but acquires/renews a **DB lease** (`job_lock`, single seeded row, atomic CAS `UPDATE`) before each tick; only the lease holder (per-process uuid) runs jobs. TTL = 3× `JOB_POLL_INTERVAL_MS` so a dead leader is replaced; fail-closed on DB error. Always-on, no extra infra. |
| 4–5 | Rate limiting | ✅ **Done.** With `REDIS_URL` set, a **Redis-backed token bucket** (atomic Lua, server `TIME` clock) shares login/admin limits across the fleet keyed by IP, matching the `governor` quota (capacity = `*_max_burst`, refill 1 every `*_ratelimit_seconds`). Shared Redis connection lives in `src/redis_conn.rs` (also backs the WS backplane). On any Redis error — or when `REDIS_URL` is unset — falls back to the per-replica in-memory `governor`. Works independently of WebSockets. |
| 6 | JWT signing key | ✅ **Done.** Loads the private key PEM from the **`PRIVATE_RSA_KEY_PEM`** env var / secret mount; disk/S3 path remains a fallback. No runtime generation when the env var is set. |
| 7 | Runtime config | ✅ **Done.** `IMMUTABLE_CONFIG` makes configuration **immutable and env-driven**: admin `config.json` writes are refused and `config.json` is ignored at boot, so env is the sole source of truth. Change config via env + rolling restart. |
| 8 | Upload temp | ✅ **Confirmed non-blocker.** Uploads are single-request Direct uploads streamed straight to opendal (S3) at the final path (`save_temp_file`); no chunked/resumable endpoint exists, so no partial spans replicas. `tmp_folder` is request-scoped Rocket spool on local ephemeral disk — allowed rebuildable scratch (§3.3). No code change. **Size pod ephemeral storage for (concurrent uploads × max send size, ≤525 MB).** |
| 9 | SQLite backup | ✅ **Confirmed off.** Disabled when DB is not SQLite — already gated by `CAN_BACKUP` (`src/api/admin.rs:96-98`). No code change needed. |
| 10 | Native Redis Cluster | ⚠️ **Optional / not done.** Standalone client already covers a single logical endpoint (managed Redis / Sentinel-behind-proxy). For a native multi-shard Cluster: swap `redis_conn` to `redis::cluster::ClusterClient` and use sharded pub/sub in the backplane. Not required unless targeting native Cluster. |

### Target external dependencies

- **SQL database**: PostgreSQL (recommended) or MySQL.
- **S3-compatible object storage**: AWS S3, MinIO, etc. (compiled in by default — no feature flag).
- **Redis**: pub/sub backplane (WebSocket fan-out) + shared rate-limit store.

### Required configuration (target)

```
DATABASE_URL=postgresql://...        # external DB, never sqlite://
DATA_FOLDER=...                      # only for ephemeral/derived data
ATTACHMENTS_FOLDER=s3://bucket/attachments?region=...
SENDS_FOLDER=s3://bucket/sends?region=...
ICON_CACHE_FOLDER=s3://bucket/icons?region=...
PRIVATE_RSA_KEY_PEM=...              # inject JWT signing key (done — #6); overrides RSA_KEY_FILENAME
RSA_KEY_FILENAME=...                 # fallback when PRIVATE_RSA_KEY_PEM is unset
# new (target):
REDIS_URL=redis://...                # WebSocket backplane + rate limiting
# logging to stdout (no LOG_FILE)
```

## 5. Roadmap

0. **Validate the S3 backend** (§3.1 caveat) — S3 is now compiled in by default (no `s3` feature); exercise upload/download/delete/recursive-purge for attachments + sends + icons against the target object store. Pin the opendal version. Prerequisite for trusting blob storage.
1. ✅ **JWT key injection** (#6) — **done.** `PRIVATE_RSA_KEY_PEM` env var, read directly in `initialize_keys` (`src/auth.rs:63`), takes precedence over `RSA_KEY_FILENAME` and disables on-boot generation. Read straight from the env (not `CONFIG`) so the key never reaches the admin panel, `config.json`, or logs. Unblocks deterministic multi-replica boot.
2. ✅ **Redis backplane for WebSocket fan-out** (#1, #2) — **done.** `REDIS_URL` enables a Redis pub/sub backplane (`src/api/notifications.rs`): each replica keeps its `DashMap` as a local registry, publishes updates to `<prefix>:ws:user` / `<prefix>:ws:anonymous`, and forwards received messages to its own clients. Publisher delivers locally + skips its own echoed messages via a per-process origin id. Unset `REDIS_URL` = local-only (single instance unchanged). Built + clippy-clean against redis-rs 0.27.
3. ✅ **DB distributed lock for the scheduler** (#3) — **done.** `job_lock` lease table + `JobLock::try_acquire` (atomic CAS `UPDATE`); `schedule_jobs` (`src/main.rs`) renews the lease each tick and gates job work behind a `SCHEDULER_IS_LEADER` flag — the leader runs jobs, others only tick (keeping their per-job baseline current). Auto-expiring lease (TTL = 3× poll) handles failover; fail-closed on DB error. Always-on, no infra beyond the DB.
4. ✅ **Redis-backed rate limiting** (#4, #5) — **done.** Token-bucket Lua keyed `{prefix}:rl:{login,admin}:{ip}` via the shared `src/redis_conn.rs` connection; matches the `governor` quota and uses Redis server `TIME` (no clock-skew). Falls back to the in-memory `governor` on Redis error or when `REDIS_URL` is unset. `redis_conn::init` opens the client lazily (only a malformed `REDIS_URL` is fatal), so the backplane no longer crashloops when Redis is down at boot — it degrades to local-only and reconnects.
5. ✅ **Immutable config mode** (#7) — **done.** `IMMUTABLE_CONFIG` env flag refuses admin config writes (`post_config`/`delete_config`, `src/api/admin.rs`) and makes `Config::load` skip `config.json` (`src/config.rs`), so config is env-only and identical across replicas. Unset = unchanged. Note: traditional Duo 2FA deployments must set `_DUO_AKEY` via env (the auto-generated AKey is otherwise persisted to `config.json`).
6. ✅ **Upload temp locality** (#8) — **confirmed non-blocker.** Traced the upload path: v2 is a single-request Direct upload (`fileUploadType:0`), the file streams straight to opendal/S3 via `save_temp_file` and metadata goes to the DB — no chunked/resumable endpoint, so nothing spans replicas. `tmp_folder` is request-scoped local spool only (§3.3). No code change; only a docs note to size pod ephemeral disk for concurrent uploads (× max send size, ≤525 MB).
7. **Docs / deploy manifests** — example k8s + env reference; ~~confirm SQLite backup is gated off (#9)~~ ✅ #9 confirmed gated by `CAN_BACKUP`.
8. **(Optional) Native Redis Cluster support** (#10) — only if targeting a multi-shard Cluster instead of a single endpoint. Swap to `redis::cluster::ClusterClient` in `src/redis_conn.rs` + sharded pub/sub in the backplane. The standalone client already covers managed Redis / Sentinel-behind-a-proxy.

## 6. Out of Scope / Open Questions

- ~~**Rate-limit semantics**: sliding window vs token bucket — TBD.~~ ✅ Resolved: atomic token bucket (Lua) matching the `governor` quota, Redis server `TIME` as the clock.
- ~~**Config mode**: hard-disable admin writes vs. shared-storage + rolling-restart — to be decided when tackling #7.~~ ✅ Resolved with #7: **hard-disable**. `IMMUTABLE_CONFIG` refuses admin config writes and ignores `config.json` at boot; change config via env + rolling restart.
- **Redis HA**: `src/redis_conn.rs` uses a standalone `redis::Client` + `ConnectionManager`. A **single logical endpoint** (managed Redis, or Sentinel behind a proxy/VIP) is a deployment concern — no code change. **Native Redis Cluster** (client-side MOVED/ASK routing + sharded pub/sub) is *not* supported by the standalone client and would need code — tracked as blocker #10 (optional, only if targeting native Cluster).
- ~~**Upload locality**: shared-storage temp vs. sticky sessions — pick during #8.~~ ✅ Resolved: no chunked upload exists; single-request uploads stream straight to S3, local temp is request-scoped only. No code change.
- **Push relay**: external Bitwarden push relay remains optional and orthogonal to the Redis backplane.
