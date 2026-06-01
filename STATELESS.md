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
| Blob storage (attachments, sends, icons, key, config) | **S3 / S3-compatible object storage** | Routed through the opendal `PathType` abstraction. The FS abstraction is stable; the **S3 backend is new and opt-in** (`s3` feature, opendal pre-1.0) — see §3.1 caveat and roadmap step 0. |
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

> **S3 backend maturity caveat.** S3 is **not** part of a default build — it requires compiling with the `s3` feature (`build.rs:16`, `Cargo.toml:43`). The opendal dependency is `0.56.0` (`Cargo.toml:257`) — **pre-1.0, so its API is not frozen**. Full S3 parameter support (`OpenDAL S3 parameter support`, #6127) landed only **2026-05-15**, ~2 weeks before this assessment. It carries **no "experimental" label** in the code or `.env.template`, but it is new and comparatively unproven. The fork's reliance on S3 for attachments/sends/icons should be **validated end-to-end** (upload, download, delete, recursive Send purge) against the target object store before production. Treat opendal version bumps as potentially breaking.

### 3.2 Stateless blockers (work required)

| # | Component | Current behavior | Problem | Reference |
|---|-----------|------------------|---------|-----------|
| 1 | **WebSocket live-sync** | `WS_USERS` — in-memory `DashMap<user, [senders]>` | Process-local. Client on replica A never receives updates produced on replica B. | `src/api/notifications.rs:27` |
| 2 | **Anonymous WebSocket subscriptions** | `WS_ANONYMOUS_SUBSCRIPTIONS` — in-memory `DashMap` | Same as #1, for passwordless auth-request flow. | `src/api/notifications.rs:33` |
| 3 | **Background job scheduler** | Dedicated in-process thread on every replica | Every replica fires every cron job → duplicate emails, races. | `src/main.rs:663` |
| 4 | **Login rate limiter** | `LIMITER_LOGIN` — in-memory `governor` keyed by IP | Per-replica; effective limit = limit × replicas; bypassable. | `src/ratelimit.rs:9` |
| 5 | **Admin rate limiter** | `LIMITER_ADMIN` — in-memory | Same as #4. | `src/ratelimit.rs:15` |
| 6 | ✅ **JWT / RSA signing key** | ~~Generated on first boot, written to disk/S3~~ **Resolved:** `PRIVATE_RSA_KEY_PEM` env var injects the key; disk/S3 read + on-boot generation remain the fallback when unset. | ~~No env-injection path~~ Done — see roadmap step 1. | `src/auth.rs:63` |
| 7 | **`config.json` runtime writes** | Admin panel writes config to disk via opendal | Config read once into immutable `CONFIG` at boot; a write on one replica is invisible to others until restart. | `src/config.rs:1440`, `src/api/admin.rs:797` |
| 8 | **tmp folder for uploads** | `save_temp_file` lands multipart uploads in local `tmp_folder` | Chunked Send upload (v2) can span requests; if they hit different replicas the partial is lost. | `src/util.rs:878`, `src/config.rs:515` |
| 9 | ✅ **SQLite backup endpoint** | `/admin/config/backup_db` writes a file | **No action needed:** already gated by `CAN_BACKUP`, which is `false` whenever the DB is not SQLite, so the endpoint returns an error under external Postgres/MySQL. | `src/api/admin.rs:96-98` (gate), `src/api/admin.rs:816` (guard) |

### 3.3 Acceptable process-local caches (no change needed)

| Component | Why it's fine | Reference |
|-----------|---------------|-----------|
| Push relay OAuth token (`API_TOKEN`) | Per-replica refresh from upstream; independently rebuildable. | `src/api/push.rs:37` |
| SSO client + refresh caches | Rebuildable from config; short TTL. | `src/sso_client.rs:28` |
| Storage operator cache | Stateless operators, reconstructable. | `src/storage.rs:55` |
| `CONFIG`, JWT issuers, WebAuthn, HTTP client | Read-only, derived from env at boot. | `src/config.rs:37`, `src/auth.rs:44` |

## 4. Target State (to-be)

| # | Component | Target |
|---|-----------|--------|
| 1–2 | WebSocket fan-out | Publish every notification to a **Redis pub/sub** channel; every replica subscribes and forwards to its locally-connected clients. In-memory `DashMap` stays as the local connection registry only. Anonymous subscriptions use the same backplane. |
| 3 | Background jobs | Scheduler acquires a **DB advisory/distributed lock** before each run; only the lock holder executes. Lock auto-expires so a dead leader is replaced. |
| 4–5 | Rate limiting | Replace in-memory `governor` with a **Redis-backed** limiter keyed by IP, shared across the fleet. |
| 6 | JWT signing key | ✅ **Done.** Loads the private key PEM from the **`PRIVATE_RSA_KEY_PEM`** env var / secret mount; disk/S3 path remains a fallback. No runtime generation when the env var is set. |
| 7 | Runtime config | Treat configuration as **immutable and env-driven**. Disable / make read-only the admin `config.json` write path under a stateless flag (or require a rolling restart to pick up shared-storage config). |
| 8 | Upload temp | Route multipart temp storage to **shared object storage**, or require **sticky sessions** on the chunked upload endpoints only. |
| 9 | SQLite backup | ✅ **Confirmed off.** Disabled when DB is not SQLite — already gated by `CAN_BACKUP` (`src/api/admin.rs:96-98`). No code change needed. |

### Target external dependencies

- **SQL database**: PostgreSQL (recommended) or MySQL.
- **S3-compatible object storage**: AWS S3, MinIO, etc. (build with the `s3` feature/cfg).
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

0. **Validate the S3 backend** (§3.1 caveat) — compile with `s3`, exercise upload/download/delete/recursive-purge for attachments + sends + icons against the target object store. Pin the opendal version. Prerequisite for trusting blob storage.
1. ✅ **JWT key injection** (#6) — **done.** `PRIVATE_RSA_KEY_PEM` env var, read directly in `initialize_keys` (`src/auth.rs:63`), takes precedence over `RSA_KEY_FILENAME` and disables on-boot generation. Read straight from the env (not `CONFIG`) so the key never reaches the admin panel, `config.json`, or logs. Unblocks deterministic multi-replica boot.
2. **Redis backplane for WebSocket fan-out** (#1, #2) — core HA blocker; reintroduces the capability the old fork had.
3. **DB distributed lock for the scheduler** (#3) — prevents duplicate job execution.
4. **Redis-backed rate limiting** (#4, #5) — correctness across the fleet.
5. **Immutable config mode** (#7) — flag to disable admin config writes.
6. **Upload temp locality** (#8) — shared-storage temp or documented sticky-session requirement.
7. **Docs / deploy manifests** — example k8s + env reference; ~~confirm SQLite backup is gated off (#9)~~ ✅ #9 confirmed gated by `CAN_BACKUP`.

## 6. Out of Scope / Open Questions

- **Rate-limit semantics**: exact Redis algorithm (sliding window vs token bucket) — TBD during implementation.
- **Config mode**: hard-disable admin writes vs. shared-storage + rolling-restart — to be decided when tackling #7.
- **Redis HA**: single Redis vs. Sentinel/Cluster — deployment concern, not code.
- **Upload locality**: shared-storage temp vs. sticky sessions — pick during #8.
- **Push relay**: external Bitwarden push relay remains optional and orthogonal to the Redis backplane.
