use diesel::prelude::*;

use crate::db::DbConn;

/// Fixed key of the singleton scheduler-leadership row (seeded by migration).
pub const SCHEDULER_LEASE_ID: &str = "scheduler";

pub struct JobLock;

impl JobLock {
    /// Try to acquire or renew the scheduler lease. Returns `true` iff this replica
    /// now holds it, so only the lease holder runs the cron jobs.
    ///
    /// Atomic compare-and-swap: the row is updated only when this replica already
    /// holds it or the current lease has expired, so concurrent replicas re-evaluate
    /// after the winner commits and exactly one gets `rows_affected == 1`.
    ///
    /// Expiry is evaluated against the *database* clock (`datetime('now')` / `UTC_TIMESTAMP()`
    /// / `now()`), never each replica's wall clock, so clock skew between replicas cannot cause a
    /// premature takeover or a split lease. The new expiry is likewise computed DB-side.
    ///
    /// Fail-closed: any DB error returns `false` (not leader) to avoid split-brain.
    pub async fn try_acquire(conn: &DbConn, holder: &str, ttl_ms: u64) -> bool {
        let holder = holder.to_owned();
        let ttl_secs = i64::try_from(ttl_ms / 1000).unwrap_or(i64::MAX).max(1);
        db_run! { conn:
            sqlite {
                diesel::sql_query(
                    "UPDATE job_lock SET holder = ?, expires_at = datetime('now', ?) \
                     WHERE id = ? AND (holder = ? OR expires_at < datetime('now'))",
                )
                .bind::<diesel::sql_types::Text, _>(holder.as_str())
                .bind::<diesel::sql_types::Text, _>(format!("+{ttl_secs} seconds"))
                .bind::<diesel::sql_types::Text, _>(SCHEDULER_LEASE_ID)
                .bind::<diesel::sql_types::Text, _>(holder.as_str())
                .execute(conn)
                .is_ok_and(|rows| rows == 1)
            }
            mysql {
                diesel::sql_query(
                    "UPDATE job_lock SET holder = ?, expires_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND) \
                     WHERE id = ? AND (holder = ? OR expires_at < UTC_TIMESTAMP())",
                )
                .bind::<diesel::sql_types::Text, _>(holder.as_str())
                .bind::<diesel::sql_types::BigInt, _>(ttl_secs)
                .bind::<diesel::sql_types::Text, _>(SCHEDULER_LEASE_ID)
                .bind::<diesel::sql_types::Text, _>(holder.as_str())
                .execute(conn)
                .is_ok_and(|rows| rows == 1)
            }
            postgresql {
                diesel::sql_query(
                    "UPDATE job_lock SET holder = $1, \
                     expires_at = (now() AT TIME ZONE 'utc') + ($2 * interval '1 second') \
                     WHERE id = $3 AND (holder = $1 OR expires_at < (now() AT TIME ZONE 'utc'))",
                )
                .bind::<diesel::sql_types::Text, _>(holder.as_str())
                .bind::<diesel::sql_types::BigInt, _>(ttl_secs)
                .bind::<diesel::sql_types::Text, _>(SCHEDULER_LEASE_ID)
                .execute(conn)
                .is_ok_and(|rows| rows == 1)
            }
        }
    }
}
