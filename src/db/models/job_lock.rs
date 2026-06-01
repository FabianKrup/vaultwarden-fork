use chrono::Utc;
use diesel::prelude::*;

use crate::db::{DbConn, schema::job_lock};

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
    /// Fail-closed: any DB error returns `false` (not leader) to avoid split-brain.
    pub async fn try_acquire(conn: &DbConn, holder: &str, ttl_ms: u64) -> bool {
        let holder = holder.to_owned();
        let now = Utc::now().naive_utc();
        let expires = now + chrono::TimeDelta::milliseconds(i64::try_from(ttl_ms).unwrap_or(i64::MAX));
        db_run! { conn:
            {
                diesel::update(
                    job_lock::table
                        .filter(job_lock::id.eq(SCHEDULER_LEASE_ID))
                        .filter(job_lock::holder.eq(&holder).or(job_lock::expires_at.lt(now))),
                )
                .set((job_lock::holder.eq(&holder), job_lock::expires_at.eq(expires)))
                .execute(conn)
                .is_ok_and(|rows| rows == 1)
            }
        }
    }
}
