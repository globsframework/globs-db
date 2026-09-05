package org.globsframework.sql;

import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.exceptions.TransientSqlException;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Decides whether a failed transaction is worth replaying.
 * <p>
 * Only the templates on {@link SqlService} apply it, and only they can: they own the connection, so
 * they know the transaction was rolled back and that a second attempt starts from a clean state.
 * Retrying is off by default — a retry re-runs the caller's lambda, which is only safe when that
 * lambda does nothing outside the database.
 */
public interface RetryPolicy {

    RetryPolicy NONE = (failure, attempt) -> Optional.empty();

    /**
     * @param attempt the attempt that just failed, 1 for the first
     * @return how long to wait before trying again, or empty to give up and rethrow
     */
    Optional<Duration> nextDelay(SqlException failure, int attempt);

    /**
     * Replays a transaction that failed for a reason the database calls transient — a serialization
     * failure, a deadlock, a lock timeout, a lost connection — never one caused by the data itself,
     * which would fail again identically. The delay doubles on each attempt and carries ±25% of
     * jitter, so a batch of workers that collided once does not collide again in lockstep.
     */
    static RetryPolicy onTransientFailures(int maxAttempts, Duration initialDelay) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1, got " + maxAttempts);
        }
        return (failure, attempt) -> {
            if (attempt >= maxAttempts || !(failure instanceof TransientSqlException)) {
                return Optional.empty();
            }
            long base = initialDelay.toMillis() << (attempt - 1);
            long jittered = base == 0 ? 0 : ThreadLocalRandom.current().nextLong(base * 3 / 4, base * 5 / 4 + 1);
            return Optional.of(Duration.ofMillis(jittered));
        };
    }
}
