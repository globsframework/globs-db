package org.globsframework.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Notified after every statement this library executes, successful or not. The hook for a slow query
 * log, a timing histogram, or a tracing span, without making this artifact depend on any of them.
 * <p>
 * It is called on the thread that ran the statement, inside the transaction: keep it cheap, and let
 * nothing escape from it — an exception thrown here would surface as a failure of the query.
 */
public interface SqlListener {

    SqlListener NONE = (sql, durationNanos, rowCount, error) -> {
    };

    /**
     * @param rowCount rows affected by a DML statement, or -1 when it is not known — a SELECT
     *                 reports -1, since at that point the result set has not been walked yet
     * @param error    null when the statement succeeded
     */
    void onStatement(String sql, long durationNanos, int rowCount, Throwable error);

    /**
     * Logs, at warn, every statement slower than the threshold, and every statement that failed.
     */
    static SqlListener logSlowerThan(Duration threshold) {
        Logger logger = LoggerFactory.getLogger(SqlListener.class);
        long thresholdNanos = threshold.toNanos();
        return (sql, durationNanos, rowCount, error) -> {
            if (error != null) {
                logger.warn("Failed after {} ms : {}", durationNanos / 1_000_000, sql);
            } else if (durationNanos > thresholdNanos) {
                logger.warn("Slow statement, {} ms : {}", durationNanos / 1_000_000, sql);
            }
        };
    }
}
