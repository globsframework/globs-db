package org.globsframework.sql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.exceptions.SqlException;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

public interface SqlService extends AutoCloseable {

    SqlConnection getDb();

    SqlConnection getAutoCommitDb();

    String getTableName(GlobType globType, boolean escaped);

    String getTableName(String name, boolean escaped);

    String getColumnName(Field field, boolean escaped);

    String getColumnName(String field, boolean escaped);

    String getLikeIgnoreCase();

    /**
     * The policy the templates below apply when a transaction fails. {@link RetryPolicy#NONE} by
     * default: replaying the caller's lambda is only safe when it does nothing outside the database,
     * which this library cannot know.
     */
    default RetryPolicy getRetryPolicy() {
        return RetryPolicy.NONE;
    }

    /**
     * Notified after every statement. {@link SqlListener#NONE} by default.
     */
    default SqlListener getListener() {
        return SqlListener.NONE;
    }

    /**
     * Fetch size given to every query that does not set its own, 0 to leave the driver's default.
     * See {@link SelectBuilder#fetchSize(int)} for the PostgreSQL caveat.
     */
    default int getDefaultFetchSize() {
        return 0;
    }

    /**
     * Timeout given to every query that does not set its own, null for no limit.
     */
    default Duration getDefaultQueryTimeout() {
        return null;
    }

    /**
     * Runs the work in a transaction: commit on normal return, rollback on any exception, and the
     * connection is released either way. This is the recommended entry point — it makes it
     * impossible to leak a connection on an error path.
     */
    default <T> T inTransaction(SqlFunction<T> work) {
        return inTransaction(work, getRetryPolicy());
    }

    default <T> T inTransaction(SqlFunction<T> work, RetryPolicy retryPolicy) {
        return attempt(this::getDb, true, work, retryPolicy);
    }

    default void runInTransaction(SqlConsumer work) {
        inTransaction(connection -> {
            work.accept(connection);
            return null;
        });
    }

    /**
     * Runs the work on an auto-commit connection, released on return. For read-only queries, which
     * do not need a transaction.
     */
    default <T> T read(SqlFunction<T> work) {
        return attempt(this::getAutoCommitDb, false, work, getRetryPolicy());
    }

    default void runRead(SqlConsumer work) {
        read(connection -> {
            work.accept(connection);
            return null;
        });
    }

    private <T> T attempt(Supplier<SqlConnection> source, boolean commit, SqlFunction<T> work,
                          RetryPolicy retryPolicy) {
        for (int attempt = 1; ; attempt++) {
            try {
                return runOnce(source, commit, work);
            } catch (SqlException e) {
                Optional<Duration> delay = retryPolicy.nextDelay(e, attempt);
                if (delay.isEmpty()) {
                    throw e;
                }
                sleep(delay.get());
            }
        }
    }

    private <T> T runOnce(Supplier<SqlConnection> source, boolean commit, SqlFunction<T> work) {
        SqlConnection connection = source.get();
        try {
            T result = work.apply(connection);
            if (commit) {
                connection.commit();
            }
            return result;
        } catch (Exception e) {
            throw asUnchecked(e);
        } finally {
            // no-op once the commit above went through, rollback otherwise
            connection.close();
        }
    }

    private static void sleep(Duration delay) {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SqlException("Interrupted while waiting to retry", e);
        }
    }

    /**
     * Releases the resources owned by this service — the connection pool, when it owns one. A
     * service that borrows a DataSource from its host does nothing here.
     */
    default void close() {
    }

    private static RuntimeException asUnchecked(Exception e) {
        if (e instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new SqlException(e.getMessage(), e);
    }
}
