package org.globsframework.sql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.exceptions.SqlException;

public interface SqlService extends AutoCloseable {

    SqlConnection getDb();

    SqlConnection getAutoCommitDb();

    String getTableName(GlobType globType, boolean escaped);

    String getTableName(String name, boolean escaped);

    String getColumnName(Field field, boolean escaped);

    String getColumnName(String field, boolean escaped);

    String getLikeIgnoreCase();

    /**
     * Runs the work in a transaction: commit on normal return, rollback on any exception, and the
     * connection is released either way. This is the recommended entry point — it makes it
     * impossible to leak a connection on an error path.
     */
    default <T> T inTransaction(SqlFunction<T> work) {
        SqlConnection connection = getDb();
        try {
            T result = work.apply(connection);
            connection.commit();
            return result;
        } catch (Exception e) {
            throw asUnchecked(e);
        } finally {
            // no-op once the commit above went through, rollback otherwise
            connection.close();
        }
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
        SqlConnection connection = getAutoCommitDb();
        try {
            return work.apply(connection);
        } catch (Exception e) {
            throw asUnchecked(e);
        } finally {
            connection.close();
        }
    }

    default void runRead(SqlConsumer work) {
        read(connection -> {
            work.accept(connection);
            return null;
        });
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
