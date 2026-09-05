package org.globsframework.sql;

/**
 * Unit of work returning nothing, run against a connection managed by
 * {@link SqlService#runInTransaction(SqlConsumer)}.
 */
@FunctionalInterface
public interface SqlConsumer {
    void accept(SqlConnection connection) throws Exception;
}
