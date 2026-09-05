package org.globsframework.sql;

/**
 * Unit of work run against a connection managed by {@link SqlService#inTransaction(SqlFunction)}.
 * Checked exceptions are allowed and wrapped by the caller.
 */
@FunctionalInterface
public interface SqlFunction<T> {
    T apply(SqlConnection connection) throws Exception;
}
