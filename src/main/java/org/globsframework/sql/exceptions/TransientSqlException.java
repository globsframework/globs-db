package org.globsframework.sql.exceptions;

import java.sql.SQLException;

/**
 * A failure the database itself says is worth trying again: the transaction was rolled back for a
 * reason outside the statement — a concurrent writer, a lost connection, exhausted resources — and
 * the same work has a fair chance of succeeding on a second attempt.
 * <p>
 * This is what {@code RetryPolicy.onTransientFailures} keys on. It never covers a failure caused by
 * the data itself, such as a constraint violation, which would fail again identically.
 */
public class TransientSqlException extends SqlException {
    public TransientSqlException(String sql, SQLException e) {
        super(sql, e);
    }

    public TransientSqlException(SQLException e) {
        super(e);
    }
}
