package org.globsframework.sql.exceptions;

import java.sql.SQLException;

/**
 * The statement was cancelled or hit its timeout. Not a {@link TransientSqlException}: re-running a
 * query that was already too slow usually just spends the timeout again.
 */
public class QueryCanceled extends SqlException {
    public QueryCanceled(String sql, SQLException e) {
        super(sql, e);
    }

    public QueryCanceled(SQLException e) {
        super(e);
    }
}
