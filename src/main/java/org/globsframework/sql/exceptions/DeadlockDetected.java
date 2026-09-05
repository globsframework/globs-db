package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class DeadlockDetected extends TransientSqlException {
    public DeadlockDetected(String sql, SQLException e) {
        super(sql, e);
    }

    public DeadlockDetected(SQLException e) {
        super(e);
    }
}
