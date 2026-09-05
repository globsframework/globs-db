package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class LockTimeout extends TransientSqlException {
    public LockTimeout(String sql, SQLException e) {
        super(sql, e);
    }

    public LockTimeout(SQLException e) {
        super(e);
    }
}
