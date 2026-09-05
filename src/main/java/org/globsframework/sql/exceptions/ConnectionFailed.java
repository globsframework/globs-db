package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class ConnectionFailed extends TransientSqlException {
    public ConnectionFailed(String sql, SQLException e) {
        super(sql, e);
    }

    public ConnectionFailed(SQLException e) {
        super(e);
    }
}
