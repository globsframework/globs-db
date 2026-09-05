package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class SerializationFailure extends TransientSqlException {
    public SerializationFailure(String sql, SQLException e) {
        super(sql, e);
    }

    public SerializationFailure(SQLException e) {
        super(e);
    }
}
