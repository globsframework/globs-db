package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class NotNullViolation extends ConstraintViolation {
    public NotNullViolation(String sql, SQLException e) {
        super(sql, e);
    }

    public NotNullViolation(SQLException e) {
        super(e);
    }
}
