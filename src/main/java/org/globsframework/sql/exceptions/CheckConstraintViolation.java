package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class CheckConstraintViolation extends ConstraintViolation {
    public CheckConstraintViolation(String sql, SQLException e) {
        super(sql, e);
    }

    public CheckConstraintViolation(SQLException e) {
        super(e);
    }
}
