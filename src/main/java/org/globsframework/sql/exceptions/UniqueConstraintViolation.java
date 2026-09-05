package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class UniqueConstraintViolation extends ConstraintViolation {
    public UniqueConstraintViolation(String sql, SQLException e) {
        super(sql, e);
    }

    public UniqueConstraintViolation(SQLException e) {
        super(e);
    }
}
