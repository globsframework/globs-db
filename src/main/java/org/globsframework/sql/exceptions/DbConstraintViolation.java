package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class DbConstraintViolation extends SqlException {
    public DbConstraintViolation(SQLException e) {
        super(e);
    }
}
