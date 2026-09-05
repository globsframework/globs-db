package org.globsframework.sql.exceptions;

import java.sql.SQLException;

public class ForeignKeyViolation extends ConstraintViolation {
    public ForeignKeyViolation(String sql, SQLException e) {
        super(sql, e);
    }

    public ForeignKeyViolation(SQLException e) {
        super(e);
    }
}
