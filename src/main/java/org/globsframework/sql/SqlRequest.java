package org.globsframework.sql;

import org.globsframework.sql.exceptions.SqlException;

public interface SqlRequest extends AutoCloseable {
    int apply() throws SqlException;

    void close();
}
