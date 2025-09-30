package org.globsframework.sql;

import org.globsframework.sql.exceptions.SqlException;

public interface BatchSqlRequest extends AutoCloseable {
    void addBatch() throws SqlException;

    int[] applyBatch();

    void close();
}
