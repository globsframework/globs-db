package org.globsframework.sqlstreams;

import org.globsframework.sqlstreams.exceptions.SqlException;

public interface SqlRequest extends AutoCloseable {
    int run() throws SqlException;

    void close();
}
