package org.globsframework.sqlstreams;

import org.globsframework.sqlstreams.exceptions.SqlException;

public interface SqlRequest extends AutoCloseable {
    void run() throws SqlException;

    void close();
}
