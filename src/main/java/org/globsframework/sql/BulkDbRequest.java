package org.globsframework.sql;

public interface BulkDbRequest extends SqlRequest {
    void flush();
}
