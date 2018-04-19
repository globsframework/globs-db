package org.globsframework.sqlstreams;

public interface BulkDbRequest extends SqlRequest {
    void flush();
}
