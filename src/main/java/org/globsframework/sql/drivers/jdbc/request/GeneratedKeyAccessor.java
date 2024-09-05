package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.sql.SqlService;

import java.sql.ResultSet;

public interface GeneratedKeyAccessor {
    void setResult(ResultSet generatedKeys, SqlService sqlService);

    void reset();
}
