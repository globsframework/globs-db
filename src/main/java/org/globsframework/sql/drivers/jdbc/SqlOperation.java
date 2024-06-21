package org.globsframework.sql.drivers.jdbc;

import org.globsframework.sql.accessors.SqlAccessor;

public interface SqlOperation {

    SqlAccessor getAccessor();

    String toSqlOpe(ToSqlName toSqlName);
}
