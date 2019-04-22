package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.sqlstreams.accessors.SqlAccessor;

public interface SqlOperation {

    SqlAccessor getAccessor();

    String toSqlOpe(ToSqlName toSqlName);
}
