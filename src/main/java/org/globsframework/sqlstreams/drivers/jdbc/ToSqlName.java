package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;

public interface ToSqlName {
    String toSqlName(Field field);
}
