package org.globsframework.sql.drivers.jdbc;

import org.globsframework.metamodel.fields.Field;

public interface ToSqlName {
    String toSqlName(Field field);
}
