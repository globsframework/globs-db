package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.TableRef;

public interface ToSqlName {
    String toSqlName(Field field);

    /**
     * @param table the occurrence the column belongs to, null for a bare field
     */
    default String toSqlName(Field field, TableRef table) {
        return toSqlName(field);
    }
}
