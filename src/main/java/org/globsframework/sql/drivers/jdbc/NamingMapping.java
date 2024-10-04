package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.annotations.DbFieldName;
import org.globsframework.sql.annotations.DbTableName;

public interface NamingMapping {
    default String getTableName(GlobType globType, boolean escaped) {
        return getTableName(DbTableName.getOptName(globType).orElse(globType.getName()), escaped);
    }

    String getTableName(String typeName, boolean escaped);

    default String getColumnName(Field field, boolean escaped) {
        return getColumnName(DbFieldName.getOptName(field).orElse(field.getName()), escaped);
    }

    String getColumnName(String fieldName, boolean escaped);

    default String getLikeIgnoreCase() {
        return null;
    }
}
