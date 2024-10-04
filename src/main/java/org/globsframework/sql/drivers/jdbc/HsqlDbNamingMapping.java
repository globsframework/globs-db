package org.globsframework.sql.drivers.jdbc;

import org.globsframework.sql.utils.AbstractSqlService;

class HsqlDbNamingMapping implements NamingMapping {
    public String getTableName(String typeName, boolean escaped) {
        return AbstractSqlService.toSqlName(typeName);
    }

    public String getColumnName(String fieldName, boolean escaped) {
        return AbstractSqlService.toSqlName(fieldName);
    }
}
