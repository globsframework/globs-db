package org.globsframework.sql.drivers.jdbc;

class DefaultNamingMapping implements NamingMapping {
    public static NamingMapping INSTANCE = new DefaultNamingMapping();

    public String getTableName(String typeName, boolean escaped) {
        return typeName;
    }

    public String getColumnName(String fieldName, boolean escaped) {
        return fieldName;
    }
}
