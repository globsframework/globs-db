package org.globsframework.sql.drivers.jdbc;

/**
 * Passes every name through untouched. This is what MySQL, MariaDB and Oracle use, and it is the
 * escape hatch for a PostgreSQL database created before the dialect defaulted to
 * {@link org.globsframework.sql.drivers.postgresql.ToPostgreCaseNamingMapping} — pass it explicitly
 * to JdbcSqlService to keep reading tables whose names PostgreSQL folded to lower case.
 */
public class DefaultNamingMapping implements NamingMapping {
    public static final NamingMapping INSTANCE = new DefaultNamingMapping();

    public String getTableName(String typeName, boolean escaped) {
        return typeName;
    }

    public String getColumnName(String fieldName, boolean escaped) {
        return fieldName;
    }
}
