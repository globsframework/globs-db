package org.globsframework.sql.drivers.jdbc;

import org.globsframework.sql.NativeValueBinder;
import org.globsframework.sql.drivers.postgresql.ToPostgreCaseNamingMapping;

public class MappingHelper {

    /**
     * PostgreSQL is the one that refuses a plain string for its own column types.
     */
    public static NativeValueBinder nativeValueBinder(DbType dbType) {
        return dbType == DbType.postgresql ? NativeValueBinder.UNTYPED : NativeValueBinder.AS_STRING;
    }

    public static NamingMapping get(DbType dbType) {
        return switch (dbType) {
            case postgresql -> new ToPostgreCaseNamingMapping();
            case hsqldb -> new HsqlDbNamingMapping();
            case mysql, mariadb, oracle -> new DefaultNamingMapping();
        };
    }
}
