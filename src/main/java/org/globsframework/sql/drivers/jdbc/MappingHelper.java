package org.globsframework.sql.drivers.jdbc;

import org.globsframework.sql.drivers.postgresql.ToPostgreCaseNamingMapping;

public class MappingHelper {

    public static NamingMapping get(DbType dbType) {
        return switch (dbType) {
            case postgresql -> new ToPostgreCaseNamingMapping();
            case hsqldb -> new HsqlDbNamingMapping();
            case mysql, mariadb -> new DefaultNamingMapping();
        };
    }
}
