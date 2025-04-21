package org.globsframework.sql.drivers.jdbc;

import org.globsframework.sql.drivers.postgresql.ToPostgreCaseNamingMapping;

public class MappingHelper {

    public static NamingMapping get(DbType dbType) {
        switch (dbType) {
            case postgresql:{
                return new ToPostgreCaseNamingMapping();
            }
            case hsqldb:{
                return new HsqlDbNamingMapping();
            }
            case mysql:
            case mariadb:{
                return new DefaultNamingMapping();
            }
            default:{
                throw new IllegalArgumentException("Unknown database type: " + dbType);
            }
        }
    }
}
