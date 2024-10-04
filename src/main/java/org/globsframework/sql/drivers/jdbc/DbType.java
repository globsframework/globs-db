package org.globsframework.sql.drivers.jdbc;

public enum DbType {
    postgresql,
    hsqldb,
    mysql,
    mariadb;

    public static DbType fromString(String dbType) {
        if (dbType.contains("postgresql")) {
            return DbType.postgresql;
        }
        else if (dbType.contains("hsqldb")) {
            return DbType.hsqldb;
        }
        else if (dbType.contains("mariadb")) {
            return DbType.mariadb;
        }
        else if (dbType.contains("mysql")) {
            return DbType.mysql;
        }
        else {
            throw new IllegalArgumentException("Unknown database type: " + dbType);
        }
    }
}
