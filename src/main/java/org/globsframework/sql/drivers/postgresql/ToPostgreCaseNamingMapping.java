package org.globsframework.sql.drivers.postgresql;

import org.globsframework.sql.drivers.jdbc.JdbcSqlService;

import java.util.Locale;

public class ToPostgreCaseNamingMapping implements JdbcSqlService.NamingMapping {
    private JdbcSqlService.NamingMapping namingMapping;

    public ToPostgreCaseNamingMapping(JdbcSqlService.NamingMapping namingMapping) {
        this.namingMapping = namingMapping;
    }

    public String getTableName(String typeName) {
        return escapeUppercase(typeName);
    }

    private static String escapeUppercase(String typeName) {
        String lowerCase = typeName.toLowerCase(Locale.ROOT);
        if (lowerCase.equals(typeName)) {
            return typeName;
        } else {
            return "\"" + typeName + "\"";
        }
    }

    public String getColumnName(String fieldName) {
        return escapeUppercase(fieldName);
    }
}
