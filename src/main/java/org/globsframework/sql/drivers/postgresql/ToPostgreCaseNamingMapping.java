package org.globsframework.sql.drivers.postgresql;

import org.globsframework.sql.drivers.jdbc.JdbcSqlService;

import java.util.Locale;

public class ToPostgreCaseNamingMapping implements JdbcSqlService.NamingMapping {
    private JdbcSqlService.NamingMapping namingMapping;

    public ToPostgreCaseNamingMapping(JdbcSqlService.NamingMapping namingMapping) {
        this.namingMapping = namingMapping;
    }

    public String getTableName(String typeName, boolean escaped) {
        if (escaped) {
            return escapeUppercase(typeName);
        } else {
            return typeName;
        }
    }

    private static String escapeUppercase(String typeName) {
        if (typeName.startsWith("\"") && typeName.endsWith("\"")) {
            return typeName;
        }
        String lowerCase = typeName.toLowerCase(Locale.ROOT);
        if (lowerCase.equals(typeName)) {
            return typeName;
        } else {
            return "\"" + typeName + "\"";
        }
    }

    public String getColumnName(String fieldName, boolean escaped) {
        if (!escaped) {
            return fieldName;
        }
        return escapeUppercase(fieldName);
    }
}
