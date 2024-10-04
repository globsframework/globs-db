package org.globsframework.sql.utils;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.utils.Strings;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.drivers.jdbc.NamingMapping;

public abstract class AbstractSqlService implements SqlService {
    private NamingMapping namingMapping;

    private static final String[] RESERVED_KEYWORDS = {
            "COUNT", "WHERE", "FROM", "SELECT", "ORDER"
    };

    public AbstractSqlService(NamingMapping namingMapping) {
        this.namingMapping = namingMapping;
    }

    public static String toSqlName(String name) {
        return replaceReserved(Strings.toNiceUpperCase(name));
    }

    public static String replaceReserved(String upper) {
        for (String keyword : RESERVED_KEYWORDS) {
            if (upper.equals(keyword)) {
                //return "_" + upper + "_";
                // MB HSQLDB > 1.8 does no more support field starting with _
                return upper + "__";
            }
        }
        return upper;
    }

    public NamingMapping getNamingMapping() {
        return namingMapping;
    }

    public String getTableName(GlobType globType, boolean escaped) {
        return namingMapping.getTableName(globType, escaped);
    }

    public String getTableName(String name, boolean escaped) {
        return namingMapping.getTableName(name, escaped);
    }

    public String getColumnName(String field, boolean escaped) {
        return namingMapping.getColumnName(field, escaped);
    }

    public String getLikeIgnoreCase() {
        return namingMapping.getLikeIgnoreCase();
    }

    public String getColumnName(Field field, boolean escaped) {
        return namingMapping.getColumnName(field, escaped);
    }

}
