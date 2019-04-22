package org.globsframework.sqlstreams.utils;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.annotations.TargetTypeName;
import org.globsframework.utils.Strings;

public abstract class AbstractSqlService implements SqlService {

    private static final String[] RESERVED_KEYWORDS = {
          "COUNT", "WHERE", "FROM", "SELECT"
    };

//    public String getTableName(GlobType globType) {
//        return toSqlName(globType.getName());
//    }
//
//    public String getColumnName(Field field) {
//        return toSqlName(field.getName());
//    }

    public static String toSqlName(String name) {
        return replaceReserved(Strings.toNiceUpperCase(name));
    }

    public static String replaceReserved(String upper) {
        for (String keyword : RESERVED_KEYWORDS) {
            if (upper.equals(keyword)) {
                return "_" + upper + "_";
            }
        }
        return upper;
    }

    public String getTableName(GlobType type) {
        return TargetTypeName.getName(type);
    }
}
