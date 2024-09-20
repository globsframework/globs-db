package org.globsframework.sql.utils;

import org.globsframework.core.utils.Strings;
import org.globsframework.sql.SqlService;

public abstract class AbstractSqlService implements SqlService {

    private static final String[] RESERVED_KEYWORDS = {
            "COUNT", "WHERE", "FROM", "SELECT", "ORDER"
    };

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

}
