package org.globsframework.sqlstreams.utils;

import org.globsframework.sqlstreams.SqlService;
import org.globsframework.utils.Strings;

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
                // >> The database setting, SET DATABASE SQL REGULAR NAMES FALSE can be used to relax the rules for regular identifier. With this setting, an underscore character can appear at the start of the regular identifier, and the dollar sign character can be used in the identifier.
                return upper + "__";
            }
        }
        return upper;
    }

}
