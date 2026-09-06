package org.globsframework.sql.utils;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.utils.Strings;
import org.globsframework.sql.RetryPolicy;
import org.globsframework.sql.SqlListener;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.drivers.jdbc.NamingMapping;

import java.time.Duration;

public abstract class AbstractSqlService implements SqlService {
    private NamingMapping namingMapping;
    private RetryPolicy retryPolicy = RetryPolicy.NONE;
    private SqlListener listener = SqlListener.NONE;
    private int defaultFetchSize = 0;
    private Duration defaultQueryTimeout = null;

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

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Applied by the inTransaction/read templates. Off by default — see {@link RetryPolicy}.
     */
    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy == null ? RetryPolicy.NONE : retryPolicy;
    }

    public SqlListener getListener() {
        return listener;
    }

    public void setListener(SqlListener listener) {
        this.listener = listener == null ? SqlListener.NONE : listener;
    }

    public int getDefaultFetchSize() {
        return defaultFetchSize;
    }

    /**
     * Applied to every query that does not set its own fetch size.
     */
    public void setDefaultFetchSize(int defaultFetchSize) {
        this.defaultFetchSize = defaultFetchSize;
    }

    public Duration getDefaultQueryTimeout() {
        return defaultQueryTimeout;
    }

    /**
     * Applied to every query that does not set its own timeout.
     */
    public void setDefaultQueryTimeout(Duration defaultQueryTimeout) {
        this.defaultQueryTimeout = defaultQueryTimeout;
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
