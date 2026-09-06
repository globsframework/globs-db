package org.globsframework.sql.drivers.jdbc.impl;

/**
 * How many placeholders an IN clause is written with, shared by the visitor that writes the clause
 * and the one that binds its values — they have to agree or the statement is left with unbound
 * parameters.
 * <p>
 * The count is rounded up so that a set of 9 values and a set of 15 produce the same statement.
 * Without that, every distinct cardinality is a distinct SQL string, and a query fired with varying
 * set sizes fills the database's plan cache with near-identical entries — pg_stat_statements on
 * PostgreSQL, the shared pool on Oracle. Small sets keep their exact size, where the rounding would
 * cost more in bound values than it saves in plans.
 */
public class InClause {

    private InClause() {
    }

    public static int placeholderCount(int valueCount) {
        if (valueCount <= 8) {
            return valueCount;
        }
        if (valueCount <= 1024) {
            int rounded = 16;
            while (rounded < valueCount) {
                rounded <<= 1;
            }
            return rounded;
        }
        return (valueCount + 1023) / 1024 * 1024;
    }
}
