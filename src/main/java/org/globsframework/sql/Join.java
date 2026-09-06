package org.globsframework.sql;

import org.globsframework.sql.constraints.Constraint;

/**
 * A table joined to the query, and the condition it is joined on.
 */
public record Join(TableRef table, Constraint on, Kind kind) {

    public enum Kind {
        inner("INNER JOIN"),
        left("LEFT JOIN");

        private final String sql;

        Kind(String sql) {
            this.sql = sql;
        }

        public String toSql() {
            return sql;
        }
    }
}
