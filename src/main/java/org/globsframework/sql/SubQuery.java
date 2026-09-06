package org.globsframework.sql;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.constraints.Constraint;

/**
 * A nested SELECT, used by {@code exists} and by the {@code in} that takes a query rather than a set
 * of values.
 * <p>
 * Its table comes from the builder ({@code table(type)}), which is what gives it an alias distinct
 * from the enclosing query's — so its condition can compare a column of the subquery with a column
 * of the query around it, which is what makes it correlated.
 *
 * @param selected the projected column, null for an {@code EXISTS}, which selects nothing
 */
public record SubQuery(TableRef table, Field selected, Constraint where) {
}
