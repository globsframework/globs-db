package org.globsframework.sql;

import org.globsframework.core.metamodel.fields.Field;

/**
 * A column of one occurrence of a table: what a bare {@link Field} cannot express once the same type
 * appears twice in a query.
 */
public record ColumnRef(TableRef table, Field field) {
}
