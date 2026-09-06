package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.TableRef;
import org.globsframework.core.metamodel.fields.Field;

import java.util.Set;

/**
 * What a column is prefixed with in the generated SQL.
 * <p>
 * Without joins that is the table name, exactly as it always was. With joins it is the alias of the
 * occurrence the column belongs to — which is the whole point of a {@link TableRef}: a bare
 * {@link Field} cannot say which side of a self join it means.
 */
public interface ColumnQualifier {

    /**
     * @param table the occurrence the column belongs to, null when the caller named a bare field
     */
    String qualify(Field field, TableRef table);

    /**
     * The joinless behaviour: every column is prefixed with its table name, and every type met that
     * way is added to the FROM clause.
     */
    static ColumnQualifier byTableName(SqlService sqlService, Set<GlobType> globTypesToUpdate) {
        return (field, table) -> {
            globTypesToUpdate.add(field.getGlobType());
            return sqlService.getTableName(field.getGlobType(), true);
        };
    }
}
