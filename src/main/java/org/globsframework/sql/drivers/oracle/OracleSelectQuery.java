package org.globsframework.sql.drivers.oracle;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.accessors.SqlAccessor;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.SqlOperation;
import org.globsframework.sql.drivers.jdbc.SqlSelectQuery;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OracleSelectQuery extends SqlSelectQuery {

    public OracleSelectQuery(Connection connection, Constraint constraint, Map<Field, SqlAccessor> fieldToAccessorHolder, SqlService sqlService, boolean autoClose, List<SqlQueryBuilder.Order> orders, List<Field> groupBy, int top, int skip, Set<Field> distinct, List<SqlOperation> sqlOperations, GlobType fallBackType) {
        super(connection, constraint, fieldToAccessorHolder, sqlService, autoClose, orders, groupBy, top, skip, distinct, sqlOperations, fallBackType);
    }

    /**
     * Oracle has no LIMIT/OFFSET : row limiting is expressed with the standard
     * "OFFSET n ROWS FETCH NEXT m ROWS ONLY" clause (Oracle 12.1 and above), which
     * comes after the ORDER BY and puts the offset before the row count.
     */
    protected void appendTopAndSkip(StringPrettyWriter prettyWriter, int top, int skip) {
        if (skip != -1) {
            prettyWriter.append(" OFFSET " + skip + " ROWS");
        }
        if (top != -1) {
            prettyWriter.append(" FETCH NEXT " + top + " ROWS ONLY");
        }
    }
}
