package org.globsframework.sql.drivers.mysql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.accessors.SqlAccessor;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.SqlOperation;
import org.globsframework.sql.drivers.jdbc.SqlSelectQuery;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.drivers.mysql.impl.MysqlWhereClauseConstraintVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MysqlSelectQuery extends SqlSelectQuery {

    public MysqlSelectQuery(Connection connection, Constraint constraint, Map<Field, SqlAccessor> fieldToAccessorHolder, SqlService sqlService, boolean autoClose, List<SqlQueryBuilder.Order> orders, List<Field> groupBy, int top, int skip, Set<Field> distinct, List<SqlOperation> sqlOperations, GlobType fallBackType) {
        super(connection, constraint, fieldToAccessorHolder, sqlService, autoClose, orders, groupBy, top, skip, distinct, sqlOperations, fallBackType);
    }

    protected WhereClauseConstraintVisitor getWhereConstraintVisitor(StringPrettyWriter where) {
        return new MysqlWhereClauseConstraintVisitor(where, sqlService, globTypes);
    }

    /**
     * MySQL and MariaDB reject an OFFSET that is not preceded by a LIMIT, so a skip() without a
     * top() has to name a limit anyway. 2^64-1 is the value their own documentation gives for
     * "all the rows from here on".
     */
    protected void appendTopAndSkip(StringPrettyWriter prettyWriter, int top, int skip) {
        if (top != -1) {
            prettyWriter.append(" LIMIT " + top);
        } else if (skip != -1) {
            prettyWriter.append(" LIMIT 18446744073709551615");
        }
        if (skip != -1) {
            prettyWriter.append(" OFFSET " + skip);
        }
    }
}
